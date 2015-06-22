package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/artyom/scribe"
	"github.com/artyom/thrift"

	"github.com/Shopify/sarama"

	"github.com/quipo/statsd"

	"github.com/golang/glog"
)

type TopicMap struct {
	m map[string]string
}

// String implements Value interface
func (m *TopicMap) String() string {
	return fmt.Sprintf("kafka_topic_name => scribe_category_name")
}

// Set implements Value interface for reading form command line
func (m *TopicMap) Set(value string) error {
	parts := strings.Split(value, "=>")

	if len(parts) == 1 {
		m.m[value] = value
	} else if len(parts) == 2 {
		m.m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	} else {
		panic("Bad format for topic map argument. Expect `-t topic_name` or `-t \"kafka_topic => scribe_category\"`.")
	}
	return nil
}

var BackPressureError = fmt.Errorf("Backpressure! Scribe client queue hit max pending requests")
var ClosingError = fmt.Errorf("Client is stopping, cancelled pending request")

// ReliableScribeClient is a thin wrapper around scribe.ScribeClient that will reconnect
// on errors and retry indefinitely.
// It is also safe to call Log() from multiple goroutines at once
type ReliableScribeClient struct {
	host     string
	scribe   *scribe.ScribeClient
	stop     chan interface{}
	stopping bool
	mu       sync.Mutex
}

func NewReliableScribeClient(host string) *ReliableScribeClient {
	rs := &ReliableScribeClient{
		host:   host,
		scribe: nil,
		stop:   make(chan interface{}),
	}
	return rs
}

func (rs *ReliableScribeClient) Stop() {
	rs.stopping = true
	close(rs.stop)
}

func (rs *ReliableScribeClient) Log(entries []*scribe.LogEntry) error {
	if rs.stopping {
		return ClosingError
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	var err error

	var backOff = 50 * time.Millisecond
	var maxBackOff = 5 * time.Minute

	var retryBackoff = func(err error) {
		glog.Errorf("Scribe connection failed, with %v. Will retry after %v", err, backOff)
		rs.scribe = nil

		select {
		case <-rs.stop:
			return
		case <-time.After(backOff):
			backOff = backOff * 2
			if backOff > maxBackOff {
				backOff = maxBackOff
			}
			return
		}
	}

	// Retry indefinitely, might block many threads as we hold lock
	// but that's as designed - they can't make progress if scribe host is
	// not available and the back-pressure is helpful
	for {
		if rs.stopping {
			return ClosingError
		}

		if rs.scribe == nil {
			rs.scribe, err = connectScribe(rs.host)
			if err != nil {
				retryBackoff(err)
				continue
			}
		}

		resp, err := rs.scribe.Log(entries)
		if err != nil || resp == scribe.ResultCode_TRY_LATER {
			retryBackoff(err)
			continue
		}

		// Sent OK, we are done...
		return nil
	}
}

func connectScribe(host string) (*scribe.ScribeClient, error) {
	var socket, err = thrift.NewTSocket(host)
	if err != nil {
		return nil, err
	}
	err = socket.Open()
	if err != nil {
		return nil, err
	}

	var transport = thrift.NewTFramedTransport(socket)
	var scribeClient = scribe.NewScribeClientFactory(transport, thrift.NewTBinaryProtocolFactoryDefault())
	return scribeClient, nil
}

type KafkaPartitionMirrorConfig struct {
	topic        string
	partition    int32
	scribeCat    string
	maxBatchSize int
	maxBatchWait time.Duration
	startOffset  int64
}

func NewKafkaPartitionMirrorConfig(topic string, partition int32, offset int64) KafkaPartitionMirrorConfig {
	return KafkaPartitionMirrorConfig{
		topic:        topic,
		partition:    partition,
		scribeCat:    topic,
		maxBatchSize: 1000,
		maxBatchWait: 500 * time.Millisecond,
		startOffset:  offset,
	}
}

type KafkaPartitionMirror struct {
	consumer  sarama.Consumer
	scribe    *ReliableScribeClient
	cfg       KafkaPartitionMirrorConfig
	stop      chan interface{}
	stopping  bool
	partition sarama.PartitionConsumer
	offStore  *LocalOffsetStore
	sd        statsd.Statsd
}

func NewKafkaPartitionMirror(c sarama.Consumer, rs *ReliableScribeClient, cfg KafkaPartitionMirrorConfig,
	ofs *LocalOffsetStore, sd statsd.Statsd) (*KafkaPartitionMirror, error) {
	kpm := &KafkaPartitionMirror{
		consumer: c,
		scribe:   rs,
		cfg:      cfg,
		stop:     make(chan interface{}),
		offStore: ofs,
		sd:       sd,
	}

	partition, err := c.ConsumePartition(cfg.topic, cfg.partition, cfg.startOffset)
	if err != nil {
		return nil, err
	}

	kpm.partition = partition

	return kpm, nil
}

func (kpm *KafkaPartitionMirror) Run() {

	batch := make([]*scribe.LogEntry, 0, kpm.cfg.maxBatchSize)
	maxOffset := kpm.cfg.startOffset
	// Create timer that is immediately stopped.
	// It won't fire until we get at least one message queued
	batchTimer := time.NewTimer(kpm.cfg.maxBatchWait)
	batchTimer.Stop()

	for {
		if kpm.stopping {
			return
		}

		select {
		case <-kpm.stop:
			return

		case m := <-kpm.partition.Messages():
			beforeLen := len(batch)
			// Grow batch slice we guarantee it's backing array isn't full below
			batch = batch[:beforeLen+1]
			batch[beforeLen] = &scribe.LogEntry{kpm.cfg.scribeCat, string(m.Value)}
			maxOffset = m.Offset

			if (beforeLen + 1) == cap(batch) {
				// Actually send the batch we have
				// this might block indefinitely waiting for scribe host to come up
				err := kpm.sendBatch(&batch, maxOffset)
				if err != nil {
					// Should only happen if scribe client has been asked to close (i.e. we are shutting down)
					// just return as we are going to stop soon too presumably.
					kpm.Stop()
					return
				}
				batchTimer.Stop()
			} else if beforeLen < 1 {
				// This was first message in this batch, set timer so it will be sent if batch is not filled
				// within the next maxBatchWait time
				batchTimer.Reset(kpm.cfg.maxBatchWait)
			}

		case <-batchTimer.C:
			if len(batch) > 0 {
				err := kpm.sendBatch(&batch, maxOffset)
				if err != nil {
					// Should only happen if scribe client has been asked to close (i.e. we are shutting down)
					// just return as we are going to stop soon too presumably.
					kpm.Stop()
					return
				}
				batchTimer.Stop()
			} else {
				glog.Infoln(kpm.cfg.partition, "Busy wait nothing to do.. ")
			}
		}
	}
}

func (kpm *KafkaPartitionMirror) sendBatch(batch *[]*scribe.LogEntry, maxOffset int64) error {
	// Actually send the batch we have
	// this might block indefinitely waiting for scribe host to come up
	err := kpm.scribe.Log(*batch)
	if err != nil {
		return err
	}

	// Sent OK update offset in local file
	kpm.offStore.MarkOffsetProcessed(kpm.cfg.topic, kpm.cfg.partition, maxOffset)

	numSent := len(*batch)

	glog.V(1).Infof("Partition (%s, %d) delivered %d messages to %s (up to offset %d)",
		kpm.cfg.topic, kpm.cfg.partition, numSent,
		kpm.cfg.scribeCat, maxOffset)

	// Reset batch, should be safe to re-use since we waited for scribe client to be done with sending it
	*batch = (*batch)[:0]

	kpm.sd.Gauge(fmt.Sprintf("%s.%02d.max_offset", kpm.cfg.topic, kpm.cfg.partition), maxOffset)
	kpm.sd.Incr(fmt.Sprintf("%s.%02d.msgs_relayed", kpm.cfg.topic, kpm.cfg.partition), int64(numSent))

	return nil
}

func (kpm *KafkaPartitionMirror) Stop() {
	if kpm.stopping {
		return
	}
	glog.Infof("Stopping mirror for (%s, %d)", kpm.cfg.topic, kpm.cfg.partition)
	kpm.stopping = true
	close(kpm.stop)
	kpm.partition.Close()
}

// startTopicMirrors fins all partitions for topic from consumer and starts a KafkaPartitionMirror
// in a goroutine for each one
// TODO make errors here non-fatal so we can gracefully continue. That means retrying and handling partial failures gracefully.
func startTopicMirrors(c sarama.Consumer, rs *ReliableScribeClient, ofs *LocalOffsetStore, sd statsd.Statsd,
	topic, category string) []*KafkaPartitionMirror {

	partitions, err := c.Partitions(topic)
	if err != nil {
		glog.Errorf("Failed to get partition info for topic %s", topic)
	}

	mirrors := make([]*KafkaPartitionMirror, 0, len(partitions))

	glog.Infof("Found %d partitions for topic %s, starting mirrors", len(partitions), topic)

	for _, p := range partitions {
		startOffset, err := ofs.GetNextOffset(topic, p)
		if err != nil {
			glog.Fatalf("Failed to get offset for (%s, %d) due to: %s", topic, p, err)
		}
		cfg := NewKafkaPartitionMirrorConfig(topic, p, startOffset)
		cfg.scribeCat = category
		mirror, err := NewKafkaPartitionMirror(c, rs, cfg, ofs, sd)
		if err != nil {
			glog.Fatalf("Failed to start mirror for (%s, %d) due to: %s", topic, p, err)
		}

		glog.Infof("Starting mirror for (%s, %d) at offset %d", topic, p, startOffset)

		go mirror.Run()

		mirrors = append(mirrors, mirror)
	}

	return mirrors
}

type LocalOffsetStore struct {
	fName              string
	file               *os.File
	m                  map[string]int64
	dirty              bool
	mu                 sync.Mutex
	stop               chan interface{}
	stopping           bool
	commitCoalesceTime time.Duration
}

func NewLocalOffsetStore(fileName string, commitCoalesceTime time.Duration) *LocalOffsetStore {
	los := &LocalOffsetStore{
		fName:              fileName,
		m:                  make(map[string]int64),
		stop:               make(chan interface{}),
		commitCoalesceTime: commitCoalesceTime,
	}

	go los.run()

	return los
}

func toPartitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

func fromPartitionKey(key string) (string, int32, error) {
	var topic string
	var partition int32
	_, err := fmt.Sscanf(key, "%s:%d", &topic, &partition)
	return topic, partition, err
}

func (los *LocalOffsetStore) Open() error {
	los.mu.Lock()
	defer los.mu.Unlock()

	// Reset m
	los.m = make(map[string]int64)

	var err error
	los.file, err = os.Open(los.fName)
	if os.IsNotExist(err) {
		// No file, we will create it when we first sync
		return nil
	}

	if err != nil {
		return err
	}

	// Read contents as JSON
	info, err := los.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() > int64(0) {
		// Parse JSON
		dec := json.NewDecoder(los.file)
		err = dec.Decode(&los.m)
		if err != nil {
			return err
		}
	}

	return nil
}

func (los *LocalOffsetStore) run() {
	ticker := time.NewTicker(los.commitCoalesceTime)
	for {
		select {
		case <-los.stop:
			return
		case <-ticker.C:
			err := los.Sync()
			if err != nil {
				glog.Errorf("Error syncing offset file:", err)
			}
		}
	}
}

func (los *LocalOffsetStore) Close() {
	los.mu.Lock()
	los.stopping = true
	los.mu.Unlock()
	close(los.stop)
	los.Sync()
}

// Assumes los is open successfully...
func (los *LocalOffsetStore) GetNextOffset(topic string, partition int32) (int64, error) {
	los.mu.Lock()
	defer los.mu.Unlock()

	offset, ok := los.m[toPartitionKey(topic, partition)]

	// If topic/partition is not known in map, start from the oldest message still on brokers
	if !ok {
		return sarama.OffsetOldest, nil
	}

	return offset, nil
}

func (los *LocalOffsetStore) MarkOffsetProcessed(topic string, partition int32, offset int64) error {
	los.mu.Lock()
	defer los.mu.Unlock()

	// oldOffset is the NEXT one we expect assuming this was just a batch of one
	// i.e. if this was a batch of one then it should have offset == oldOffset since we store
	// the NEXT position to read from in the offset store
	oldOffset, ok := los.m[toPartitionKey(topic, partition)]

	// We already committed a higher offset which implicitly means this is already committed
	if ok && oldOffset > offset {
		glog.Errorf("Hmm processed older offset than previously committed... (%d > %d)", oldOffset, offset)
		return nil
	}

	// We store the next offset we haven't yet processed...
	los.m[toPartitionKey(topic, partition)] = offset + 1
	los.dirty = true
	glog.V(1).Infof("Marked (%s, %d) processed up to offset %d", topic, partition, offset+1)

	return nil
}

func (los *LocalOffsetStore) Sync() error {
	los.mu.Lock()
	defer los.mu.Unlock()

	if !los.dirty {
		// No updates since last sync
		return nil
	}

	tmpFile := los.fName + "." + time.Now().Format(time.RFC3339Nano) + ".tmp"

	// Write temp file to disk
	out, err := os.Create(tmpFile)
	if err != nil {
		out.Close()
		os.Remove(tmpFile)
		return err
	}

	bytes, err := json.MarshalIndent(los.m, "", "    ")
	if err != nil {
		out.Close()
		os.Remove(tmpFile)
		return err
	}

	_, err = out.Write(bytes)
	if err != nil {
		out.Close()
		os.Remove(tmpFile)
		return err
	}

	// Flush to disk
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmpFile)
		return err
	}

	out.Close()

	// Atomic rename
	if err := os.Rename(tmpFile, los.fName); err != nil {
		os.Remove(tmpFile)
		return err
	}

	los.dirty = false
	glog.V(1).Infof("Local offset table Sync OK")

	return nil
}

func main() {
	var kafkaBrokers string
	var scribeHost string
	var topicMap = TopicMap{m: make(map[string]string)}
	var offsetStoreFile string
	var offsetCommitWaitMs int
	var statsdHost string

	flag.StringVar(&kafkaBrokers, "kafka-brokers", "localhost:9092",
		"hostname:port[,hostname:port[,...]] for the kafka brokers used to bootstrap consumer")

	flag.StringVar(&scribeHost, "scribe-host", "localhost:1464",
		"hostname:port for the Scribe host to relay messages")

	flag.StringVar(&statsdHost, "statsd-host", "",
		"hostname:port for statsd. If none given then metrics are not recorded")

	flag.StringVar(&offsetStoreFile, "offset-file", "kafka-scribe-offsets.json",
		"The file to read/write offsets to as we go")

	flag.Var(&topicMap, "t", "Topic map, for each topic in Kafka to relay, add an argument like: '-t topic_name'."+
		"If you want the Kafka topic to be relayed to a Scribe category with a different name then use '-t \"topic_name => scribe_category\"'.")

	flag.IntVar(&offsetCommitWaitMs, "offset-file-commit-wait-ms", 100,
		"how regularly (in milliseconds) to commit offsets file in an attempt to coalesce multiple writes into one")

	flag.Parse()

	if len(topicMap.m) < 1 {
		fmt.Println("No topics given, must specify at least one topic to relay")
		flag.PrintDefaults()
		return
	}

	fmt.Println("Connecting to Scribe host:", scribeHost)
	fmt.Println("Connecting to Kafka brokers:", kafkaBrokers)

	offsetStore := NewLocalOffsetStore(offsetStoreFile, time.Duration(offsetCommitWaitMs)*time.Millisecond)
	err := offsetStore.Open()
	if err != nil {
		glog.Fatalln("Failed to open offset store file:", err)
	}
	defer offsetStore.Close()

	scribeClient := NewReliableScribeClient(scribeHost)
	defer scribeClient.Stop()

	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(strings.Split(kafkaBrokers, ","), config)
	if err != nil {
		glog.Fatal("Failed to start Kafka consumer with:", err)
	}
	defer consumer.Close()

	var statsdClient *statsd.StatsdClient
	var sd statsd.Statsd

	if len(statsdHost) > 0 {
		statsdClient = statsd.NewStatsdClient(statsdHost, "kafka-scribe.")
		statsdClient.CreateSocket()
		sd = statsd.NewStatsdBuffer(1*time.Second, statsdClient)
	} else {
		sd = &statsd.NoopClient{}
	}

	mirrors := make([]*KafkaPartitionMirror, 0, 16)

	for topic, category := range topicMap.m {
		ms := startTopicMirrors(consumer, scribeClient, offsetStore, sd, topic, category)
		mirrors = append(mirrors, ms...)
	}

	// Wait for term
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	<-signals
	glog.Infoln("Got TERM, stopping mirrors")

	// We were interrupted, stop all the mirrors
	for _, mirror := range mirrors {
		mirror.Stop()
	}

	glog.Flush()

	return
}
