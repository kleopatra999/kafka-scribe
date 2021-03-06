package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/Shopify/sarama"

	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/quipo/statsd"

	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/golang/glog"
)

// startTopicMirrors finds all partitions for topic from consumer and starts a KafkaPartitionMirror
// in a goroutine for each one
// TODO make errors here non-fatal so we can gracefully continue. That means retrying and handling partial failures gracefully.
func startTopicMirrors(c sarama.Consumer, rs *ReliableScribeClient, ofs *LocalOffsetStore, sd statsd.Statsd,
	topic, category string, addOffsetsToJSON bool, filterWithSuffix string) []*KafkaPartitionMirror {

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
		cfg := NewKafkaPartitionMirrorConfig(topic, p, startOffset, addOffsetsToJSON, filterWithSuffix)
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

// SaramaGlogger is a shim that implements sarama.StdLogger interface but calls glog.* methods
type SaramaGlogger struct{}

func (sg *SaramaGlogger) Print(v ...interface{}) {
	v = append([]interface{}{"[Sarama]"}, v...)
	glog.Info(v...)
}

func (sg *SaramaGlogger) Printf(format string, v ...interface{}) {
	glog.Infof("[Sarama] "+format, v...)
}

func (sg *SaramaGlogger) Println(v ...interface{}) {
	v = append([]interface{}{"[Sarama]"}, v...)
	glog.Infoln(v...)
}

func main() {
	var kafkaBrokers string
	var scribeHost string
	var topicMap = TopicMap{m: make(map[string]string)}
	var offsetStoreFile string
	var offsetCommitWaitMs int
	var statsdHost, statsdPrefix string
	var addOffsetsToJSON bool
	var filterWithSuffix string

	flag.Var(&topicMap, "t", "Topic map, for each topic in Kafka to relay, add an argument like: '-t topic_name'."+
		"If you want the Kafka topic to be relayed to a Scribe category with a different name then use "+
		"'-t \"topic_name => scribe_category\"'.")

	flag.StringVar(&kafkaBrokers, "kafka-brokers", "localhost:9092",
		"hostname:port[,hostname:port[,...]] for the kafka brokers used to bootstrap consumer")

	flag.StringVar(&scribeHost, "scribe-host", "localhost:1464",
		"hostname:port for the Scribe host to relay messages")

	flag.StringVar(&statsdHost, "statsd-host", "",
		"hostname:port for statsd. If none given then metrics are not recorded")

	flag.StringVar(&statsdPrefix, "statsd-prefix", "kafka-scribe.",
		"Prefix for statsd metrics logged")

	flag.StringVar(&offsetStoreFile, "offset-file", "kafka-scribe-offsets.json",
		"The file to read/write offsets to as we go")

	flag.IntVar(&offsetCommitWaitMs, "offset-file-commit-wait-ms", 100,
		"How regularly (in milliseconds) to commit offsets file in an attempt to coalesce multiple writes into one")

	flag.BoolVar(&addOffsetsToJSON, "add-offsets-to-json", false,
		"If set, will add kafka partition and offset keys to messages that look like JSON objects (end in '}')."+
			" WARNING: If you might have messages that are NOT json objects but might end in '}' you really shouldn't use this")

	flag.StringVar(&filterWithSuffix, "filter-with-suffix", "",
		"A suffix string that will be matched exactly against each message."+
			"If this is non-empty and a message has a matching suffix, we will drop the message silently instead of relaying it.")

	flag.Parse()

	if len(topicMap.m) < 1 {
		fmt.Println("No topics given, must specify at least one topic to relay")
		flag.PrintDefaults()
		return
	}

	glog.Info("Connecting to Scribe host:", scribeHost)
	glog.Info("Connecting to Kafka brokers:", kafkaBrokers)

	offsetStore := NewLocalOffsetStore(offsetStoreFile, time.Duration(offsetCommitWaitMs)*time.Millisecond)
	err := offsetStore.Open()
	if err != nil {
		glog.Fatalln("Failed to open offset store file:", err)
	}
	defer offsetStore.Close()

	scribeClient := NewReliableScribeClient(scribeHost)
	defer scribeClient.Stop()

	// Set sarama logger to use glog shim
	sarama.Logger = &SaramaGlogger{}

	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(strings.Split(kafkaBrokers, ","), config)
	if err != nil {
		glog.Fatal("Failed to start Kafka consumer with:", err)
	}
	defer consumer.Close()

	var statsdClient *statsd.StatsdClient
	var sd statsd.Statsd

	if len(statsdHost) > 0 {
		statsdClient = statsd.NewStatsdClient(statsdHost, statsdPrefix)
		statsdClient.CreateSocket()
		sd = statsd.NewStatsdBuffer(1*time.Second, statsdClient)
	} else {
		sd = &statsd.NoopClient{}
	}

	mirrors := make([]*KafkaPartitionMirror, 0, 16)

	for topic, category := range topicMap.m {
		ms := startTopicMirrors(consumer, scribeClient, offsetStore, sd, topic, category, addOffsetsToJSON, filterWithSuffix)
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

	// Explicit close since Sarama apparently can keep whole program alive
	// even after we stopped all our mirrors with background meta updates...
	//consumer.Close()

	glog.Flush()

	return
}
