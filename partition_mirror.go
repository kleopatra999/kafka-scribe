package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/artyom/scribe"
	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/quipo/statsd"
)

const (
	OffsetKeyJson        = "%s,\"kafka_offset\":%d,\"kafka_partition\":%d}"
	OffsetKeyJsonNoComma = "%s\"kafka_offset\":%d,\"kafka_partition\":%d}"
)

type KafkaPartitionMirrorConfig struct {
	topic            string
	partition        int32
	scribeCat        string
	maxBatchSize     int
	maxBatchWait     time.Duration
	startOffset      int64
	addOffsetsToJSON bool
}

func NewKafkaPartitionMirrorConfig(topic string, partition int32, offset int64, addOffsetsToJSON bool) KafkaPartitionMirrorConfig {
	return KafkaPartitionMirrorConfig{
		topic:            topic,
		partition:        partition,
		scribeCat:        topic,
		maxBatchSize:     1000,
		maxBatchWait:     500 * time.Millisecond,
		startOffset:      offset,
		addOffsetsToJSON: addOffsetsToJSON,
	}
}

type KafkaPartitionMirror struct {
	consumer  sarama.Consumer
	scribe    *ReliableScribeClient
	cfg       KafkaPartitionMirrorConfig
	stop      chan interface{}
	stopOnce  sync.Once
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
		select {
		case <-kpm.stop:
			return

		case m := <-kpm.partition.Messages():
			beforeLen := len(batch)
			// Grow batch slice we guarantee it's backing array isn't full below
			batch = batch[:beforeLen+1]
			// Hackily insert kafka offset into message if it's jsonish
			// To save on overhead of parsing JSON and re-encoding it, we assume
			// all messages ending in `}` are valid JSON just do simple replace...
			var message string
			if kpm.cfg.addOffsetsToJSON && len(m.Value) > 1 && m.Value[len(m.Value)-1] == '}' {
				// We already checked there is at least 2 bytes in message above
				format := OffsetKeyJson
				if bytes.Equal(m.Value[len(m.Value)-2:], []byte("{}")) {
					// Edge case where value ends with empty object, remove comma from our trailer
					format = OffsetKeyJsonNoComma
				}
				message = fmt.Sprintf(format, m.Value[0:len(m.Value)-1], m.Offset, kpm.cfg.partition)
			} else {
				message = string(m.Value)
			}
			batch[beforeLen] = &scribe.LogEntry{kpm.cfg.scribeCat, message}
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
	kpm.stopOnce.Do(func() {
		glog.Infof("Stopping mirror for (%s, %d)", kpm.cfg.topic, kpm.cfg.partition)
		close(kpm.stop)
		kpm.partition.Close()
	})
}
