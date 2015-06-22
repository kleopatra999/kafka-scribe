package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

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
