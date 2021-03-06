package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/artyom/scribe"
	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/artyom/thrift"
	"github.com/DeviantArt/kafka-scribe/Godeps/_workspace/src/github.com/golang/glog"
)

var BackPressureError = fmt.Errorf("Backpressure! Scribe client queue hit max pending requests")
var ClosingError = fmt.Errorf("Client is stopping, cancelled pending request")

// ReliableScribeClient is a thin wrapper around scribe.ScribeClient that will reconnect
// on errors and retry indefinitely.
// It is also safe to call Log() from multiple goroutines at once
type ReliableScribeClient struct {
	host     string
	scribe   *scribe.ScribeClient
	stop     chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
}

func NewReliableScribeClient(host string) *ReliableScribeClient {
	rs := &ReliableScribeClient{
		host:   host,
		scribe: nil,
		stop:   make(chan struct{}),
	}
	return rs
}

func (rs *ReliableScribeClient) Stop() {
	rs.stopOnce.Do(func() {
		close(rs.stop)
	})
}

func (rs *ReliableScribeClient) Log(entries []*scribe.LogEntry) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	var err error

	var backOff = 250 * time.Millisecond
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
		if rs.shouldStop() {
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

// shouldStop checks if the stop chan is closed, without blocking on it
// if it is not.
func (rs *ReliableScribeClient) shouldStop() bool {
	select {
	case _, ok := <-rs.stop:
		// ok = true should never actually happen as we never send on stop, just close it
		return !ok
	default:
		// Continue without blocking if the stop chan is still open
		return false
	}
}

func connectScribe(host string) (*scribe.ScribeClient, error) {
	var socket, err = thrift.NewTSocketTimeout(host, 5*time.Second)
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
