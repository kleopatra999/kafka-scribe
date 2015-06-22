package main

import (
	"strings"

	"github.com/golang/glog"
)

type TopicMap struct {
	m map[string]string
}

// String implements Value interface
func (m *TopicMap) String() string {
	return "kafka_topic_name => scribe_category_name"
}

// Set implements Value interface for reading form command line
func (m *TopicMap) Set(value string) error {
	parts := strings.Split(value, "=>")

	if len(parts) == 1 {
		m.m[value] = value
	} else if len(parts) == 2 {
		m.m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	} else {
		glog.Fatal("Bad format for topic map argument. Expect `-t topic_name` or `-t \"kafka_topic => scribe_category\"`.")
	}
	return nil
}
