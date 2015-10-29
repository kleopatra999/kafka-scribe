package main

// This file is a verification for manual end-to-end integration test.
//
// We have a custom Scribe store that ends up outputting a custom indexed binary log format.
// The data file of that ends up being the raw log entries with a 4 byte prefix. Ignoring the meaning
// of prefix (it's NOT a length prefix - that is indexed separately) we can scan through and verify that
// all of the sequential messages from the generate_test_logs.go file in this dir made it into this log segment.
//
// This is in repo even for external users who will not have scribe logs in this format as it should be trivial to change
// if you require and is a useful sanity check. It could be automated into an integration test but the coordination of a whole
// kafka cluster, scribe instance, kafka-scribe instance, and any simulated failures you might want to test is a lot of extra work
// that is not warranted right now for our purposes. Ad-hoc manual tests with these scripts are enough to be reasonably confident.
//
// This assumes several things:
//  - that this log file contains nothing but logs from a single run of the test script
//  - that we are relaying the whole topic the test script produced to to this one Scribe category
//    (kafka-scribe doesn't support partial relay anyway yet)
//  - that any kafka or scribe or relay errors you would like to test are manually invoked during the one test run and
//    this log captures output, after all failures are resolved and relay/scribe caught up
//
// We verify that:
//  - all log entries parse in expected format (int sequence number and hex hash)
//  - every sequence number from 0 through maximum produced appears only once
//    (Note that they won't be sequential if kafka topic has more than one partition, so we exhaustively
//    check the whole sequence space with a set)
//  - that we got exactly the right number of entries
//  - that none of them had a higher value than expected form the num sent
//  - implicitly, given the above, that if every seq is unique, lower than expected max, and we have the right number, then each one
//    must have been delivered exactly once

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strconv"
)

func panic_or_break(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	panic(err)
}

func main() {

	var inputFile string
	var expectedNum int
	var expectOffsets bool

	flag.StringVar(&inputFile, "i", "", "The input file to read")
	flag.IntVar(&expectedNum, "expected-num", -1, "The total number of messages sent during the test run by generate_test_logs.go")
	flag.BoolVar(&expectOffsets, "expect-offsets", false, "Include flag if generate_test_logs.go was run with -add-json-offsets option to validate json messages contain sane offsets")

	flag.Parse()

	if len(inputFile) == 0 || expectedNum < 0 {
		fmt.Println("Input file, and expected number must be given")
		flag.PrintDefaults()
		os.Exit(1)
	}

	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}

	r := bufio.NewReader(f)

	s := bufio.NewScanner(r)
	// We have two formats of message - text ending in | and json (which we guarantee
	// not to contain } except at end)
	s.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		n := bytes.IndexAny(data, "|}")
		if n < 0 {
			return 0, nil, nil
		}
		// Include the terminating char
		return n + 1, data[0 : n+1], nil
	})

	var seen = make(map[int]bool)
	var offsetsSeen = make(map[int]map[int]bool)
	hasher := fnv.New64a()

	fail := false

	numMessages := 0
	for {
		var n int
		var h uint64

		// Lines should alternate between JSON and plain text but due to partitioning
		// we don't get order guaranteed so accept either
		if !s.Scan() {
			break
		}

		m := s.Bytes()

		// Each record in log format has a 4 byte prefix
		// which I assumed was meant to be length but is actually always 0x00000000
		// for some reason.. ignore it anyway...
		m = m[4:]

		if len(m) < 1 {
			fmt.Println("WARN: Empty message")
			continue
		}

		if m[0] == '{' {
			// Treat it as JSON
			var message map[string]interface{}
			err = json.Unmarshal(m, &message)
			if panic_or_break(err) {
				break
			}
			n = int(message["n"].(float64))
			hex := message["hash"].(string)
			h, err = strconv.ParseUint(hex, 16, 64)
			if panic_or_break(err) {
				break
			}
			if expectOffsets {
				offset := int(message["kafka_offset"].(float64))
				part := int(message["kafka_partition"].(float64))
				if offsetsSeen[part][offset] {
					fmt.Println("FAIL: Duplicate delivery for %d", n)
					fail = true
				} else {
					offsetsSeen[part][offset] = true
				}
			}
		} else {
			_, err = fmt.Sscanf(string(m), "%10d %x|", &n, &h)
			if panic_or_break(err) {
				break
			}
		}

		if seen[n] {
			fmt.Println("FAIL: Duplicate delivery for %d", n)
			fail = true
		} else {
			seen[n] = true
		}

		if n >= expectedNum {
			fmt.Println("FAIL: Got an entry with higher sequence number than expected, Expected max %d, got %d", expectedNum-1, n)
			fail = true
		}

		hasher.Reset()
		hasher.Write([]byte(fmt.Sprintf("%010d", n)))
		if h != hasher.Sum64() {
			fmt.Println("FAIL: mismatched hash for %d. Got %x expected %x", n, hasher.Sum64(), h)
			fail = true
		}

		fmt.Printf("\rProcessed % 10d messages", numMessages+1)

		numMessages++
	}

	fmt.Printf("\nDONE: found %d log entries, expected %d, difference: %d\n", numMessages, expectedNum, numMessages-expectedNum)

	if numMessages-expectedNum != 0 {
		fail = true
	}

	if fail {
		fmt.Println("FAIL")
		os.Exit(1)
	} else {
		fmt.Println("OK")
	}
}
