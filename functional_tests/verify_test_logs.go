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
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
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
	inputFile := flag.String("i", "", "The input file to read")

	var expectedNum int
	flag.IntVar(&expectedNum, "expected-num", -1, "The total number of messages sent during the test run by generate_test_logs.go")

	flag.Parse()

	if len(*inputFile) == 0 || expectedNum < 0 {
		fmt.Println("Input file, and expected number must be given")
		flag.PrintDefaults()
		os.Exit(1)
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		panic(err)
	}

	r := bufio.NewReader(f)

	var seen = make(map[int]struct{})
	hasher := fnv.New64a()

	fail := false

	numLines := 0
	for {
		var n int
		var h uint64

		// Each record in log format has a 4 byte prefix
		// which I assumed was meant to be length but is actually always 0x00000000
		// for some reason.. ignore it anyway...
		var l uint32
		err := binary.Read(r, binary.LittleEndian, &l)
		if panic_or_break(err) {
			break
		}

		_, err = fmt.Fscanf(r, "%10d %x|", &n, &h)
		if panic_or_break(err) {
			break
		}

		_, ok := seen[n]
		if ok {
			fmt.Println("FAIL: Duplicate delivery for %d", n)
			fail = true
		} else {
			seen[n] = struct{}{}
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

		fmt.Printf("\rProcessed % 10d lines", numLines+1)

		numLines++
	}

	fmt.Printf("\nDONE: found %d log entries, expected %d, difference: %d\n", numLines, expectedNum, numLines-expectedNum)

	if numLines-expectedNum != 0 {
		fail = true
	}

	if fail {
		fmt.Println("FAIL")
		os.Exit(1)
	} else {
		fmt.Println("OK")
	}
}
