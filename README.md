# kafka-scribe
Simple relay to push one or more kafka topics to an single upstream Scribe host

## Goals/Features

 - Can relay multiple Kafka topics, mapping each whole topic to one scribe category
 - Records kafka offsets per partition in a local file on disk (syncs to disk after scribe Log for batch completes)
 - Optionally dumps per-partition relay rate and offset to StatsD for monitoring of progress/stall
 - Retries indefinitely with exponential backoff if upstream Scribe is overloaded (TRY_AGAIN response) or fails. This blocks all progress consuming more from kafka.
 - Handles Kafka leadership changes thanks to Sarama client library

## Limitations

 - Only designed to run on single node - does NOT use high level Kafka consumer to allow splitting work over many nodes
 - Records kafka offset to local disk only. If that disk or machine dies so does the ability to resume accurately as described above
 - Above points imply it's not highly available
 - No real performance testing - it's plenty fast enough for our use cases
 - Can only consume an entire kafka topic, not just some partitions
 - Static topic configuration - can't automatically start relaying new topics on creation
 - Can only produce to a single upstream Scribe host currently. So all topics must be relayed to same host (or run multiple instances relaying different topics to different hosts)
 - It can't atomically record it's position to disk on successful batch send, so there is small chance it could crash and relay same events to Scribe. Scribe itself does not provide any stronger guarantee though so you should already be prepared for duplicates.
 - By default we only write/sync the offset position file to disk once every 100ms across all partitions, so there is a 100ms window of failure as described above in addition to the non-atomicity.

## Building/Installing

Assumes you have working Go toolchain.

```
go get https://github.com/DeviantArt/kafka-scribe.git
```

## Usage

```
$ kafka-scribe -t "topic_name => scribe_cat" -t topic2 \
  -kafka-brokers kafka1:9092,kafka2:9092 -scribe-host scribe:1464 \
  -statsd-host localhost:8125 -logtostderr=true
```

## Caveats

Limited testing - see `/functional_test` for simple ad-hoc scripts used to test it manually through various failure conditions and configurations

The individual parts are such simple plumbing that getting test harness working was significantly more work than building it as it is.

We don't anticipate this being a permanent part of our infrastructure, just a shim to allow migration between systems. So it's here as-is, maybe useful to someone else interfacing these two systems.

Performance may or may not be "good". In my testing on laptop with VMs involved, even with a single thread, we could relay an 8 partition kafka topic to Scribe faster than the Scribe server could log the data to disk and were continually backing off waiting for Scribe at about 70k (28 byte) messages a second. Absolutely no tuning for performance has done as this is ample for our needs, although we did at least add a little extra complexity to avoid syncing offset file to disk on every batch sent.

**NOTE: may cause data loss/duplication:** we intentionally stuck with the risky method of relying on a local file to maintain cursor for kafka position. This is **NOT** reliable for critical production use. If you loose the one machine this file is on you will be left guessing where you got up to. You will either end up underestimating and replaying duplicates or overestimating and loosing messages.

For our temporary needs this was sufficient and acceptable. If you want this as part of your production setup it's probably not. But you can still use the code as a basis for something more robust if you need that.
