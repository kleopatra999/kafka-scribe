# kafka-scribe
Simple relay to push one or more kafka topics to an single upstream Scribe host

## Usage

```
$ kafka-scribe -t "topic_name => scribe_cat" -t topic2 -kafka-brokers kafka1:9092,kafka2:9092 -scribe-host scribe:1464 -statsd-host localhost:8125 -logtostderr=true
```

## Caveats

Limited testing - see `/functional_test` for simple ad-0hoc scripts used to test it manually through various failure conditions and configurations

The individual parts are such simple plumbing that getting test harness working was significantly more work than building it as it is.

We don't anticipate this beeing a permenant part of our infrastructure even, just a shim to allo migration between systems. So it's here as-is, maybe useful to someone else interfacing these two systems.

Performance may or may not be "good". In my testing on laptop with VMs involved, even with a single thread, we could relay an 8 partition kafka topic to Scribe faster than the Scribe server could log the data to disk and were continually backing off waiting for Scribe at about 70k (28 byte) messages a second. Absolutely no tuning for performance has done as this is ample for our needs, although we did at least add a little extra complexity to avoid syncing offset file to disk on every batch sent.

**NOTE: may cause data loss/duplication:** we intentionally stuck with the risky method of relying on a local file to maintain cursor for kafka postition. This is **NOT** reliable for critical production use. If you loose the one machine this file is on you will be left guessing where you got up to. You will either end up underestimating and replaying duplicates or overestimating and loosing messages.

For our temporary needs this was sufficient and acceptable. If you want this as part of your production setup it's probably not. But you can still use the code as a basis for something more robust if you need that.
