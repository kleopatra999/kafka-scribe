package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
)

func main() {

	var brokerList, topicList string

	flag.StringVar(&brokerList, "broker-list", "localhost:9092", "host:port[,..] list of brokers to produce to")

	flag.Parse()

	producer, err := NewAsyncProducer(strings.Split(brokerList, ","), nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
