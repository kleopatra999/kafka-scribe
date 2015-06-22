package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

func main() {

	var brokerList, topic string

	flag.StringVar(&brokerList, "broker-list", "localhost:9092", "host:port[,..] list of brokers to produce to")
	flag.StringVar(&topic, "topic", "", "topic to produce to")

	flag.Parse()

	if len(topic) < 1 {
		fmt.Println("Must provide -topic")
		flag.PrintDefaults()
		os.Exit(1)
	}

	producer, err := sarama.NewAsyncProducer(strings.Split(brokerList, ","), nil)
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
	n := 0
	hasher := fnv.New64a()
ProducerLoop:
	for {
		hasher.Reset()

		val := []byte(fmt.Sprintf("%010d", n))
		hasher.Write(val)
		val = append(val, fmt.Sprintf(" %x|", hasher.Sum64())...)
		n++

		m := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", n)),
			Value: sarama.ByteEncoder(val),
		}

		select {
		case producer.Input() <- m:
			fmt.Printf("\rSent % 10d", n)
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
