package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

var shouldConsume = "local-test-consumer"

func main() {
	client, err := sarama.NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalln(err)
	}
	defer consumer.Close()

	topics, err := consumer.Topics()
	if err != nil {
		log.Fatalf("consumer.Topics() %+v\n", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consume(topics, consumer)

	<-signals
	log.Println("finishing consumer")
	os.Exit(1)
}

func consume(topics []string, master sarama.Consumer) {
	for _, topic := range topics {
		topic := topic

		if !strings.Contains(topic, shouldConsume) {
			continue
		}

		log.Println("Consuming topic", topic)
		go func() {
			partitions, _ := master.Partitions(topic)
			for _, partition := range partitions {
				consumer, err := master.ConsumePartition(topic, partition, sarama.ReceiveTime)
				if nil != err {
					fmt.Printf("Topic %v Partitions: %v", topic, partitions)
					panic(err)
				}

				log.Println(consumer.HighWaterMarkOffset())

				go func(topic string, consumer sarama.PartitionConsumer) {
					for {
						select {
						case consumerError := <-consumer.Errors():
							fmt.Println("consumerError: ", consumerError.Err)
						case msg := <-consumer.Messages():
							fmt.Println("Got message on topic ", topic, msg.Value)
						}
					}
				}(topic, consumer)
			}
		}()
	}
}
