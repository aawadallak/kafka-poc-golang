package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	defer c.Close()

	if err := c.Subscribe("local-test-consumer", nil); err != nil {
		log.Fatalln(err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			panic(err)
		}

		log.Println(msg)
	}
}
