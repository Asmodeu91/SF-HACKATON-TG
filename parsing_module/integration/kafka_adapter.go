package integration

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	. "json_parser_module/utils"
)

type KafkaAdapter struct {
	consumer *kafka.Consumer
}

func NewKafkaAdapter() *KafkaAdapter {
	var bootstrap = GetEnv("KAFKA_BOOTSTRAP", "192.168.3.134:9092")
	log.Println(bootstrap)
	var group = GetEnv("KAFKA_CONSUMER_GROUP", "parser")
	var inputTopic = GetEnv("KAFKA_INPUT_TOPIC", "INPUT")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrap,
		"group.id":          group,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		go log.Printf("Unable kafka connect %s: %v", bootstrap, err)
		return nil
	}

	err = consumer.SubscribeTopics([]string{inputTopic}, nil) // Указываем подписку на топик
	if err != nil {
		go log.Printf("Unable kafka subscribe %s: %v", inputTopic, err)
		return nil
	}
	go log.Println("Consumer initialized")

	var adapter = &KafkaAdapter{
		consumer: consumer,
	}

	return adapter
}

func (adapter *KafkaAdapter) ReadMessage() (string, error) {
	var msg, err = adapter.consumer.ReadMessage(time.Minute)
	fmt.Printf("Message on: %s\n", err)
	if err == nil {
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		return string(msg.Value), nil
	} else if !err.(kafka.Error).IsTimeout() {
		// The client will automatically try to recover from all errors.
		// Timeout is not considered an error because it is raised by
		// ReadMessage in absence of messages.
		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	}

	return "", err

	// return nil, nil
	// var event = adapter.consumer.Poll(time.Second * 60)
	// switch e := event.(type) {
	// case *kafka.Message:
	// 	var message = event.String()
	// 	log.Println(e)
	// 	return &message, nil
	// case kafka.Error:
	// 	return nil, e
	// default:
	// 	return nil, nil
	// }
}

func (adapter *KafkaAdapter) Close() {
	adapter.consumer.Close()
}
