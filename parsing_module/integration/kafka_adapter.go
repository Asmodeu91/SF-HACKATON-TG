package integration

import (
	"context"
	"fmt"
	"json_parser_module/utils"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaAdapter struct {
	reader *kafka.Reader
	writer *kafka.Writer
}

func NewKafkaAdapter() *KafkaAdapter {
	var bootstrap = utils.GetEnv("KAFKA_BOOTSTRAP", "kafka:9092")
	var inputTopic = utils.GetEnv("KAFKA_INPUT_TOPIC", "INPUT")
	var outputTopic = utils.GetEnv("KAFKA_OUTPUT_TOPIC", "OUTPUT")
	var consumerGroup = utils.GetEnv("KAFKA_CONSUMER_GROUP", "parser")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{bootstrap},
		Topic:   inputTopic,
		GroupID: consumerGroup,
	})
	log.Println("Consumer initialized")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{bootstrap},
		Topic:        outputTopic,
		RequiredAcks: -1,                  // Подтверждение от всех реплик
		MaxAttempts:  10,                  //кол-во попыток доставки(по умолчанию всегда 10)
		BatchSize:    100,                 // Ограничение на количество сообщений(по дефолту 100)
		WriteTimeout: 10 * time.Second,    //время ожидания для записи(по умолчанию 10сек)
		Balancer:     &kafka.RoundRobin{}, //балансировщик.
	})
	log.Println("Producer initialized")

	var adapter = &KafkaAdapter{
		reader: reader,
		writer: writer,
	}

	return adapter
}

// Чтение сообщений из кафки
func (adapter *KafkaAdapter) ReadMessage() (string, error) {
	var message, err = adapter.reader.ReadMessage(context.Background())
	if err != nil {
		fmt.Printf("Consumer error: %v (%v)\n", err, message)
		return "", err
	}
	fmt.Printf("Message on %s: %s\n", message.Topic, string(message.Value))

	return string(message.Value), nil
}

// Отправка сообщений в кафку
func (adapter *KafkaAdapter) WriteMessage(message []byte) error {
	var err = adapter.writer.WriteMessages(context.Background(), kafka.Message{
		Value: message,
	})
	if err != nil {
		fmt.Printf("Producer error: %v (%v)\n", err, string(message))
		return err
	}

	fmt.Printf("Message to %s: %s\n", adapter.writer.Topic, string(message))

	return nil
}

// Закрыть все соединения с кафкой
func (adapter *KafkaAdapter) Close() {
	adapter.reader.Close()
	adapter.writer.Close()
}
