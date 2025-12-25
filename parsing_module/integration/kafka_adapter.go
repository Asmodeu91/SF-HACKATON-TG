package integration

import (
	"context"
	"json_parser_module/utils"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zillow/zfmt"
	"github.com/zillow/zkafka/v2"
)

type KafkaAdapter struct {
	context *context.Context
	config  *zkafka.ConsumerTopicConfig
	client  *zkafka.Client
	writer  *zkafka.Writer
}

func NewKafkaAdapter() *KafkaAdapter {
	var bootstrap = utils.GetEnv("KAFKA_BOOTSTRAP", "kafka:9092")
	var inputTopic = utils.GetEnv("KAFKA_INPUT_TOPIC", "INPUT")
	var outputTopic = utils.GetEnv("KAFKA_OUTPUT_TOPIC", "OUTPUT")
	var consumerGroup = utils.GetEnv("KAFKA_CONSUMER_GROUP", "parser")

	context := context.Background()

	client := zkafka.NewClient(zkafka.Config{
		BootstrapServers: []string{bootstrap},
	})
	writer, _ := client.Writer(context, zkafka.ProducerTopicConfig{
		ClientID:  "service-parser",
		Topic:     outputTopic,
		Formatter: zfmt.JSONFmt,
	})

	config := zkafka.ConsumerTopicConfig{
		ClientID:  "service-parser",
		GroupID:   consumerGroup,
		Topic:     inputTopic,
		Formatter: zfmt.JSONFmt,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}

	return &KafkaAdapter{
		context: &context,
		config:  &config,
		client:  client,
		writer:  &writer,
	}
}

// Слушаем топик входящих сообщений
func (adapter *KafkaAdapter) Listen(process func(msg []byte) error) {
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	shutdown := make(chan struct{})

	go func() {
		<-stopCh
		close(shutdown)
	}()

	callback := func(_ context.Context, msg *zkafka.Message) error {
		log.Printf(" offset: %d, partition: %d\n", msg.Offset, msg.Partition)
		return process(msg.Value())
	}

	wf := zkafka.NewWorkFactory(adapter.client)
	work := wf.CreateWithFunc(*adapter.config, callback, zkafka.Speedup(5))
	if err := work.Run(*adapter.context, shutdown); err != nil {
		log.Panic(err)
	}
}

// Отправка сообщения в топик исходящих сообщений
func (adapter *KafkaAdapter) Send(value *any) error {
	writer := *adapter.writer
	response, err := writer.Write(*adapter.context, *value)
	log.Printf("Response: %+v\n", response)
	return err
}

func (adapter *KafkaAdapter) Close() {
	client := adapter.client
	client.Close()
	writer := *adapter.writer
	writer.Close()
}
