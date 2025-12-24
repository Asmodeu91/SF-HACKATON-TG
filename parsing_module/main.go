package main

import (
	"json_parser_module/integration"
	"json_parser_module/process"
)

func main() {
	kafkaAdapter := integration.NewKafkaAdapter()
	defer kafkaAdapter.Close()

	var processor = process.NewProcessor()
	kafkaAdapter.Listen(func(msg []byte) error {
		return processor.Process(msg, func(value any) error {
			return kafkaAdapter.Send(&value)
		})
	})
}
