package main

import (
	"fmt"
	. "json_parser_module/integration"
	. "json_parser_module/parsing"
	"strings"
)

func main() {
	var minioAdapter = NewMinioAdapter()
	var kafkaAdapter = NewKafkaAdapter()

	// var run = true
	// for run == true {
	// 	var message, err = kafkaAdapter.ReadMessage()
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	var input, _ = ParseInputEvent(message)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	fmt.Println(input)
	// }

	var message, err = kafkaAdapter.ReadMessage()
	if err != nil {
		panic(err)
	}

	var input, _ = ParseInputEvent(message)
	if err != nil {
		panic(err)
	}

	fmt.Println(input)

	var usernameMap map[string]string
	var fileName = strings.Split(input.Storage.ObjectPath, "/")[1]
	usernameMap, err = ParseBytes(minioAdapter.GetFileAsBytes(input.Storage.Bucket, fileName))
	if err != nil {
		panic(err)
	}
	defer minioAdapter.RemoveFile(input.Storage.Bucket, fileName)

	fmt.Println("")
	fmt.Println("Users: ")
	for key, value := range usernameMap {
		fmt.Println(key, ": ", value)
	}
}
