package main

import (
	"json_parser_module/process"
)

func main() {
	var processor = process.NewProcessor()
	defer processor.Close()

	for {
		processor.Process()
	}
}
