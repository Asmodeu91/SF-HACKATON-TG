package main

import (
	"fmt"
	. "json_parser_module/integration"
	. "json_parser_module/parsing"
)

func main() {
	var adapter = NewMinioAdapter()
	var usernameMap, err = ParseBytes(adapter.GetFileAsBytes("result.json"))
	if err != nil {
		panic(err)
	}
	defer adapter.RemoveFile("result.json")

	fmt.Println("")
	fmt.Println("Users: ")
	for key, value := range usernameMap {
		fmt.Println(key, ": ", value)
	}
}
