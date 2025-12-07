package main

import (
	. "json_parser_module/parsing"
)

func main() {
	err := ParseFile("result.json")
	if err != nil {
		panic(err)
	}
}
