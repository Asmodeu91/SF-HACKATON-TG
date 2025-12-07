package parsing

import (
	"encoding/json"
	"fmt"
	"os"

	. "json_parser_module/entities"
)

func ParseFile(filePatch string) error {
	fmt.Println("### Read as reader ###")
	f, err := os.Open(filePatch)
	if err != nil {
		return err
	}
	defer f.Close()

	var decodedRequest Root
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&decodedRequest)
	if err != nil {
		return err
	}

	var messageCountMap map[string]int
	messageCountMap = make(map[string]int)

	var usernameMap map[string]string
	usernameMap = make(map[string]string)

	for _, value := range decodedRequest.Messages {
		if value.FromId == "" {
			continue
		}

		messageCountMap[value.FromId]++
		usernameMap[value.FromId] = value.From
	}

	// fmt.Printf("%+v\n", decodedRequest)
	for key, value := range messageCountMap {
		fmt.Println(usernameMap[key], ": ", value)
	}

	// fmt.Println(wr.String())

	return nil
}
