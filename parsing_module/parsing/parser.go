package parsing

import (
	"fmt"
	"os"

	jsoniter "github.com/json-iterator/go"

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
	decoder := jsoniter.NewDecoder(f)
	err = decoder.Decode(&decodedRequest)
	if err != nil {
		return err
	}

	var messageCountMap map[string]int = make(map[string]int)
	var usernameMap map[string]string = make(map[string]string)

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

func ParseBytes(input []byte) (map[string]string, error) {
	fmt.Println("### Read from byte array ###")

	var decodedRequest Root
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal(input, &decodedRequest)
	if err != nil {
		return nil, err
	}

	var resultMap map[string]string = make(map[string]string)
	var counter int = 0
	for _, value := range decodedRequest.Messages {
		counter++
		if value.FromId == "" {
			continue
		}

		resultMap[value.FromId] = value.From
	}
	fmt.Println("Message count: ", counter)

	return resultMap, nil
}
