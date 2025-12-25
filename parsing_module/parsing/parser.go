package parsing

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	. "json_parser_module/dto"
)

func ParseInputEventAsByteArray(input []byte) (*InputEvent, error) {
	var decodedInput InputEvent
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal([]byte(input), &decodedInput)
	if err != nil {
		return nil, err
	}

	return &decodedInput, nil
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
