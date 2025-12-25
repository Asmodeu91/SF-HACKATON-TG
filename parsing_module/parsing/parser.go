package parsing

import (
	"fmt"
	"sort"

	jsoniter "github.com/json-iterator/go"

	. "json_parser_module/dto"
)

// Парсинг входящего сообщения
func ParseInputEventAsByteArray(input []byte) (*InputEvent, error) {
	var decodedInput InputEvent
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal([]byte(input), &decodedInput)
	if err != nil {
		return nil, err
	}

	return &decodedInput, nil
}

// Парсинг выгрузки из telegram
func ParseBytes(input []byte) (UserInfoSlice, error) {
	fmt.Println("### Read from byte array ###")

	var decodedRequest Root
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal(input, &decodedRequest)
	if err != nil {
		return nil, err
	}

	var resultMap map[string]*UserInfo = make(map[string]*UserInfo)
	var counter int = 0
	for _, value := range decodedRequest.Messages {
		counter++
		if value.FromId == "" {
			continue
		}

		var info UserInfo
		if resultMap[value.FromId] == nil {
			info = UserInfo{
				UserId:       value.FromId,
				UserName:     value.From,
				MessageCount: 1,
			}
		} else {
			info = *resultMap[value.FromId]
			info.MessageCount++
		}

		resultMap[value.FromId] = &info
	}
	fmt.Println("Message count: ", counter)

	// Считаем проценты (процент от общего количества сообщений) и собираем в итоговую структуру
	result := make(UserInfoSlice, 0, len(resultMap))
	for key := range resultMap {
		var info *UserInfo = resultMap[key]
		var percent = (info.MessageCount * 100) / counter
		info.PercentMsg = percent
		result = append(result, info)
	}

	sort.Sort(result)

	return result, nil
}
