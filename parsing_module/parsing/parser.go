package parsing

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"

	jsoniter "github.com/json-iterator/go"

	. "json_parser_module/dto"

	"github.com/PuerkitoBio/goquery"
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

func ParseBytesAsTelegramFile(input []byte, file_type string) (UserInfoSlice, error) {
	if file_type == "json" {
		return parseBytesAsJson(input)
	} else if file_type == "html" {
		return parseBytesAsHTML(input)
	} else {
		return nil, fmt.Errorf("Unknown file type %s", file_type)
	}
}

// Парсинг выгрузки из telegram
func parseBytesAsJson(input []byte) (UserInfoSlice, error) {
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

func ParseHtmlFromFile(filePatch string) {
	fmt.Println("### Read as reader ###")
	v, err := ioutil.ReadFile(filePatch) //read the content of file
	if err != nil {
		fmt.Println(err)
		return
	}

	result, _ := parseBytesAsHTML(v)
	for _, value := range result {
		log.Println(value.UserName, ": ", value.MessageCount)
	}
}

// Функция parseHTML обрабатывает полученные элементы класса "message"
func parseBytesAsHTML(input []byte) (UserInfoSlice, error) {
	reader := bytes.NewReader(input)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		log.Fatal(err)
	}

	var resultMap map[string]*UserInfo = make(map[string]*UserInfo)
	var counter int = 0
	doc.Find("div.message").Each(func(i int, s *goquery.Selection) {
		s.Find("div.from_name").Each(func(i int, s *goquery.Selection) {
			if s.Parent().HasClass("forwarded") {
				return
			}
			key := strings.TrimSpace(strings.ReplaceAll(strings.Trim(s.Text(), "\n"), "via @gif", ""))
			counter++

			var info UserInfo
			if resultMap[key] == nil {
				info = UserInfo{
					UserId:       "N/A",
					UserName:     key,
					MessageCount: 1,
				}
			} else {
				info = *resultMap[key]
				info.MessageCount++
			}

			resultMap[key] = &info
		})
	})
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
