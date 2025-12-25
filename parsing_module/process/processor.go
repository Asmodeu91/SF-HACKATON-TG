package process

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"json_parser_module/dto"
	"json_parser_module/integration"
	"json_parser_module/parsing"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/zillow/zkafka/v2"
)

type Processor struct {
	minioAdapter integration.MinioAdapter
	writeFunc    *func(value any) (*zkafka.Response, error)
}

func NewProcessor() *Processor {
	var minioAdapter = integration.NewMinioAdapter()

	var processor = Processor{
		minioAdapter: *minioAdapter,
	}

	return &processor
}

// Процесс обработки сообщения и парсинга файла
func (processor *Processor) Process(message []byte, sendResponse func(value any) error) error {
	go processor.process(message, sendResponse)

	return nil
}

// Процесс обработки сообщения и парсинга файла
func (processor *Processor) process(message []byte, sendResponse func(value any) error) {
	startTime := time.Now()

	var inputEvent, err = parsing.ParseInputEventAsByteArray(message)
	if err != nil {
		log.Println(err)
		var outputEvent = processor.makeResponseError(inputEvent, err, startTime)
		if err = sendResponse(&outputEvent); err != nil {
			log.Panic(err)
		}
		return
	}

	log.Println(&inputEvent)
	var fileName = strings.Split(inputEvent.Storage.ObjectPath, "/")[1]
	var inputFile []byte
	inputFile, err = processor.minioAdapter.GetFileAsBytes(inputEvent.Storage.Bucket, fileName)
	if err != nil {
		log.Println(err)
		var outputEvent = processor.makeResponseError(inputEvent, err, startTime)
		if err = sendResponse(&outputEvent); err != nil {
			log.Panic(err)
		}
		return
	}
	defer processor.minioAdapter.RemoveFile(inputEvent.Storage.Bucket, fileName)

	var userInfoMap dto.UserInfoSlice
	userInfoMap, err = parsing.ParseBytes(inputFile)
	if err != nil {
		log.Println(err)
		var outputEvent = processor.makeResponseError(inputEvent, err, startTime)
		if err = sendResponse(&outputEvent); err != nil {
			log.Panic(err)
		}
		return
	}

	var outputFile = processor.makeOutputFile(userInfoMap)
	var outputFileName = processor.makeOutputFileName()
	var checksum string
	checksum, err = processor.minioAdapter.UploadObject(outputFileName, outputFile, "application/csv")
	if err != nil {
		log.Println(err)
		var outputEvent = processor.makeResponseError(inputEvent, err, startTime)
		if err = sendResponse(&outputEvent); err != nil {
			log.Panic(err)
		}
		return
	}

	var outputEvent = processor.makeResponseSuccess(inputEvent, outputFileName, int64(len(outputFile)), checksum, len(userInfoMap), startTime)
	if err = sendResponse(&outputEvent); err != nil {
		log.Panic(err)
		return
	}

	log.Println("Success")
}

// Формирование ответного сообщения
func (processor *Processor) makeResponseSuccess(inputEvent *dto.InputEvent, outputFileName string, fileSize int64, checksum string, countUsername int, startTime time.Time) *dto.OutputEvent {
	var outputEvent = dto.OutputEvent{
		EventID:          inputEvent.EventID,
		EventType:        "file_processed",
		EventTimestamp:   time.Now(),
		TaskID:           inputEvent.Task.TaskID,
		Status:           "success",
		ProcessingTimeMS: time.Since(startTime).Milliseconds(),
		Input: dto.InputData{
			FilePath: inputEvent.Storage.ObjectPath,
			FileSize: inputEvent.File.FileSize,
			FileType: inputEvent.File.FileType,
		},
		Output: dto.OutputData{
			FilePath:    processor.minioAdapter.GetBucket() + "/" + outputFileName,
			FileSize:    fileSize,
			FileType:    "csv",
			DownloadURL: processor.minioAdapter.GetEndpoint() + "/" + processor.minioAdapter.GetBucket() + "/" + outputFileName,
			Checksum:    checksum,
		},
		Results: dto.Results{
			Validation: dto.ValidationResult{
				Status:           "valid",
				SchemaValid:      true,
				DataQualityScore: 0,
			},
			Transformation: dto.TransformationResult{
				RecordsProcessed:       countUsername,
				RecordsTransformed:     countUsername,
				TransformationsApplied: []string{"cleanup", "normalize", "enrich"},
			},
			Analysis: dto.AnalysisResult{
				RecordCount:    countUsername,
				FieldCount:     2,
				DetectedIssues: []string{},
				Statistics: dto.FieldStatistics{
					NumericFields: 0,
					TextFields:    2,
					DateFields:    0,
				},
			},
		},
		Metadata: dto.Metadata{
			ProcessorVersion: "0.1",
			ProcessedBy:      "parsing_module",
			ProcessingNode:   "worker-01",
			CorrelationID:    "corr_xyz789",
		},
		Notifications: dto.Notifications{
			TelegramChatID:    inputEvent.Task.ChatID,
			TelegramMessageID: inputEvent.Recovery.OriginalMessageID,
			ShouldSendFile:    countUsername > 50,
		},
	}

	return &outputEvent
}

// Формирование ответа с ошибкой
func (processor *Processor) makeResponseError(inputEvent *dto.InputEvent, err error, startTime time.Time) *dto.ErrorEvent {
	var outputEvent = dto.ErrorEvent{
		EventID:          inputEvent.EventID,
		EventType:        "file_processing_failed",
		EventTimestamp:   time.Now(),
		TaskID:           inputEvent.Task.TaskID,
		Status:           "error",
		ErrorCode:        "processing error",
		ErrorMessage:     err.Error(),
		ProcessingTimeMS: time.Since(startTime).Milliseconds(),
		Input: dto.InputData{
			FilePath: inputEvent.Storage.ObjectPath,
			FileSize: inputEvent.File.FileSize,
			FileType: inputEvent.File.FileType,
		},
		ErrorDetails: dto.ErrorDetails{},
		Recovery:     dto.RecoveryOptions{},
		Metadata: dto.Metadata{
			ProcessorVersion: "0.1",
			ProcessedBy:      "parsing_module",
			ProcessingNode:   "worker-01",
			CorrelationID:    "corr_xyz789",
		},
		Notifications: dto.NotificationSettings{
			TelegramChatID:    inputEvent.Task.ChatID,
			TelegramMessageID: inputEvent.Recovery.OriginalMessageID,
			ShouldNotifyUser:  true,
		},
	}

	return &outputEvent
}

// Метод собирает файл с результатами
func (processor *Processor) makeOutputFile(userInfoMap dto.UserInfoSlice) []byte {
	log.Println("Users: ")
	var sb strings.Builder
	for key, value := range userInfoMap {
		log.Println(key, ": ", value)
		sb.WriteString(value.UserId)
		sb.WriteString(";")
		sb.WriteString(value.UserName)
		sb.WriteString(";")
		sb.WriteString(strconv.Itoa(value.MessageCount))
		sb.WriteString(";")
		sb.WriteString(strconv.Itoa(value.PercentMsg))
		sb.WriteString("\n")
	}

	return []byte(sb.String())
}

// создает уникальное имя файла с датой, временем и случайными символами
func (processor *Processor) makeOutputFileName() string {
	now := time.Now()
	filename := fmt.Sprintf("%s_%s_%s_result.csv", now.Format("20060102"), now.Format("150405"), generateRandomString())
	return strings.Replace(filename, "/", "-", -1)
}

// generateRandomString генерирует случайную строку длиной 6 символов
func generateRandomString() string {
	randomBytes := make([]byte, 6)
	rand.Read(randomBytes)
	return base64.StdEncoding.EncodeToString(randomBytes)[0:6]
}
