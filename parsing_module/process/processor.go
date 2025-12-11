package process

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"json_parser_module/dto"
	"json_parser_module/integration"
	"json_parser_module/parsing"
	"strings"
	"time"
)

type Processor struct {
	minioAdapter integration.MinioAdapter
	kafkaAdapter integration.KafkaAdapter
}

func NewProcessor() *Processor {
	var minioAdapter = integration.NewMinioAdapter()
	var kafkaAdapter = integration.NewKafkaAdapter()

	var processor = Processor{
		minioAdapter: *minioAdapter,
		kafkaAdapter: *kafkaAdapter,
	}

	return &processor
}

// Закрыть все соединения с кафкой
func (processor *Processor) Close() {
	processor.kafkaAdapter.Close()
}

func (processor *Processor) Process() {
	var message, _ = processor.kafkaAdapter.ReadMessage()
	if message == "" {
		return
	}

	go processor.process(message)
}

func (processor *Processor) process(message string) {
	var inputEvent, err = parsing.ParseInputEvent(message)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(&inputEvent)
	var fileName = strings.Split(inputEvent.Storage.ObjectPath, "/")[1]
	var inputFile = processor.minioAdapter.GetFileAsBytes(inputEvent.Storage.Bucket, fileName)
	defer processor.minioAdapter.RemoveFile(inputEvent.Storage.Bucket, fileName)

	var usernameMap map[string]string
	usernameMap, err = parsing.ParseBytes(inputFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	var outputFile = processor.makeOutputFile(usernameMap)
	var outputFileName = processor.makeOutputFileName()
	var checksum string
	checksum, _ = processor.minioAdapter.UploadObject(outputFileName, outputFile, "application/csv")

	// var outputEvent = dto.OutputEvent{
	// 	EventID:          inputEvent.EventID,
	// 	EventType:        inputEvent.EventType,
	// 	EventTimestamp:   time.Now(),
	// 	TaskID:           inputEvent.Task.TaskID,
	// 	Status:           "success",
	// 	ProcessingTimeMS: 0,
	// 	Input: dto.InputData{
	// 		FilePath: inputEvent.Storage.ObjectPath,
	// 		FileSize: inputEvent.File.FileSize,
	// 		FileType: inputEvent.File.FileType,
	// 	},
	// 	Output: dto.OutputData{
	// 		FilePath:    processor.minioAdapter.GetBucket() + "/" + outputFileName,
	// 		FileSize:    int64(len(outputFile)),
	// 		FileType:    "csv",
	// 		DownloadURL: processor.minioAdapter.GetEndpoint() + "/" + processor.minioAdapter.GetBucket() + "/" + outputFileName,
	// 		Checksum:    "",
	// 	},
	// 	Notifications: dto.Notifications{
	// 		TelegramChatID:    inputEvent.Task.ChatID,
	// 		TelegramMessageID: inputEvent.Recovery.OriginalMessageID,
	// 		ShouldSendFile:    len(usernameMap) > 50,
	// 	},
	// }

	var outputEvent = processor.makeResponse(inputEvent, outputFileName, int64(len(outputFile)), checksum, len(usernameMap))

	var json []byte
	json, err = parsing.SerializeToJson(outputEvent)
	if err != nil {
		fmt.Println(err)
		return
	}
	processor.kafkaAdapter.WriteMessage(json)

	fmt.Println("Success")
}

func (processor *Processor) makeResponse(inputEvent *dto.InputEvent, outputFileName string, fileSize int64, checksum string, countUsername int) *dto.OutputEvent {
	var outputEvent = dto.OutputEvent{
		EventID:          inputEvent.EventID,
		EventType:        inputEvent.EventType,
		EventTimestamp:   time.Now(),
		TaskID:           inputEvent.Task.TaskID,
		Status:           "success",
		ProcessingTimeMS: 0,
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

func (processor *Processor) makeOutputFile(usernameMap map[string]string) []byte {
	fmt.Println("Users: ")
	var sb strings.Builder
	for key, value := range usernameMap {
		fmt.Println(key, ": ", value)
		sb.WriteString(key)
		sb.WriteString(";")
		sb.WriteString(value)
		sb.WriteString("\n")
	}

	return []byte(sb.String())
}

// создает уникальное имя файла с датой, временем и случайными символами
func (processor *Processor) makeOutputFileName() string {
	now := time.Now()
	filename := fmt.Sprintf("%s_%s_%s_result.csv", now.Format("20060102"), now.Format("150405"), generateRandomString())
	return filename
}

// generateRandomString генерирует случайную строку длиной 6 символов
func generateRandomString() string {
	randomBytes := make([]byte, 6)
	rand.Read(randomBytes)
	return base64.StdEncoding.EncodeToString(randomBytes)[0:6]
}
