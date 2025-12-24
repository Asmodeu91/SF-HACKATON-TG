package process

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"json_parser_module/dto"
	"json_parser_module/integration"
	"json_parser_module/parsing"
	"log"
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

func (processor *Processor) Process(message []byte, sendResponse func(value any) error) error {
	go processor.process(message, sendResponse)

	return nil
}

func (processor *Processor) process(message []byte, sendResponse func(value any) error) {
	var inputEvent, err = parsing.ParseInputEventAsByteArray(message)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println(&inputEvent)
	var fileName = strings.Split(inputEvent.Storage.ObjectPath, "/")[1]
	var inputFile = processor.minioAdapter.GetFileAsBytes(inputEvent.Storage.Bucket, fileName)
	defer processor.minioAdapter.RemoveFile(inputEvent.Storage.Bucket, fileName)

	var usernameMap map[string]string
	usernameMap, err = parsing.ParseBytes(inputFile)
	if err != nil {
		log.Println(err)
		return
	}

	var outputFile = processor.makeOutputFile(usernameMap)
	var outputFileName = processor.makeOutputFileName()
	var checksum string
	checksum, _ = processor.minioAdapter.UploadObject(outputFileName, outputFile, "application/csv")

	var outputEvent = processor.makeResponse(inputEvent, outputFileName, int64(len(outputFile)), checksum, len(usernameMap))

	err = sendResponse(&outputEvent)
	if err != nil {
		log.Panic(err)
	}

	log.Println("Success")
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
	log.Println("Users: ")
	var sb strings.Builder
	for key, value := range usernameMap {
		log.Println(key, ": ", value)
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
	return strings.Replace(filename, "/", "-", -1)
}

// generateRandomString генерирует случайную строку длиной 6 символов
func generateRandomString() string {
	randomBytes := make([]byte, 6)
	rand.Read(randomBytes)
	return base64.StdEncoding.EncodeToString(randomBytes)[0:6]
}
