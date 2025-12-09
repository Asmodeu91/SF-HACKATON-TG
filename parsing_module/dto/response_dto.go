package dto

import (
	"time"
)

// Event описывает структуру обработанного события
type OutputEvent struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	EventTimestamp time.Time `json:"event_timestamp"` // Предполагается, что timestamp передан в ISO8601 формате

	TaskID           string `json:"task_id"`
	Status           string `json:"status"`
	ProcessingTimeMS int64  `json:"processing_time_ms"`

	Input         InputData     `json:"input"`
	Output        OutputData    `json:"output"`
	Results       Results       `json:"results"`
	Metadata      Metadata      `json:"metadata"`
	Notifications Notifications `json:"notifications"`
}

// InputData содержит информацию о файле перед обработкой
type InputData struct {
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
	FileType string `json:"file_type"`
}

// OutputData содержит информацию о результате обработки
type OutputData struct {
	FilePath    string `json:"file_path"`
	FileSize    int64  `json:"file_size"`
	FileType    string `json:"file_type"`
	DownloadURL string `json:"download_url"`
	Checksum    string `json:"checksum"`
}

// Results хранит результаты этапов обработки
type Results struct {
	Validation     ValidationResult     `json:"validation"`
	Transformation TransformationResult `json:"transformation"`
	Analysis       AnalysisResult       `json:"analysis"`
}

// ValidationResult показывает статус проверки валидности данных
type ValidationResult struct {
	Status           string  `json:"status"`
	SchemaValid      bool    `json:"schema_valid"`
	DataQualityScore float64 `json:"data_quality_score"`
}

// TransformationResult отображает статистику преобразования файлов
type TransformationResult struct {
	RecordsProcessed       int      `json:"records_processed"`
	RecordsTransformed     int      `json:"records_transformed"`
	TransformationsApplied []string `json:"transformations_applied"`
}

// AnalysisResult предоставляет итоговую аналитику после обработки
type AnalysisResult struct {
	RecordCount    int             `json:"record_count"`
	FieldCount     int             `json:"field_count"`
	DetectedIssues []string        `json:"detected_issues"`
	Statistics     FieldStatistics `json:"statistics"`
}

// FieldStatistics отражает типы полей и их количество
type FieldStatistics struct {
	NumericFields int `json:"numeric_fields"`
	TextFields    int `json:"text_fields"`
	DateFields    int `json:"date_fields"`
}

// Metadata содержит дополнительную служебную информацию
type Metadata struct {
	ProcessorVersion string `json:"processor_version"`
	ProcessedBy      string `json:"processed_by"`
	ProcessingNode   string `json:"processing_node"`
	CorrelationID    string `json:"correlation_id"`
}

// Notifications управляет уведомлениями и отправкой результатов
type Notifications struct {
	TelegramChatID    int64 `json:"telegram_chat_id"`
	TelegramMessageID int64 `json:"telegram_message_id"`
	ShouldSendFile    bool  `json:"should_send_file"`
}

// Event описывает событие неудачной обработки файла
type ErrorEvent struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	EventTimestamp time.Time `json:"event_timestamp"` // Предполагается, что timestamp передается в формате ISO8601

	TaskID           string `json:"task_id"`
	Status           string `json:"status"`
	ErrorCode        string `json:"error_code"`
	ErrorMessage     string `json:"error_message"`
	ProcessingTimeMS int64  `json:"processing_time_ms"`

	Input         InputData            `json:"input"`
	ErrorDetails  ErrorDetails         `json:"error_details"`
	Recovery      RecoveryOptions      `json:"recovery"`
	Metadata      Metadata             `json:"metadata"`
	Notifications NotificationSettings `json:"notifications"`
}

// ErrorDetails хранит подробную информацию об ошибке
type ErrorDetails struct {
	ErrorType    string      `json:"error_type"`
	Field        string      `json:"field"`
	ExpectedType string      `json:"expected_type"`
	ActualValue  interface{} `json:"actual_value"` // Поле может иметь произвольный тип
	LineNumber   int         `json:"line_number"`
	Position     int         `json:"position"`
	Suggestion   string      `json:"suggestion"`
}

// RecoveryOptions определяет стратегию восстановления после сбоя
type RecoveryOptions struct {
	CanRetry          bool   `json:"can_retry"`
	RetryAfterSeconds int    `json:"retry_after_seconds"`
	SuggestedFix      string `json:"suggested_fix"`
	MaxRetries        int    `json:"max_retries"`
}

// NotificationSettings управляют настройками уведомлений
type NotificationSettings struct {
	TelegramChatID    int64 `json:"telegram_chat_id"`
	TelegramMessageID int64 `json:"telegram_message_id"`
	ShouldNotifyUser  bool  `json:"should_notify_user"`
}
