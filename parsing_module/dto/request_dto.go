package dto

import (
	"time"
)

// Event описывает структуру основного события
type InputEvent struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	EventTimestamp time.Time `json:"event_timestamp"` // Предполагается, что timestamp передан в ISO8601 формате

	Task        Task            `json:"task"`
	File        FileInfo        `json:"file"`
	Storage     Storage         `json:"storage"`
	Processing  Processing      `json:"processing"`
	UserContext UserContext     `json:"user_context"`
	Recovery    Recovery        `json:"recovery"`
	Metadata    RequestMetadata `json:"metadata"`
}

// Task представляет информацию о задаче
type Task struct {
	TaskID string `json:"task_id"`
	UserID int64  `json:"user_id"`
	ChatID int64  `json:"chat_id"`
	Source string `json:"source"`
}

// FileInfo хранит метаданные файла
type FileInfo struct {
	OriginalName string `json:"original_name"`
	FileSize     int64  `json:"file_size"`
	FileType     string `json:"file_type"`
	Encoding     string `json:"encoding"`
	Checksum     string `json:"checksum"`
	MimeType     string `json:"mime_type"`
}

// Storage хранит информацию о месте хранения файла
type Storage struct {
	Type        string      `json:"type"`
	Bucket      string      `json:"bucket"`
	ObjectPath  string      `json:"object_path"`
	AccessURL   string      `json:"access_url"`
	Credentials Credentials `json:"credentials"`
}

// Credentials содержат информацию для аутентификации хранилища
type Credentials struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
}

// Processing хранит настройки обработки файла
type Processing struct {
	RequiredOperations []string `json:"required_operations"`
	Priority           string   `json:"priority"`
	TimeoutSeconds     int      `json:"timeout_seconds"`
	ExpectedFormat     string   `json:"expected_format"`
	OutputBucket       string   `json:"output_bucket"`
	CallbackTopic      string   `json:"callback_topic"`
}

// UserContext содержит контекст пользователя
type UserContext struct {
	Username     string `json:"username"`
	FirstName    string `json:"first_name"`
	LanguageCode string `json:"language_code"`
}

// Recovery содержит информацию о восстановлении процесса обработки
type Recovery struct {
	RetryCount        int        `json:"retry_count"`
	LastAttempt       *time.Time `json:"last_attempt"` // Может быть пустым
	OriginalMessageID int64      `json:"original_message_id"`
	BotTokenHash      string     `json:"bot_token_hash"`
}

// Metadata содержит дополнительные мета-данные события
type RequestMetadata struct {
	Version            string `json:"version"`
	Environment        string `json:"environment"`
	ProcessingPipeline string `json:"processing_pipeline"`
	CorrelationID      string `json:"correlation_id"`
}
