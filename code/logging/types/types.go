package types

import (
	"time"
)

// LogEntry represents a structured log entry.
type LogEntry struct {
	Timestamp time.Time     `json:"timestamp"`
	Level     string        `json:"level"`
	Message   string        `json:"message"`
	Details   map[string]any `json:"details,omitempty"` // Optional details
}

// MigrationError represents an error in the migration process.
type MigrationError struct {
	TaskID   string `json:"task_id"`
	ErrorMsg string `json:"error_msg"`
}

// NewMigrationError creates a new MigrationError with a generated UUID for the ID.
func NewMigrationError(taskID, errorMsg string) MigrationError {
	return MigrationError{
		TaskID:    taskID,
		ErrorMsg:  errorMsg,
	}
}
