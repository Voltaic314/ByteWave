// Package logging provides types for logging operations and data structures.
package logging

import (
	"time"
)

// LogEntry represents a structured log entry compatible with the DuckDB schema.
type LogEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Entity    string         `json:"entity,omitempty"`    // Entity type: 'worker', 'user', 'system', etc.
	EntityID  string         `json:"entity_id,omitempty"` // Unique identifier for the entity
	Path      string         `json:"path,omitempty"`      // Optional path for task-related logs
	Details   map[string]any `json:"details,omitempty"`   // Optional details
	Message   string         `json:"message"`
	Action    string         `json:"action,omitempty"`    // Action like 'CREATE_WORKER'
	Queue     string         `json:"queue,omitempty"`     // Queue name
}

// QueueAcronyms maps queue names to logical acronyms for logging subtopic filtering.
var QueueAcronyms = map[string]string{
	"src-traversal": "src",
	"dst-traversal": "dst",
	"upload":        "upload",
}

// TableAcronyms maps table names to logical acronyms for logging subtopic filtering.
var TableAcronyms = map[string]string{
	"audit_log":          "logs",
	"source_nodes":       "src",
	"destination_nodes":  "dst",
}

// MigrationError represents an error in the migration process.
type MigrationError struct {
	TaskID   string `json:"task_id"`
	ErrorMsg string `json:"error_msg"`
}

// NewMigrationError creates a new MigrationError with a generated UUID for the ID.
func NewMigrationError(taskID, errorMsg string) MigrationError {
	return MigrationError{
		TaskID:   taskID,
		ErrorMsg: errorMsg,
	}
}
