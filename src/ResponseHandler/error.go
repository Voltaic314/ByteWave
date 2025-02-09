package responsehandler

import (
	"fmt"
	"time"
)

// Error represents an error with a type, message, details, and metadata.
type Error struct {
	Type      string
	Message   string
	Details   *string
	Metadata  map[string]interface{}
	Timestamp time.Time
}

// NewError creates a new Error instance.
func NewError(errorType, message string, details *string, metadata map[string]interface{}) Error {
	return Error{
		Type:      errorType,
		Message:   message,
		Details:   details,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}
}

// ToMap converts the Error to a map for serialization/logging.
func (e Error) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"type":      e.Type,
		"message":   e.Message,
		"details":   e.Details,
		"metadata":  e.Metadata,
		"timestamp": e.Timestamp.Format(time.RFC3339),
	}
}

// String implements the fmt.Stringer interface for pretty-printing.
func (e Error) String() string {
	metadataStr := ""
	for k, v := range e.Metadata {
		metadataStr += fmt.Sprintf("%s=%v, ", k, v)
	}

	return fmt.Sprintf("%s: %s\nMetadata: %s", e.Type, e.Message, metadataStr)
}
