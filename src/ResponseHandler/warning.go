package ResponseHandler

import (
	"fmt"
	"time"
)

// Warning represents a warning with a type, message, details, and metadata.
type Warning struct {
	Type      string
	Message   string
	Details   *string
	Metadata  map[string]interface{}
	Timestamp time.Time
}

// NewWarning creates a new Warning instance.
func NewWarning(warningType, message string, details *string, metadata map[string]interface{}) Warning {
	return Warning{
		Type:      warningType,
		Message:   message,
		Details:   details,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}
}

// ToMap converts the Warning to a map for serialization/logging.
func (w Warning) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"type":      w.Type,
		"message":   w.Message,
		"details":   w.Details,
		"metadata":  w.Metadata,
		"timestamp": w.Timestamp.Format(time.RFC3339),
	}
}

// String implements the fmt.Stringer interface for pretty-printing.
func (w Warning) String() string {
	metadataStr := ""
	for k, v := range w.Metadata {
		metadataStr += fmt.Sprintf("%s=%v, ", k, v)
	}

	return fmt.Sprintf("%s: %s\nMetadata: %s", w.Type, w.Message, metadataStr)
}
