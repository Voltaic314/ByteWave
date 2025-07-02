package db

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Voltaic314/ByteWave/code/types/logging" // Import the types package for LogEntry
)

// AuditLogTable defines the schema for the "audit_log" table.
type AuditLogTable struct{}

// Name returns the name of the audit log table.
func (t AuditLogTable) Name() string {
	return "audit_log"
}

// Schema returns the DuckDB-compatible schema definition.
func (t AuditLogTable) Schema() string {
	return `
		id VARCHAR PRIMARY KEY, 
		-- Use a UUID for the primary key
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		level VARCHAR NOT NULL CHECK(level IN ('trace', 'debug', 'info', 'warning', 'error', 'critical')),
		entity VARCHAR DEFAULT NULL,
		-- Entity type: 'worker', 'user', 'system', 'QP', 'Conductor', 'API', 'cloud storage service', etc.
		entity_id VARCHAR DEFAULT NULL,
		-- Unique identifier for the entity (worker ID, service ID, etc.)
		path VARCHAR DEFAULT NULL,
		-- Optional path for task-related logs
		details VARCHAR DEFAULT NULL,
		message VARCHAR NOT NULL
	`
}

// Init creates the audit log table if it doesn't exist.
func (t AuditLogTable) Init(db *DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}

// sanitizeJSON ensures the details map is compatible with DuckDB's JSON schema
func sanitizeJSON(details map[string]any) (string, error) {
	if details == nil {
		return "null", nil
	}

	// Convert any non-JSON compatible types
	sanitized := make(map[string]any)
	for k, v := range details {
		switch val := v.(type) {
		case nil:
			sanitized[k] = nil
		case string, bool, float64, int, int64:
			sanitized[k] = val
		case time.Time:
			sanitized[k] = val.Format("2006-01-02 15:04:05.000000")
		case error:
			sanitized[k] = val.Error()
		default:
			// For any other type, try to marshal it to JSON
			jsonBytes, err := json.Marshal(val)
			if err != nil {
				sanitized[k] = fmt.Sprintf("%v", val)
			} else {
				sanitized[k] = string(jsonBytes)
			}
		}
	}

	// Marshal the sanitized map to JSON
	jsonBytes, err := json.Marshal(sanitized)
	if err != nil {
		return "null", err
	}
	return string(jsonBytes), nil
}

// WriteLog inserts a log entry into the audit log table asynchronously.
func (db *DB) WriteLog(entry logging.LogEntry) {
	go func() {
		query := `
			INSERT OR IGNORE INTO audit_log (timestamp, level, details, message)
			VALUES (?, ?, ?, ?)
		`
		// Format timestamp in ISO 8601 format that DuckDB expects
		timestamp := entry.Timestamp.Format("2006-01-02 15:04:05.000000")

		// Sanitize the details JSON
		detailsJSON, err := sanitizeJSON(entry.Details)
		if err != nil {
			detailsJSON = "null"
		}

		db.QueueWrite("audit_log", query, timestamp, entry.Level, detailsJSON, entry.Message)
	}()
}

// LogError logs an error with details and a retry count asynchronously.
func (db *DB) LogError(errorType, message string, details *string, retryCount int) {
	go db.WriteLog(logging.LogEntry{
		Level: "error",
		Details: func() map[string]any {
			if details != nil {
				return map[string]any{"details": *details}
			}
			return nil
		}(),
		Message: message,
	})
}

// LogWarning logs a warning message with optional details asynchronously.
func (db *DB) LogWarning(message string, details *string) {
	go db.WriteLog(logging.LogEntry{
		Level: "warning",
		Details: func() map[string]any {
			if details != nil {
				return map[string]any{"details": *details}
			}
			return nil
		}(),
		Message: message,
	})
}

// LogInfo logs an informational message asynchronously.
func (db *DB) LogInfo(message string) {
	go db.WriteLog(logging.LogEntry{
		Level:   "info",
		Message: message,
	})
}

// CleanOldLogs deletes log entries older than a specified duration asynchronously.
func (db *DB) CleanOldLogs(retentionPeriod time.Duration) {
	go func() {
		days := int(retentionPeriod.Hours() / 24)
		query := fmt.Sprintf(`DELETE FROM audit_log WHERE timestamp < NOW() - INTERVAL '%d days'`, days)
		db.QueueWrite("audit_log", query)
	}()
}
