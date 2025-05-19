package db

import (
	"fmt"
	"log"
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
		id VAR CHAR PRIMARY KEY, 
		-- Use a UUID for the primary key
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		level VARCHAR NOT NULL CHECK(level IN ('trace', 'debug', 'info', 'warning', 'error', 'critical')),
		details JSON DEFAULT NULL,
		message VARCHAR NOT NULL
	`
}

// Init creates the audit log table if it doesn't exist.
func (t AuditLogTable) Init(db *DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}

// WriteLog inserts a log entry into the audit log table asynchronously.
func (db *DB) WriteLog(entry logging.LogEntry) {
	go func() {
		query := `
			INSERT INTO audit_log (timestamp, level, details, message)
			VALUES (?, ?, ?, ?)
		`
		db.QueueWrite("audit_log", query, entry.Timestamp, entry.Level, entry.Details, entry.Message)
	}()
}

// LogError logs an error with details and a retry count asynchronously.
func (db *DB) LogError(errorType, message string, details *string, retryCount int) {
	go db.WriteLog(logging.LogEntry{
		Level:    "error",
		Details: func() map[string]any {
			if details != nil {
				return map[string]any{"details": *details}
			}
			return nil
		}(),
		Message:  message,
	})
}

// LogWarning logs a warning message with optional details asynchronously.
func (db *DB) LogWarning(message string, details *string) {
	go db.WriteLog(logging.LogEntry{
		Level:   "warning",
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
		log.Printf("Deleted logs older than %d days", days)
	}()
}
