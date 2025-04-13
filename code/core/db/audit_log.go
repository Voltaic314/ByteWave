package db

import (
	"fmt"
	"log"
	"time"
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
		id BIGINT,
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		category VARCHAR NOT NULL CHECK(category IN ('info', 'warning', 'error')),
		error_type VARCHAR DEFAULT NULL,
		details JSON DEFAULT NULL,
		message VARCHAR NOT NULL
	`
}

// Init creates the audit log table if it doesn't exist.
func (t AuditLogTable) Init(db *DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}

// LogEntry represents an entry in the audit log.
type LogEntry struct {
	Category   string
	ErrorType  *string
	Details    *string
	Message    string
}

// WriteLog inserts a log entry into the audit log table asynchronously.
func (db *DB) WriteLog(entry LogEntry) {
	go func() {
		query := `
			INSERT INTO audit_log (category, error_type, details, message) 
			VALUES (?, ?, ?, ?)
		`
		db.QueueWrite("audit_log", query, entry.Category, entry.ErrorType, entry.Details, entry.Message)
	}()
}

// LogError logs an error with details and a retry count asynchronously.
func (db *DB) LogError(errorType, message string, details *string, retryCount int) {
	go db.WriteLog(LogEntry{
		Category:  "error",
		ErrorType: &errorType,
		Details:   details,
		Message:   message,
	})
}

// LogWarning logs a warning message with optional details asynchronously.
func (db *DB) LogWarning(message string, details *string) {
	go db.WriteLog(LogEntry{
		Category: "warning",
		Details:  details,
		Message:  message,
	})
}

// LogInfo logs an informational message asynchronously.
func (db *DB) LogInfo(message string) {
	go db.WriteLog(LogEntry{
		Category: "info",
		Message:  message,
	})
}

// CleanOldLogs deletes log entries older than a specified duration asynchronously.
func (db *DB) CleanOldLogs(retentionPeriod time.Duration) {
	go func() {
		// DuckDB uses: WHERE timestamp < NOW() - INTERVAL 'N days'
		days := int(retentionPeriod.Hours() / 24)
		query := fmt.Sprintf(`DELETE FROM audit_log WHERE timestamp < NOW() - INTERVAL '%d days'`, days)

		db.QueueWrite("audit_log", query)
		log.Printf("Deleted logs older than %d days", days)
	}()
}

// GenerateErrorID returns a simple epoch-based ID.
func GenerateErrorID() int64 {
	return time.Now().Unix()
}
