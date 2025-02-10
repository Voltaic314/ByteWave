package db

import (
	"log"
	"time"
)

// AuditLogTable defines the schema for the "audit_log" table.
type AuditLogTable struct{}

// Name returns the name of the audit log table.
func (t AuditLogTable) Name() string {
	return "audit_log"
}

// Schema returns the schema definition for the audit log table.
func (t AuditLogTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		category TEXT NOT NULL CHECK(category IN ('info', 'warning', 'error')),
		error_type TEXT DEFAULT NULL,
		details TEXT DEFAULT NULL,
		retry_count INTEGER DEFAULT 0,
		message TEXT NOT NULL
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
	RetryCount int
	Message    string
}

// WriteLog inserts a log entry into the audit log table.
func (db *DB) WriteLog(entry LogEntry) {
	query := `
		INSERT INTO audit_log (category, error_type, details, retry_count, message) 
		VALUES (?, ?, ?, ?, ?)
	`
	db.QueueWrite("audit_log", query, entry.Category, entry.ErrorType, entry.Details, entry.RetryCount, entry.Message)
}

// LogError logs an error with details and a retry count.
func (db *DB) LogError(errorType, message string, details *string, retryCount int) {
	db.WriteLog(LogEntry{
		Category:   "error",
		ErrorType:  &errorType,
		Details:    details,
		RetryCount: retryCount,
		Message:    message,
	})
}

// LogWarning logs a warning message with optional details.
func (db *DB) LogWarning(message string, details *string) {
	db.WriteLog(LogEntry{
		Category:   "warning",
		ErrorType:  nil,
		Details:    details,
		RetryCount: 0,
		Message:    message,
	})
}

// LogInfo logs an informational message.
func (db *DB) LogInfo(message string) {
	db.WriteLog(LogEntry{
		Category:   "info",
		ErrorType:  nil,
		Details:    nil,
		RetryCount: 0,
		Message:    message,
	})
}

// CleanOldLogs deletes log entries older than a specified duration.
func (db *DB) CleanOldLogs(retentionPeriod time.Duration) {
	query := "DELETE FROM audit_log WHERE timestamp < datetime('now', ?)"
	retentionStr := "-%d days" // Convert Go duration to SQL-compatible format

	// Convert duration to days for SQLite compatibility
	days := int(retentionPeriod.Hours() / 24)
	db.QueueWrite("audit_log", query, retentionStr, days)

	log.Printf("Deleted logs older than %d days", days)
}
