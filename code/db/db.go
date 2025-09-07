package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Voltaic314/ByteWave/code/types/logging"

	typesdb "github.com/Voltaic314/ByteWave/code/types/db"
	_ "github.com/marcboeker/go-duckdb"
)

type DB struct {
	conn   *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
	wqMap  map[string]*WriteQueue
}

// NewDB initializes the DuckDB connection without any write queues.
func NewDB(dbPath string) (*DB, error) {
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	db := &DB{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		wqMap:  make(map[string]*WriteQueue),
	}

	return db, nil
}

// InitWriteQueue initializes a write queue for a specific table.
func (db *DB) InitWriteQueue(table string, queueType typesdb.WriteQueueType, batchSize int, flushInterval time.Duration) {
	wq := NewWriteQueue(table, queueType, batchSize, flushInterval)
	db.wqMap[table] = wq
	// Start a listener for this new queue
	go db.startQueueListener(table, wq)
}

// Close shuts down all write queues and DB connection.
func (db *DB) Close() {
	for tableName, wq := range db.wqMap {
		db.flushWriteQueue(wq, tableName, true)
	}

	db.cancel()
	if db.conn != nil {
		db.conn.Close()
	}
}

// Query runs a read query after flushing pending writes for the given table.
func (db *DB) Query(table string, query string, params ...any) (*sql.Rows, error) {
	if wq, ok := db.wqMap[table]; ok {
		// if we want to read from a table that has pending writes, we need to flush them first to make sure we query all of the data
		db.flushWriteQueue(wq, table, true)
	}
	return db.conn.QueryContext(db.ctx, query, params...)
}

// Write runs a direct write query (e.g. schema setup).
func (db *DB) Write(query string, params ...any) error {
	_, err := db.conn.ExecContext(db.ctx, query, params...)
	return err
}

func (db *DB) flushWriteQueue(wq *WriteQueue, tableName string, force bool) {
	batches := wq.Flush(force)
	for _, b := range batches {
		qs := make([]string, len(b.Ops))
		ps := make([][]any, len(b.Ops))
		for i, op := range b.Ops {
			qs[i] = op.Query
			ps[i] = op.Params
		}
		if err := batchExecute(db.conn, map[string][]string{tableName: qs}, map[string][][]any{tableName: ps}); err != nil {
			// Log the error instead of silently ignoring it
			fmt.Printf("❌ Database batch execution failed for table %s: %v\n", tableName, err)
			sampleCount := len(qs)
			if sampleCount > 3 {
				sampleCount = 3
			}
			fmt.Printf("   Query samples: %v\n", qs[:sampleCount]) // Show first 3 queries for debugging
		}
	}
}

// QueueWrite always treats ops here as inserts
func (db *DB) QueueWrite(tableName, query string, params ...any) {
	if wq, ok := db.wqMap[tableName]; ok {
		wq.Add("", typesdb.WriteOp{
			Path:   "",
			Query:  query,
			Params: params,
			OpType: "insert",
		})
		// if we are now above threshold, flush the queue
		wq.Flush() // Do not pass in force flush True here
		// because we want to flush the queue if it is ready
		// to be flushed, not because we are forcing a flush
	}
}

// QueueWriteWithPath is for update‐style ops
func (db *DB) QueueWriteWithPath(tableName, path, query string, params ...any) {
	if wq, ok := db.wqMap[tableName]; ok {
		wq.Add(path, typesdb.WriteOp{
			Path:   path,
			Query:  query,
			Params: params,
			OpType: "update",
		})
		// ✅ FIX: Add flush call like QueueWrite has
		wq.Flush()
	}
}

// CreateTable creates a table if it doesn't exist.
func (db *DB) CreateTable(tableName string, schema string) error {
	query := "CREATE TABLE IF NOT EXISTS " + tableName + " (" + schema + ")"
	return db.Write(query)
}

// DropTable removes a table if it exists.
func (db *DB) DropTable(tableName string) error {
	query := "DROP TABLE IF EXISTS " + tableName
	return db.Write(query)
}

// WriteBatch exposes batchExecute for use by external modules (e.g., logger).
func (db *DB) WriteBatch(tableQueries map[string][]string, tableParams map[string][][]any) error {
	return batchExecute(db.conn, tableQueries, tableParams)
}

// GetWriteQueue returns the write queue for a given table.
func (db *DB) GetWriteQueue(table string) typesdb.WriteQueueInterface {
	if wq, ok := db.wqMap[table]; ok {
		return wq
	}
	return nil
}

// ForceFlushTable forces a flush of the write queue for a specific table
func (db *DB) ForceFlushTable(tableName string) {
	if wq, ok := db.wqMap[tableName]; ok {
		db.flushWriteQueue(wq, tableName, true)
	}
}

// batchExecute flushes all pending write queries in a single transaction.
func batchExecute(conn *sql.DB, tableQueries map[string][]string, tableParams map[string][][]any) error {
	if len(tableQueries) == 0 {
		return nil
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	// Ensure rollback happens on error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for table, queries := range tableQueries {
		params := tableParams[table]
		for i, query := range queries {
			_, err = tx.Exec(query, params[i]...)
			if err != nil {
				return fmt.Errorf("failed to execute query for table %s: %w", table, err)
			}
		}
	}

	err = tx.Commit()
	return err
}

func (db *DB) ExecuteBatchCommands(batches []typesdb.Batch) {
	for _, b := range batches {
		// Convert Batch to tableQueries and tableParams format
		tableQueries := make(map[string][]string)
		tableParams := make(map[string][][]any)

		// Group operations by table
		for _, op := range b.Ops {
			tableQueries[b.Table] = append(tableQueries[b.Table], op.Query)
			tableParams[b.Table] = append(tableParams[b.Table], op.Params)
		}

		if err := db.WriteBatch(tableQueries, tableParams); err != nil {
			// Log the error instead of silently ignoring it
			fmt.Printf("❌ Database batch write failed for table %s: %v\n", b.Table, err)
		}
	}
}

func (db *DB) startQueueListener(tableName string, queue *WriteQueue) {
	timer := time.NewTimer(queue.GetFlushInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			db.flushWriteQueue(queue, tableName, true)
			timer.Reset(queue.GetFlushInterval())
		case <-db.ctx.Done():
			return
		}
	}
}

// WriteLog inserts a log entry into the audit log table asynchronously.
func (db *DB) WriteLog(entry logging.LogEntry) {
	go func() {
		// Match the canonical audit_log schema from tables/audit_log.go
		query := `
			INSERT INTO audit_log (id, timestamp, level, entity, entity_id, details, message, action, topic, subtopic, queue)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		// Generate UUID for primary key
		id := fmt.Sprintf("log_%d", entry.Timestamp.UnixNano())

		// Format timestamp in ISO 8601 format that DuckDB expects
		timestamp := entry.Timestamp.Format("2006-01-02 15:04:05.000000")

		// Sanitize the details JSON
		detailsJSON, err := sanitizeJSON(entry.Details)
		if err != nil {
			detailsJSON = "null"
		}

		db.QueueWrite("audit_log", query, id, timestamp, entry.Level, entry.Entity, entry.EntityID, detailsJSON, entry.Message, entry.Action, nil, nil, entry.Queue)
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
