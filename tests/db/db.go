package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	typesdb "github.com/Voltaic314/ByteWave/tests/types/db"
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
			result, err := tx.Exec(query, params[i]...)
			if err != nil {
				return fmt.Errorf("failed to execute query for table %s: %w", table, err)
			}

			// Check if UPDATE/DELETE affected any rows
			trimmedQuery := strings.TrimSpace(strings.ToUpper(query))
			if strings.HasPrefix(trimmedQuery, "UPDATE") || strings.HasPrefix(trimmedQuery, "DELETE") {
				rowsAffected, err := result.RowsAffected()
				if err != nil {
					fmt.Printf("⚠️  Could not get rows affected for table %s: %v\n", table, err)
				} else if rowsAffected == 0 {
					fmt.Printf("⚠️  UPDATE/DELETE affected 0 rows for table %s\n", table)
					fmt.Printf("   Query: %s\n", query)
					fmt.Printf("   Params: %v\n", params[i])
				}
			}
		}
	}

	err = tx.Commit()
	return err
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
