package db

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// WriteOp represents a queued SQL operation
type WriteOp struct {
	Path   string
	Query  string
	Params []any
	OpType string // "insert", "update", "delete"
}

// Batch represents a group of write operations
type Batch struct {
	Table  string
	OpType string
	Ops    []WriteOp
}

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
func (db *DB) InitWriteQueue(table string, queueType WriteQueueType, batchSize int, flushInterval time.Duration) {
	wq := NewWriteQueue(table, queueType, batchSize, flushInterval)
	db.wqMap[table] = wq
	// Start a listener for this new queue
	go db.startQueueListener(table, wq)
}

// Close shuts down all write queues and DB connection.
func (db *DB) Close() {
	for tableName, wq := range db.wqMap {
		// 1. Get all pending batches for this table
		batches := wq.Flush(true)
		// 2. Execute each batch
		for _, b := range batches {
			// build queries + params
			queries := make([]string, len(b.Ops))
			params := make([][]any, len(b.Ops))
			for i, op := range b.Ops {
				queries[i] = op.Query
				params[i] = op.Params
			}
			// wrap into the maps batchExecute expects
			tableQueries := map[string][]string{tableName: queries}
			tableParams := map[string][][]any{tableName: params}
			if err := batchExecute(db.conn, tableQueries, tableParams); err != nil {
				log.Printf("Error flushing %s on Close(): %v", tableName, err)
			}
		}
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
		batches := wq.Flush(true)
		for _, b := range batches {
			// same execution logic as in Close()
			qs := make([]string, len(b.Ops))
			ps := make([][]any, len(b.Ops))
			for i, op := range b.Ops {
				qs[i] = op.Query
				ps[i] = op.Params
			}
			if err := batchExecute(db.conn, map[string][]string{table: qs}, map[string][][]any{table: ps}); err != nil {
				log.Printf("Error flushing %s on Query(): %v", table, err)
			}
		}
	}
	return db.conn.QueryContext(db.ctx, query, params...)
}

// Write runs a direct write query (e.g. schema setup).
func (db *DB) Write(query string, params ...any) error {
	_, err := db.conn.ExecContext(db.ctx, query, params...)
	return err
}

// QueueWrite always treats ops here as inserts
func (db *DB) QueueWrite(tableName, query string, params ...any) {
	if wq, ok := db.wqMap[tableName]; ok {
		wq.Add("", WriteOp{
			Path:   "",
			Query:  query,
			Params: params,
			OpType: "insert",
		})
		batches := wq.Flush()
		for _, b := range batches {
			qs := make([]string, len(b.Ops))
			ps := make([][]any, len(b.Ops))
			for i, op := range b.Ops {
				qs[i] = op.Query
				ps[i] = op.Params
			}
			if err := batchExecute(db.conn, map[string][]string{tableName: qs}, map[string][][]any{tableName: ps}); err != nil {
				log.Printf("Error flushing %s on QueueWrite(): %v", tableName, err)
			}
		}
	}
}

// QueueWriteWithPath is for update‐style ops
func (db *DB) QueueWriteWithPath(tableName, path, query string, params ...any) {
	if wq, ok := db.wqMap[tableName]; ok {
		wq.Add(path, WriteOp{
			Path:   path,
			Query:  query,
			Params: params,
			OpType: "update",
		})
		batches := wq.Flush()
		for _, b := range batches {
			qs := make([]string, len(b.Ops))
			ps := make([][]any, len(b.Ops))
			for i, op := range b.Ops {
				qs[i] = op.Query
				ps[i] = op.Params
			}
			if err := batchExecute(db.conn, map[string][]string{tableName: qs}, map[string][][]any{tableName: ps}); err != nil {
				log.Printf("Error flushing %s on QueueWriteWithPath(): %v", tableName, err)
			}
		}
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
func (db *DB) GetWriteQueue(table string) *WriteQueue {
	if wq, ok := db.wqMap[table]; ok {
		return wq
	}
	return nil
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

	var failedQueries []string

	for table, queries := range tableQueries {
		params := tableParams[table]
		for i, query := range queries {
			_, err := tx.Exec(query, params[i]...)
			if err != nil {
				log.Printf("Query failed in table %s: %s | Error: %v", table, query, err)
				failedQueries = append(failedQueries, query)
			}
		}
	}

	if len(failedQueries) > 0 {
		log.Printf("%d queries failed, but committing successful ones.", len(failedQueries))
	}

	return tx.Commit()
}

func (db *DB) ExecuteBatchCommands(batches []Batch) {
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
			log.Printf("Error executing batch for table %s: %v", b.Table, err)
		}
	}
}

func (db *DB) startQueueListener(tableName string, queue *WriteQueue) {
	timer := time.NewTimer(queue.GetFlushInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			db.processQueueBatch(tableName, queue)
			timer.Reset(queue.GetFlushInterval())
		case <-db.ctx.Done():
			return
		}
	}
}

func (db *DB) processQueueBatch(tableName string, queue *WriteQueue) {
	if !queue.IsReadyToWrite() {
		return
	}

	batches := queue.Flush()
	if len(batches) == 0 {
		return
	}

	for _, b := range batches {
		qs := make([]string, len(b.Ops))
		ps := make([][]any, len(b.Ops))
		for i, op := range b.Ops {
			qs[i] = op.Query
			ps[i] = op.Params
		}

		if err := batchExecute(db.conn, map[string][]string{tableName: qs}, map[string][][]any{tableName: ps}); err != nil {
			log.Printf("Error executing batch for table %s: %v", tableName, err)
		}
	}
}
