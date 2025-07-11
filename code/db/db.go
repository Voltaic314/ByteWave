package db

import (
	"context"
	"database/sql"
	"time"

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
		db.flushWriteQueue(wq, tableName)
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
		db.flushWriteQueue(wq, table)
	}
	return db.conn.QueryContext(db.ctx, query, params...)
}

// Write runs a direct write query (e.g. schema setup).
func (db *DB) Write(query string, params ...any) error {
	_, err := db.conn.ExecContext(db.ctx, query, params...)
	return err
}

func (db *DB) flushWriteQueue(wq *WriteQueue, tableName string) {
	batches := wq.Flush()
	for _, b := range batches {
		qs := make([]string, len(b.Ops))
		ps := make([][]any, len(b.Ops))
		for i, op := range b.Ops {
			qs[i] = op.Query
			ps[i] = op.Params
		}
		if err := batchExecute(db.conn, map[string][]string{tableName: qs}, map[string][][]any{tableName: ps}); err != nil {
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
		db.flushWriteQueue(wq, tableName)
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
		db.flushWriteQueue(wq, tableName)
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

// batchExecute flushes all pending write queries in a single transaction.
func batchExecute(conn *sql.DB, tableQueries map[string][]string, tableParams map[string][][]any) error {
	if len(tableQueries) == 0 {
		return nil
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	for table, queries := range tableQueries {
		params := tableParams[table]
		for i, query := range queries {
			_, err := tx.Exec(query, params[i]...)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
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
		}
	}
}

func (db *DB) startQueueListener(tableName string, queue *WriteQueue) {
	timer := time.NewTimer(queue.GetFlushInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			db.flushWriteQueue(queue, tableName)
			timer.Reset(queue.GetFlushInterval())
		case <-db.ctx.Done():
			return
		}
	}
}
