package db

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/Voltaic314/ByteWave/code/db/writequeue"
)

type DB struct {
	conn   *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
	wqMap  map[string]*writequeue.WriteQueue
}

// NewDB initializes the DuckDB connection without any write queues.
func NewDB(dbPath string) (*DB, error) {
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &DB{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		wqMap:  make(map[string]*writequeue.WriteQueue),
	}, nil
}

// InitWriteQueueTable initializes a write queue for a specific table.
func (db *DB) InitWriteQueueTable(table string, batchSize int, flushInterval time.Duration) {
	wq := writequeue.NewQueue(batchSize, flushInterval, func(tableQueries map[string][]string, tableParams map[string][][]any) error {
		return batchExecute(db.conn, tableQueries, tableParams)
	})
	db.wqMap[table] = wq
}

// Close shuts down all write queues and DB connection.
func (db *DB) Close() {
	for _, wq := range db.wqMap {
		wq.Stop()
	}
	db.cancel()
	if db.conn != nil {
		db.conn.Close()
	}
}

// Query runs a read query after flushing pending writes for the given table.
func (db *DB) Query(table string, query string, params ...any) (*sql.Rows, error) {
	if wq, ok := db.wqMap[table]; ok {
		wq.FlushTable(table)
	}
	return db.conn.QueryContext(db.ctx, query, params...)
}

// Write runs a direct write query (e.g. schema setup).
func (db *DB) Write(query string, params ...any) error {
	_, err := db.conn.ExecContext(db.ctx, query, params...)
	return err
}

// QueueWrite adds an insert/update to the async write queue.
// If the table is "audit_log", path is empty string.
func (db *DB) QueueWrite(tableName string, query string, params ...any) {
	if wq, ok := db.wqMap[tableName]; ok {
		go wq.AddWriteOperation(tableName, "", query, params)
	}
}

// QueueWriteWithPath adds a write op with path, used for node-style updates.
func (db *DB) QueueWriteWithPath(tableName string, path string, query string, params ...any) {
	if wq, ok := db.wqMap[tableName]; ok {
		go wq.AddWriteOperation(tableName, path, query, params)
	}
}

// CreateTable creates a table if it doesn’t exist.
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
func (db *DB) GetWriteQueue(table string) *writequeue.WriteQueue {
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
