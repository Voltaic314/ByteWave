package db

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/Voltaic314/ByteWave/code/core/db/writequeue"
)

type DB struct {
	conn   *sql.DB
	wq     *writequeue.WriteQueue
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDB initializes the DuckDB connection and write queue.
func NewDB(dbPath string, batchSize int, flushTimer time.Duration) (*DB, error) {
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize write queue
	wq := writequeue.NewQueue(batchSize, flushTimer, func(tableQueries map[string][]string, tableParams map[string][][]any) error {
		return batchExecute(ctx, conn, tableQueries, tableParams)
	})

	return &DB{
		conn:   conn,
		wq:     wq,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Close shuts down the write queue and DB connection.
func (db *DB) Close() {
	db.wq.Stop()
	db.cancel()
	if db.conn != nil {
		db.conn.Close()
	}
}

// Query runs a read query after flushing pending writes for the given table.
func (db *DB) Query(table string, query string, params ...any) (*sql.Rows, error) {
	db.wq.FlushTable(table)
	return db.conn.QueryContext(db.ctx, query, params...)
}

// Write runs a direct write query (e.g. schema setup).
func (db *DB) Write(query string, params ...any) error {
	_, err := db.conn.ExecContext(db.ctx, query, params...)
	return err
}

// QueueWrite adds an insert/update to the async write queue.
func (db *DB) QueueWrite(tableName string, query string, params ...any) {
	go db.wq.AddWriteOperation(tableName, query, params)
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
	return batchExecute(db.ctx, db.conn, tableQueries, tableParams)
}

// batchExecute flushes all pending write queries in a single transaction.
func batchExecute(ctx context.Context, conn *sql.DB, tableQueries map[string][]string, tableParams map[string][][]any) error {
	if len(tableQueries) == 0 {
		return nil
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	var failedQueries []string

	for table, queries := range tableQueries {
		params := tableParams[table]
		for i, query := range queries {
			_, err := tx.ExecContext(ctx, query, params[i]...)
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
