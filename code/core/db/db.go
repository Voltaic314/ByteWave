package db

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn *sql.DB
	wq   *WriteQueue
	ctx  context.Context
	cancel context.CancelFunc
}

// NewDB initializes the database connection and ensures the DB file exists.
func NewDB(dbPath string, batchSize int, flushTimer time.Duration) (*DB, error) {
	// Ensure the database file exists before opening the connection
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		file, err := os.Create(dbPath) // Create the file if it doesn't exist
		if err != nil {
			return nil, err
		}
		file.Close() // Close the file since we just needed to create it
	}

	conn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize write queue
	wq := NewQueue(batchSize, flushTimer, func(tableQueries map[string][]string, tableParams map[string][][]interface{}) error {
		return batchExecute(ctx, conn, tableQueries, tableParams)
	})

	return &DB{conn: conn, wq: wq, ctx: ctx, cancel: cancel}, nil
}

// Close closes the database connection.
func (db *DB) Close() {
	db.cancel()
	if db.conn != nil {
		db.conn.Close()
	}
}

// Write executes an immediate query (for table creation, schema updates, etc.).
func (db *DB) Write(query string, params ...interface{}) error {
	_, err := db.conn.ExecContext(db.ctx, query, params...)
	return err
}

// QueueWrite adds a query to the flush queue for batch processing asynchronously.
func (db *DB) QueueWrite(tableName string, query string, params ...interface{}) {
	go db.wq.AddWriteOperation(tableName, query, params)
}

// CreateTable creates a table if it doesn’t exist.
func (db *DB) CreateTable(tableName string, schema string) error {
	query := "CREATE TABLE IF NOT EXISTS " + tableName + " (" + schema + ")"
	return db.Write(query)
}

// DropTable drops a table.
func (db *DB) DropTable(tableName string) error {
	query := "DROP TABLE IF EXISTS " + tableName
	return db.Write(query)
}

// batchExecute processes batch write operations grouped by table name asynchronously.
func batchExecute(ctx context.Context, conn *sql.DB, tableQueries map[string][]string, tableParams map[string][][]interface{}) error {
	if len(tableQueries) == 0 { // No queries to execute
		return nil
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	failedQueries := []string{}

	// Execute queries per table
	for table, queries := range tableQueries {
		params := tableParams[table]
		for i, query := range queries {
			_, err := tx.ExecContext(ctx, query, params[i]...)
			if err != nil {
				// Log the failed query instead of rolling back everything
				log.Printf("Query failed in table %s: %s | Error: %v", table, query, err)
				failedQueries = append(failedQueries, query)
			}
		}
	}

	if len(failedQueries) > 0 {
		log.Printf("%d queries failed, but committing successful ones.", len(failedQueries))
	}

	return tx.Commit() // Commit whatever succeeded
}
