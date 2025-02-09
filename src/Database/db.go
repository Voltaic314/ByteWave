package database

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn       *sql.DB
	writeQueue *WriteQueue
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

	// Initialize write queue
	writeQueue := NewQueue(batchSize, flushTimer, func(queries []string, params [][]interface{}) error {
		return batchExecute(conn, queries, params)
	})

	return &DB{conn: conn, writeQueue: writeQueue}, nil
}

// Close closes the database connection.
func (db *DB) Close() {
	db.conn.Close()
}

// Write executes an immediate query (for table creation, schema updates, etc.).
func (db *DB) Write(query string, params ...interface{}) error {
	_, err := db.conn.Exec(query, params...)
	return err
}

// QueueWrite adds a query to the flush queue for batch processing.
func (db *DB) QueueWrite(query string, params ...interface{}) {
	db.writeQueue.AddWriteOperation(query, params)
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

func batchExecute(conn *sql.DB, queries []string, params [][]interface{}) error {
	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	failedQueries := []string{}

	for i, query := range queries {
		_, err := tx.Exec(query, params[i]...)
		if err != nil {
			// Log the failed query instead of rolling back everything
			log.Printf("Query failed: %s | Error: %v", query, err)
			failedQueries = append(failedQueries, query)
		}
	}

	if len(failedQueries) > 0 {
		// Optional: Retry failed queries separately
		log.Printf("%d queries failed, but committing successful ones.", len(failedQueries))
	}

	return tx.Commit() // Commit whatever succeeded
}