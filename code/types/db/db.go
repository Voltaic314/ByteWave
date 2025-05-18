// Package db provides database-related type definitions and interfaces.
// This package contains types used for database operations and data structures.
package db

import (
	"sync"
	"time"
)

// DB represents the main database connection and configuration
type DB struct {
	Conn   interface{}            // *sql.DB
	Ctx    interface{}            // context.Context
	Cancel interface{}            // context.CancelFunc
	WQMap  map[string]interface{} // map[string]*writequeue.WriteQueue
}

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

// Table interface defines methods for table operations
type Table interface {
	Add(path string, op WriteOp)
	Flush() []Batch
	StopTimer()
	Name() string
}

// BaseWriteQueueTable provides shared fields for write queue tables
type BaseWriteQueueTable struct {
	Mu             *sync.Mutex
	TableName      string
	LastFlushed    time.Time
	BatchSize      int
	FlushTimer     time.Duration
	ResetTimerChan chan struct{}
	StopChan       chan struct{}
	ReadyToWrite   bool // indicates if queue is ready to be flushed
	IsWriting      bool // prevents concurrent flushes
}

// WriteQueueTableInterface defines methods for write queue tables
type WriteQueueTableInterface interface {
	Flush()
	StopTimer()
}
