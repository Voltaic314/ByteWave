// Package db provides database-related type definitions and interfaces for testing.
// This package contains types used for database operations and data structures in the test harness.
package db

import (
	"time"
)

// WriteQueueType determines how the queue handles operations
type WriteQueueType int

const (
	NodeWriteQueue WriteQueueType = iota // For node tables with path-based batching
	LogWriteQueue                        // For log tables with simple insert operations
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

// WriteQueueInterface defines methods for write queue operations
type WriteQueueInterface interface {
	Add(path string, op WriteOp)
	Flush(force ...bool) []Batch
	IsReadyToWrite() bool
	GetFlushInterval() time.Duration
	SetFlushInterval(interval time.Duration)
}
