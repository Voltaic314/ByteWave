package writequeue

import (
	"context"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/types/db"
)

// WriteQueue manages multiple WriteQueueTable instances (one per table)
type WriteQueue struct {
	mu         sync.Mutex
	tables     map[string]db.Table
	batchSize  int
	flushTimer time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewQueue initializes a WriteQueue with per-table batching logic
func NewQueue(batchSize int, flushTimer time.Duration) *WriteQueue {
	ctx, cancel := context.WithCancel(context.Background())

	return &WriteQueue{
		tables:     make(map[string]db.Table),
		batchSize:  batchSize,
		flushTimer: flushTimer,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Stop gracefully stops all write queues
func (wq *WriteQueue) Stop() {
	wq.cancel()
	wq.mu.Lock()
	defer wq.mu.Unlock()

	for _, table := range wq.tables {
		table.StopTimer()
	}
}

// AddWriteOperation queues a new operation and returns any ready-to-write batches.
func (wq *WriteQueue) AddWriteOperation(
	tableName, _, query string, params []any, opType string,
) []db.Batch {
	return wq.AddWriteOperationWithPath(tableName, "", query, params, opType)
}

// AddWriteOperationWithPath queues an op (deduped by path) and returns any batches.
func (wq *WriteQueue) AddWriteOperationWithPath(
	tableName, path, query string,
	params []any,
	opType string,
) []db.Batch {
	wq.mu.Lock()
	tbl, exists := wq.tables[tableName]
	if !exists {
		if tableName == "audit_log" {
			tbl = NewLogWriteQueueTable(tableName, wq.batchSize, wq.flushTimer)
		} else {
			tbl = NewNodeWriteQueueTable(tableName, wq.batchSize, wq.flushTimer)
		}
		wq.tables[tableName] = tbl
	}
	wq.mu.Unlock()

	tbl.Add(path, db.WriteOp{Path: path, Query: query, Params: params, OpType: opType})
	return tbl.Flush() // This will now only flush if ready
}

// FlushTable manually flushes a table's queue and returns its batches.
func (wq *WriteQueue) FlushTable(tableName string) []db.Batch {
	wq.mu.Lock()
	tbl, exists := wq.tables[tableName]
	wq.mu.Unlock()
	if !exists {
		return nil
	}
	return tbl.Flush()
}

// FlushTables flushes multiple specific tables
func (wq *WriteQueue) FlushTables(tables []string) {
	for _, t := range tables {
		wq.FlushTable(t)
	}
}

// FlushAll flushes every table and returns a map from table name → its ordered Batches.
func (wq *WriteQueue) FlushAll() map[string][]db.Batch {
	wq.mu.Lock()
	names := make([]string, 0, len(wq.tables))
	for name := range wq.tables {
		names = append(names, name)
	}
	wq.mu.Unlock()

	result := make(map[string][]db.Batch, len(names))
	for _, name := range names {
		batches := wq.FlushTable(name)
		if len(batches) > 0 {
			result[name] = batches
		}
	}
	return result
}
