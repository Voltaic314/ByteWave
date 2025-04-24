package writequeue

import (
	"context"
	"log"
	"sync"
	"time"
)

// writeOp represents a queued SQL operation.
type writeOp struct {
	Path   string
	Query  string
	Params []any
}

type Table interface {
	Add(path string, op writeOp)
	Flush()
	StopTimer()
	Name() string
}

// WriteQueue manages multiple WriteQueueTable instances (one per table)
type WriteQueue struct {
	mu         sync.Mutex
	tables     map[string]Table
	batchSize  int
	flushTimer time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	flushFunc  func(tableQueries map[string][]string, tableParams map[string][][]any) error
}

// NewQueue initializes a WriteQueue with per-table batching logic
func NewQueue(batchSize int, flushTimer time.Duration, flushFunc func(map[string][]string, map[string][][]any) error) *WriteQueue {
	ctx, cancel := context.WithCancel(context.Background())

	return &WriteQueue{
		tables:     make(map[string]Table),
		batchSize:  batchSize,
		flushTimer: flushTimer,
		ctx:        ctx,
		cancel:     cancel,
		flushFunc:  flushFunc,
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

// AddWriteOperation queues a new operation for a table
func (wq *WriteQueue) AddWriteOperation(table string, path, query string, params []any) {
	wq.AddWriteOperationWithPath(table, "", query, params)
}

// AddWriteOperationWithPath lets node-style tables de-dupe by path
func (wq *WriteQueue) AddWriteOperationWithPath(table string, path string, query string, params []any) {
	wq.mu.Lock()
	t, exists := wq.tables[table]
	if !exists {
		var newTable Table
		if table == "audit_log" {
			newTable = NewLogWriteQueueTable(table, wq.batchSize, wq.flushTimer, wq.flushSingleTable)
		} else {
			newTable = NewNodeWriteQueueTable(table, wq.batchSize, wq.flushTimer, wq.flushSingleTable)
		}
		t = newTable
		wq.tables[table] = t
	}
	wq.mu.Unlock()

	op := writeOp{Path: path, Query: query, Params: params}
	t.Add(path, op)
}

// flushSingleTable performs a flush for a single table
func (wq *WriteQueue) flushSingleTable(table string, ops []writeOp) {
	if len(ops) == 0 {
		return
	}

	first := ops[0].Query
	allSame := true
	for _, op := range ops {
		if op.Query != first {
			allSame = false
			break
		}
	}

	tableQueries := make(map[string][]string)
	tableParams := make(map[string][][]any)

	if allSame {
		combined := make([]any, 0, len(ops)*len(ops[0].Params))
		for _, op := range ops {
			combined = append(combined, op.Params...)
		}
		tableQueries[table] = []string{first}
		tableParams[table] = [][]any{combined}
	} else {
		queries := make([]string, len(ops))
		params := make([][]any, len(ops))
		for i, op := range ops {
			queries[i] = op.Query
			params[i] = op.Params
		}
		tableQueries[table] = queries
		tableParams[table] = params
	}

	err := wq.flushFunc(tableQueries, tableParams)
	if err != nil {
		log.Printf("WriteQueue flush failed for table %s: %v", table, err)
	}
}

// FlushTable manually flushes a table's queue
func (wq *WriteQueue) FlushTable(table string) {
	wq.mu.Lock()
	t, exists := wq.tables[table]
	wq.mu.Unlock()

	if exists {
		t.Flush()
	}
}

// FlushTables flushes multiple specific tables
func (wq *WriteQueue) FlushTables(tables []string) {
	for _, t := range tables {
		wq.FlushTable(t)
	}
}

// FlushAll flushes every table
func (wq *WriteQueue) FlushAll() {
	wq.mu.Lock()
	names := make([]string, 0, len(wq.tables))
	for name := range wq.tables {
		names = append(names, name)
	}
	wq.mu.Unlock()

	wq.FlushTables(names)
}
