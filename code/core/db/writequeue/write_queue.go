package writequeue

import (
	"context"
	"log"
	"sync"
	"time"
)

type WriteQueue struct {
	mu         sync.Mutex
	tables     map[string]*WriteQueueTable
	batchSize  int
	flushTimer time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	flushFunc  func(tableQueries map[string][]string, tableParams map[string][][]any) error
}

// NewQueue initializes a WriteQueue with per-table batching.
func NewQueue(batchSize int, flushTimer time.Duration, flushFunc func(map[string][]string, map[string][][]any) error) *WriteQueue {
	ctx, cancel := context.WithCancel(context.Background())

	return &WriteQueue{
		tables:     make(map[string]*WriteQueueTable),
		batchSize:  batchSize,
		flushTimer: flushTimer,
		ctx:        ctx,
		cancel:     cancel,
		flushFunc:  flushFunc,
	}
}

// Stop gracefully shuts down all table flush loops.
func (wq *WriteQueue) Stop() {
	wq.cancel()

	wq.mu.Lock()
	defer wq.mu.Unlock()
	for _, table := range wq.tables {
		table.StopTimer()
	}
}

// AddWriteOperation queues a new operation for a specific table.
func (wq *WriteQueue) AddWriteOperation(table string, query string, params []any) {
	wq.mu.Lock()
	t, exists := wq.tables[table]
	if !exists {
		t = NewWriteQueueTable(table, wq.batchSize, wq.flushSingleTable)
		wq.tables[table] = t
	}
	wq.mu.Unlock()

	t.Add(writeOp{Query: query, Params: params})
}

// flushSingleTable is used by individual WriteQueueTables to flush themselves.
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

// FlushTable manually flushes a single table's queue.
func (wq *WriteQueue) FlushTable(table string) {
	wq.mu.Lock()
	t, exists := wq.tables[table]
	wq.mu.Unlock()
	if exists {
		t.Flush()
	}
}

// FlushTables flushes multiple named tables.
func (wq *WriteQueue) FlushTables(tables []string) {
	for _, t := range tables {
		wq.FlushTable(t)
	}
}

// FlushAll flushes all registered tables.
func (wq *WriteQueue) FlushAll() {
	wq.mu.Lock()
	names := make([]string, 0, len(wq.tables))
	for name := range wq.tables {
		names = append(names, name)
	}
	wq.mu.Unlock()

	wq.FlushTables(names)
}
