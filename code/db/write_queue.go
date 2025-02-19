package db

import (
	"sync"
	"time"
)

type WriteQueue struct {
	mu         sync.Mutex
	queue      map[string][]struct {
		Query  string
		Params []interface{}
	} // Grouped by table name
	batchSize  int
	flushTimer time.Duration
	flushFunc  func(map[string][]string, map[string][][]interface{}) error // Adjusted flush function signature
}

// NewQueue initializes the write queue.
func NewQueue(batchSize int, flushTimer time.Duration, flushFunc func(map[string][]string, map[string][][]interface{}) error) *WriteQueue {
	wq := &WriteQueue{
		queue:      make(map[string][]struct{ Query string; Params []interface{} }),
		batchSize:  batchSize,
		flushTimer: flushTimer,
		flushFunc:  flushFunc,
	}
	go wq.startFlushTimer()
	return wq
}

// AddWriteOperation adds a query to the queue under its respective table.
func (wq *WriteQueue) AddWriteOperation(table string, query string, params []interface{}) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	// Append the query under the respective table
	wq.queue[table] = append(wq.queue[table], struct{ Query string; Params []interface{} }{Query: query, Params: params})

	// Trigger a flush if batch size is reached for any table
	for _, queries := range wq.queue {
		if len(queries) >= wq.batchSize {
			go wq.Flush()
			break
		}
	}
}

// Flush writes all pending queries, grouped by table, to the DB and clears the queue.
func (wq *WriteQueue) Flush() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if len(wq.queue) == 0 {
		return // Nothing to flush
	}

	// Separate queries and params by table
	tableQueries := make(map[string][]string)
	tableParams := make(map[string][][]interface{})

	for table, entries := range wq.queue {
		queries := make([]string, len(entries))
		params := make([][]interface{}, len(entries))
		for i, entry := range entries {
			queries[i] = entry.Query
			params[i] = entry.Params
		}
		tableQueries[table] = queries
		tableParams[table] = params
	}

	// Execute batch write per table
	err := wq.flushFunc(tableQueries, tableParams)
	if err != nil {
		// Handle error logging or retry logic
		return
	}

	// Clear queue after flush
	wq.queue = make(map[string][]struct{ Query string; Params []interface{} })
}

// startFlushTimer ensures queue flushes even if batch size isn’t reached.
func (wq *WriteQueue) startFlushTimer() {
	ticker := time.NewTicker(wq.flushTimer)
	defer ticker.Stop()

	for range ticker.C {
		wq.Flush()
	}
}
