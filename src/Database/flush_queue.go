package database

import (
	"sync"
	"time"
)

type WriteQueue struct {
	mu         sync.Mutex
	queue      []struct {
		Query  string
		Params []interface{}
	} // Directly store queries and params
	batchSize  int
	flushTimer time.Duration
	flushFunc  func([]string, [][]interface{}) error // Adjusted flush function signature
}

// NewQueue initializes the write queue.
func NewQueue(batchSize int, flushTimer time.Duration, flushFunc func([]string, [][]interface{}) error) *WriteQueue {
	wq := &WriteQueue{
		queue:      make([]struct{ Query string; Params []interface{} }, 0),
		batchSize:  batchSize,
		flushTimer: flushTimer,
		flushFunc:  flushFunc,
	}
	go wq.startFlushTimer()
	return wq
}

// AddWriteOperation adds a query to the queue.
func (wq *WriteQueue) AddWriteOperation(query string, params []interface{}) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	wq.queue = append(wq.queue, struct{ Query string; Params []interface{} }{Query: query, Params: params})

	// Trigger a flush if batch size is reached
	if len(wq.queue) >= wq.batchSize {
		go wq.Flush()
	}
}

// Flush writes all pending queries to the DB and clears the queue.
func (wq *WriteQueue) Flush() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if len(wq.queue) == 0 {
		return // Nothing to flush
	}

	// Extract queries and params
	queries := make([]string, len(wq.queue))
	params := make([][]interface{}, len(wq.queue))
	for i, q := range wq.queue {
		queries[i] = q.Query
		params[i] = q.Params
	}

	err := wq.flushFunc(queries, params) // Execute batch write
	if err != nil {
		// Handle error logging or retry logic
		return
	}

	wq.queue = []struct{ Query string; Params []interface{} }{} // Clear the queue after flush
}

// startFlushTimer ensures queue flushes even if batch size isn’t reached.
func (wq *WriteQueue) startFlushTimer() {
	ticker := time.NewTicker(wq.flushTimer)
	defer ticker.Stop()

	for range ticker.C {
		wq.Flush()
	}
}
