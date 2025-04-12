package writequeue

import (
	"sync"
	"time"
)

// writeOp represents a queued SQL operation.
type writeOp struct {
	Query  string
	Params []any
}

// WriteQueueTable manages queued writes for a single database table.
type WriteQueueTable struct {
	mu          sync.Mutex
	name        string
	queue       []writeOp
	lastFlushed time.Time
	batchSize   int
	flushFunc   func(string, []writeOp) // Takes table name and the batch of ops
}

// NewWriteQueueTable creates a new write queue for a single table.
func NewWriteQueueTable(name string, batchSize int, flushFunc func(string, []writeOp)) *WriteQueueTable {
	return &WriteQueueTable{
		name:        name,
		queue:       make([]writeOp, 0),
		lastFlushed: time.Now(),
		batchSize:   batchSize,
		flushFunc:   flushFunc,
	}
}

// Add queues a new writeOp and triggers a flush if the batch threshold is reached.
func (t *WriteQueueTable) Add(op writeOp) {
	t.mu.Lock()
	t.queue = append(t.queue, op)

	if len(t.queue) >= t.batchSize {
		queueCopy := t.queue
		t.queue = nil
		t.lastFlushed = time.Now()
		t.mu.Unlock()
		t.flushFunc(t.name, queueCopy)
		return
	}

	t.mu.Unlock()
}

// Flush forces all pending operations to be flushed immediately.
func (t *WriteQueueTable) Flush() {
	t.mu.Lock()
	queueCopy := t.queue
	t.queue = nil
	t.lastFlushed = time.Now()
	t.mu.Unlock()

	if len(queueCopy) > 0 {
		t.flushFunc(t.name, queueCopy)
	}
}
