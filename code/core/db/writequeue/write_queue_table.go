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
	flushTimer  time.Duration
	resetTimerChan chan struct{}
	stopChan    chan struct{}
}

// NewWriteQueueTable creates a new write queue for a single table.
func NewWriteQueueTable(name string, batchSize int, flushFunc func(string, []writeOp)) *WriteQueueTable {
	t := &WriteQueueTable{
		name:        name,
		queue:       make([]writeOp, 0),
		lastFlushed: time.Now(),
		batchSize:   batchSize,
		flushFunc:   flushFunc,
		flushTimer:  5 * time.Second, // default, can override later
		resetTimerChan: make(chan struct{}),
		stopChan:    make(chan struct{}),
	}

	go t.startFlushTimer()
	return t
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

	// signal the timer loop to reset
	select {
	case t.resetTimerChan <- struct{}{}:
	case <-t.stopChan: // Prevent sending if shutting down
	default:
	}

}

// startFlushTimer runs in the background and flushes on interval if needed.
func (t *WriteQueueTable) startFlushTimer() {
	var timer <-chan time.Time
	timer = time.After(t.flushTimer)

	for {
		select {
		case <-timer:
			t.mu.Lock()
			if len(t.queue) > 0 {
				queueCopy := t.queue
				t.queue = nil
				t.mu.Unlock()
				t.flushFunc(t.name, queueCopy)
			} else {
				t.mu.Unlock()
			}
			// reset the timer
			timer = time.After(t.flushTimer)

		case <-t.resetTimerChan:
			// flush happened manually, reset the timer
			timer = time.After(t.flushTimer)

		case <-t.stopChan:
			return
		}
	}
}


// StopTimer gracefully shuts down the table's flush timer loop.
func (t *WriteQueueTable) StopTimer() {
	close(t.stopChan)
}
