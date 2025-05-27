package db

import (
	"sync"
	"time"
)

// WriteQueueType determines how the queue handles operations
type WriteQueueType int

const (
	NodeWriteQueue WriteQueueType = iota // For node tables with path-based batching
	LogWriteQueue                        // For log tables with simple insert operations
)

// WriteQueue manages write operations for a single table
type WriteQueue struct {
	mu           sync.Mutex
	tableName    string
	queueType    WriteQueueType
	queue        map[string][]WriteOp // keyed by path for node tables
	logQueue     []WriteOp            // flat list for log tables
	lastFlushed  time.Time
	batchSize    int
	flushTimer   time.Duration // now just used to store the interval
	readyToWrite bool          // indicates if queue is ready to be flushed
	isWriting    bool          // prevents concurrent flushes
}

// NewWriteQueue creates a new write queue for a specific table
func NewWriteQueue(tableName string, queueType WriteQueueType, batchSize int, flushTimer time.Duration) *WriteQueue {
	return &WriteQueue{
		tableName:   tableName,
		queueType:   queueType,
		queue:       make(map[string][]WriteOp),
		lastFlushed: time.Now(),
		batchSize:   batchSize,
		flushTimer:  flushTimer,
	}
}

// Add queues a new operation
func (wq *WriteQueue) Add(path string, op WriteOp) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.queueType == LogWriteQueue {
		wq.logQueue = append(wq.logQueue, op)
		if len(wq.logQueue) >= wq.batchSize {
			wq.readyToWrite = true
		}
	} else {
		wq.queue[path] = append(wq.queue[path], op)
		if len(wq.queue) >= wq.batchSize {
			wq.readyToWrite = true
		}
	}
}

// IsReadyToWrite returns whether the queue is ready to be flushed
func (wq *WriteQueue) IsReadyToWrite() bool {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.readyToWrite
}

// GetFlushInterval returns the current flush interval
func (wq *WriteQueue) GetFlushInterval() time.Duration {
	return wq.flushTimer
}

// SetFlushInterval allows changing the flush interval
func (wq *WriteQueue) SetFlushInterval(interval time.Duration) {
	wq.mu.Lock()
	wq.flushTimer = interval
	wq.mu.Unlock()
}

// Flush processes all queued operations and returns the batches
func (wq *WriteQueue) Flush(force ...bool) []Batch {
	wq.mu.Lock()
	// If we're already writing, don't flush
	if wq.isWriting {
		wq.mu.Unlock()
		return nil
	}
	// If we're not forcing a flush and there are not enough operations to write, don't flush
	shouldForce := len(force) > 0 && force[0]
	if !shouldForce && !wq.readyToWrite {
		wq.mu.Unlock()
		return nil
	}
	wq.isWriting = true
	wq.readyToWrite = false // Reset ready state after flush
	wq.mu.Unlock()

	if wq.queueType == LogWriteQueue {
		return wq.flushLogQueue()
	}
	return wq.flushNodeQueue()
}

func (wq *WriteQueue) flushLogQueue() []Batch {
	wq.mu.Lock()
	if len(wq.logQueue) == 0 {
		wq.isWriting = false
		wq.mu.Unlock()
		return nil
	}
	queue := wq.logQueue
	wq.logQueue = nil
	wq.mu.Unlock()

	// Create a single batch for all operations
	batch := Batch{
		Table:  wq.tableName,
		OpType: "insert",
		Ops:    make([]WriteOp, len(queue)),
	}
	copy(batch.Ops, queue)

	wq.mu.Lock()
	wq.lastFlushed = time.Now()
	wq.isWriting = false
	wq.mu.Unlock()

	batches := []Batch{batch}
	return batches
}

func (wq *WriteQueue) flushNodeQueue() []Batch {
	if len(wq.queue) == 0 {
		wq.mu.Lock()
		wq.isWriting = false
		wq.mu.Unlock()
		return nil
	}

	// snapshot the keys
	keys := make([]string, 0, len(wq.queue))
	for p := range wq.queue {
		keys = append(keys, p)
	}

	var all []Batch
	// drain round-by-round
	for {
		round := wq.drainRound(keys)
		if len(round) == 0 {
			break
		}
		all = append(all, round...)
	}

	wq.mu.Lock()
	wq.isWriting = false
	wq.mu.Unlock()

	return all
}

// drainRound processes one operation per path, grouped by operation type
func (wq *WriteQueue) drainRound(keys []string) []Batch {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	byType := make(map[string][]WriteOp)
	for _, p := range keys {
		ops, ok := wq.queue[p]
		if !ok || len(ops) == 0 {
			continue
		}
		first := ops[0]
		byType[first.OpType] = append(byType[first.OpType], first)
		wq.queue[p] = ops[1:]
		if len(wq.queue[p]) == 0 {
			delete(wq.queue, p)
		}
	}

	wq.lastFlushed = time.Now()

	// build Batch slices
	out := make([]Batch, 0, len(byType))
	for typ, ops := range byType {
		out = append(out, Batch{
			Table:  wq.tableName,
			OpType: typ,
			Ops:    ops,
		})
	}
	return out
}
