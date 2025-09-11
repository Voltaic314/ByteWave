package db

import (
	"sync"
	"time"

	typesdb "github.com/Voltaic314/ByteWave/tests/types/db"
)

// WriteQueue manages write operations for a single table
type WriteQueue struct {
	mu           sync.Mutex
	tableName    string
	queueType    typesdb.WriteQueueType
	queue        map[string][]typesdb.WriteOp // keyed by path for node tables
	logQueue     []typesdb.WriteOp            // flat list for log tables
	lastFlushed  time.Time
	batchSize    int
	flushTimer   time.Duration // now just used to store the interval
	readyToWrite bool          // indicates if queue is ready to be flushed
	isWriting    bool          // prevents concurrent flushes
}

// NewWriteQueue creates a new write queue for a specific table
func NewWriteQueue(tableName string, queueType typesdb.WriteQueueType, batchSize int, flushTimer time.Duration) *WriteQueue {
	return &WriteQueue{
		tableName:   tableName,
		queueType:   queueType,
		queue:       make(map[string][]typesdb.WriteOp),
		lastFlushed: time.Now(),
		batchSize:   batchSize,
		flushTimer:  flushTimer,
		isWriting:   false,
	}
}

// Add queues a new operation
func (wq *WriteQueue) Add(path string, op typesdb.WriteOp) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.queueType == typesdb.LogWriteQueue {
		wq.logQueue = append(wq.logQueue, op)
		if len(wq.logQueue) >= wq.batchSize {
			wq.readyToWrite = true
		}
	} else {
		wq.queue[path] = append(wq.queue[path], op)
		// Count total operations across all paths, not just unique paths
		totalOps := 0
		for _, ops := range wq.queue {
			totalOps += len(ops)
		}
		if totalOps >= wq.batchSize {
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
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.flushTimer
}

// SetFlushInterval allows changing the flush interval
func (wq *WriteQueue) SetFlushInterval(interval time.Duration) {
	wq.mu.Lock()
	wq.flushTimer = interval
	wq.mu.Unlock()
}

// Flush processes all queued operations and returns the batches
func (wq *WriteQueue) Flush(force ...bool) []typesdb.Batch {
	// 1. Check if we should flush (with proper locking)
	// CAREFUL. This function LOCKS the mutex.
	shouldFlush := wq.ShouldFlush(force...)
	if !shouldFlush {
		return nil
	}

	// 2. Execute the flush (each method handles its own locking)
	wq.mu.Lock()
	if wq.queueType == typesdb.LogWriteQueue {
		wq.mu.Unlock()

		// CAREFUL. This function LOCKS the mutex.
		batches := wq.flushLogQueue()

		// Reset isWriting flag now that flush is complete
		wq.mu.Lock()
		wq.isWriting = false
		wq.mu.Unlock()

		return batches
	}
	wq.mu.Unlock()

	// CAREFUL. This function LOCKS the mutex.
	batches := wq.flushNodeQueue()

	// Reset isWriting flag now that flush is complete
	wq.mu.Lock()
	wq.isWriting = false
	wq.mu.Unlock()

	return batches
}

// ShouldFlush determines if a flush should occur and sets up the writing state
// Returns (ShouldFlush bool)
func (wq *WriteQueue) ShouldFlush(force ...bool) bool {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	ShouldForce := len(force) > 0 && force[0]

	// If we're already writing, don't flush
	if wq.isWriting {
		return false
	}

	// Check timing and operations
	timeSinceLastFlush := time.Since(wq.lastFlushed)
	timeBasedFlush := timeSinceLastFlush >= wq.flushTimer

	hasOperations := false
	if wq.queueType == typesdb.LogWriteQueue {
		hasOperations = len(wq.logQueue) > 0
	} else {
		hasOperations = len(wq.queue) > 0
	}

	// Flush if: forced, batch size reached, OR time interval passed (and we have operations)
	ShouldFlush := ShouldForce || wq.readyToWrite || (timeBasedFlush && hasOperations)

	if !ShouldFlush {
		return false
	}

	// Set up for writing (only if we're going to flush)
	wq.isWriting = true
	wq.readyToWrite = false

	return true
}

func (wq *WriteQueue) flushLogQueue() []typesdb.Batch {
	// 1. Snapshot and clear the queue
	wq.mu.Lock()
	if len(wq.logQueue) == 0 {
		wq.mu.Unlock()
		return nil
	}

	// Take snapshot of operations and clear the queue
	operations := make([]typesdb.WriteOp, len(wq.logQueue))
	copy(operations, wq.logQueue)
	wq.logQueue = nil
	wq.lastFlushed = time.Now()
	wq.mu.Unlock()

	// 2. Create batch outside of lock
	batch := typesdb.Batch{
		Table:  wq.tableName,
		OpType: "insert",
		Ops:    operations,
	}

	return []typesdb.Batch{batch}
}

func (wq *WriteQueue) flushNodeQueue() []typesdb.Batch {
	wq.mu.Lock()
	if len(wq.queue) == 0 {
		wq.mu.Unlock()
		return nil
	}

	// Collect all operations grouped by type
	byType := make(map[string][]typesdb.WriteOp)
	for _, ops := range wq.queue {
		for _, op := range ops {
			byType[op.OpType] = append(byType[op.OpType], op)
		}
	}

	// Clear the entire queue
	wq.queue = make(map[string][]typesdb.WriteOp)
	wq.lastFlushed = time.Now()
	wq.mu.Unlock()

	// Create batches directly from all operations
	batches := make([]typesdb.Batch, 0, len(byType))
	for opType, ops := range byType {
		batches = append(batches, typesdb.Batch{
			Table:  wq.tableName,
			OpType: opType,
			Ops:    ops,
		})
	}

	return batches
}
