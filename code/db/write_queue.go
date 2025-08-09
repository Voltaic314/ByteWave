package db

import (
	"sync"
	"time"

	typesdb "github.com/Voltaic314/ByteWave/code/types/db"
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
func (wq *WriteQueue) Flush(force ...bool) []typesdb.Batch {
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

	if wq.queueType == typesdb.LogWriteQueue {
		return wq.flushLogQueue()
	}
	return wq.flushNodeQueue()
}

func (wq *WriteQueue) flushLogQueue() []typesdb.Batch {
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
	batch := typesdb.Batch{
		Table:  wq.tableName,
		OpType: "insert",
		Ops:    make([]typesdb.WriteOp, len(queue)),
	}
	copy(batch.Ops, queue)

	wq.mu.Lock()
	wq.lastFlushed = time.Now()
	wq.isWriting = false
	wq.mu.Unlock()

	batches := []typesdb.Batch{batch}
	return batches
}

func (wq *WriteQueue) flushNodeQueue() []typesdb.Batch {
	wq.mu.Lock()
	if len(wq.queue) == 0 {
		wq.isWriting = false
		wq.mu.Unlock()
		return nil
	}
	// snapshot the keys under lock to avoid concurrent map iteration
	keys := make([]string, 0, len(wq.queue))
	for p := range wq.queue {
		keys = append(keys, p)
	}
	wq.mu.Unlock()

	var all []typesdb.Batch
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

/*
drainRound processes one operation per path, grouped by operation type

NOTE: This complex path-based batching logic was originally designed to prevent
write conflicts when multiple operations could target the same path concurrently.
With the new phase-based traversal design, each path is only written to once
per phase, making this logic technically redundant. However, we keep it as
defensive programming - it provides an extra safety layer and doesn't hurt
performance too much. If you ever need to support concurrent writes to the
same path again, this logic is already in place.
*/
func (wq *WriteQueue) drainRound(keys []string) []typesdb.Batch {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	byType := make(map[string][]typesdb.WriteOp)
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
	out := make([]typesdb.Batch, 0, len(byType))
	for typ, ops := range byType {
		out = append(out, typesdb.Batch{
			Table:  wq.tableName,
			OpType: typ,
			Ops:    ops,
		})
	}
	return out
}
