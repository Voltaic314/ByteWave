package writequeue

import (
	"sync"
	"time"
)

// Shared base struct used by both node/log write queues
type BaseWriteQueueTable struct {
	mu             sync.Mutex
	name           string
	lastFlushed    time.Time
	batchSize      int
	flushTimer     time.Duration
	resetTimerChan chan struct{}
	stopChan       chan struct{}
}

type WriteQueueTableInterface interface {
	Flush()
	StopTimer()
}

// NodeWriteQueueTable manages deduplicated writes by path
type NodeWriteQueueTable struct {
	BaseWriteQueueTable
	queue     map[string][]writeOp // keyed by path
	flushFunc func(table string, ops []writeOp)
}

// LogWriteQueueTable is used for insert-only audit log-style tables
type LogWriteQueueTable struct {
	BaseWriteQueueTable
	queue     []writeOp
	flushFunc func(table string, ops []writeOp)
}

// --- Constructors ---

func NewNodeWriteQueueTable(name string, batchSize int, flushTimer time.Duration, flushFunc func(string, []writeOp)) *NodeWriteQueueTable {
	t := &NodeWriteQueueTable{
		BaseWriteQueueTable: BaseWriteQueueTable{
			name:           name,
			lastFlushed:    time.Now(),
			batchSize:      batchSize,
			flushTimer:     flushTimer,
			resetTimerChan: make(chan struct{}),
			stopChan:       make(chan struct{}),
		},
		queue:     make(map[string][]writeOp),
		flushFunc: flushFunc,
	}
	go t.startFlushTimer()
	return t
}

func NewLogWriteQueueTable(name string, batchSize int, flushTimer time.Duration, flushFunc func(string, []writeOp)) *LogWriteQueueTable {
	t := &LogWriteQueueTable{
		BaseWriteQueueTable: BaseWriteQueueTable{
			name:           name,
			lastFlushed:    time.Now(),
			batchSize:      batchSize,
			flushTimer:     flushTimer,
			resetTimerChan: make(chan struct{}),
			stopChan:       make(chan struct{}),
		},
		queue:     []writeOp{},
		flushFunc: flushFunc,
	}
	go t.startFlushTimer()
	return t
}

// --- NodeWriteQueueTable Methods ---

func (t *NodeWriteQueueTable) Name() string {
	return t.name
}

func (t *NodeWriteQueueTable) Add(path string, op writeOp) {
	t.mu.Lock()
	t.queue[path] = append(t.queue[path], op)

	if len(t.queue) >= t.batchSize {
		snapshot := t.drainBatch()
		t.mu.Unlock()
		t.flushFunc(t.name, snapshot)
		return
	}
	t.mu.Unlock()
}

func (t *NodeWriteQueueTable) Flush() {
	t.mu.Lock()
	snapshot := t.drainBatch()
	t.mu.Unlock()
	if len(snapshot) > 0 {
		t.flushFunc(t.name, snapshot)
	}
	select {
	case t.resetTimerChan <- struct{}{}:
	case <-t.stopChan:
	default:
	}
}

func (t *NodeWriteQueueTable) drainBatch() []writeOp {
	result := make([]writeOp, 0)
	for path, ops := range t.queue {
		if len(ops) > 0 {
			result = append(result, ops[0])
			t.queue[path] = ops[1:]
			if len(t.queue[path]) == 0 {
				delete(t.queue, path)
			}
		}
	}
	t.lastFlushed = time.Now()
	return result
}

func (t *NodeWriteQueueTable) startFlushTimer() {
	timer := time.After(t.flushTimer)
	for {
		select {
		case <-timer:
			t.Flush()
			timer = time.After(t.flushTimer)
		case <-t.resetTimerChan:
			timer = time.After(t.flushTimer)
		case <-t.stopChan:
			return
		}
	}
}

func (t *NodeWriteQueueTable) StopTimer() {
	close(t.stopChan)
}

// --- LogWriteQueueTable Methods ---

func (t *LogWriteQueueTable) Name() string {
	return t.name
}

func (t *LogWriteQueueTable) Add(_ string, op writeOp) {
	t.mu.Lock()
	t.queue = append(t.queue, op)

	if len(t.queue) >= t.batchSize {
		snapshot := t.queue
		t.queue = nil
		t.lastFlushed = time.Now()
		t.mu.Unlock()
		t.flushFunc(t.name, snapshot)
		return
	}
	t.mu.Unlock()
}

func (t *LogWriteQueueTable) Flush() {
	t.mu.Lock()
	snapshot := t.queue
	t.queue = nil
	t.lastFlushed = time.Now()
	t.mu.Unlock()

	if len(snapshot) > 0 {
		t.flushFunc(t.name, snapshot)
	}

	select {
	case t.resetTimerChan <- struct{}{}:
	case <-t.stopChan:
	default:
	}
}

func (t *LogWriteQueueTable) startFlushTimer() {
	timer := time.After(t.flushTimer)
	for {
		select {
		case <-timer:
			t.Flush()
			timer = time.After(t.flushTimer)
		case <-t.resetTimerChan:
			timer = time.After(t.flushTimer)
		case <-t.stopChan:
			return
		}
	}
}

func (t *LogWriteQueueTable) StopTimer() {
	close(t.stopChan)
}
