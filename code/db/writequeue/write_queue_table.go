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
	queue map[string][]writeOp // keyed by path

}

// LogWriteQueueTable is used for insert-only audit log-style tables
type LogWriteQueueTable struct {
	BaseWriteQueueTable
	queue []writeOp
}

// --- Constructors ---

func NewNodeWriteQueueTable(name string, batchSize int, flushTimer time.Duration) *NodeWriteQueueTable {
	t := &NodeWriteQueueTable{
		BaseWriteQueueTable: BaseWriteQueueTable{
			name:           name,
			lastFlushed:    time.Now(),
			batchSize:      batchSize,
			flushTimer:     flushTimer,
			resetTimerChan: make(chan struct{}),
			stopChan:       make(chan struct{}),
		},
		queue: make(map[string][]writeOp),
	}
	go t.startFlushTimer()
	return t
}

func NewLogWriteQueueTable(name string, batchSize int, flushTimer time.Duration) *LogWriteQueueTable {
	t := &LogWriteQueueTable{
		BaseWriteQueueTable: BaseWriteQueueTable{
			name:           name,
			lastFlushed:    time.Now(),
			batchSize:      batchSize,
			flushTimer:     flushTimer,
			resetTimerChan: make(chan struct{}),
			stopChan:       make(chan struct{}),
		},
		queue: []writeOp{},
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
	t.mu.Unlock()

	if len(t.queue) >= t.batchSize {
		t.Flush()
	}
}

func (t *NodeWriteQueueTable) Flush() []Batch {
	t.mu.Lock()
	if len(t.queue) == 0 {
		t.mu.Unlock()
		return nil
	}
	// snapshot the keys
	keys := make([]string, 0, len(t.queue))
	for p := range t.queue {
		keys = append(keys, p)
	}
	t.mu.Unlock()

	var all []Batch
	// drain round-by-round
	for {
		round := t.drainRound(keys)
		if len(round) == 0 {
			break
		}
		all = append(all, round...)
	}
	select {
	case t.resetTimerChan <- struct{}{}:
	case <-t.stopChan:
	default:
	}
	return all
}

// drainRound pops exactly one op per path, groups by opType,
// and returns a slice of Batch in arbitrary opType order.
func (t *NodeWriteQueueTable) drainRound(keys []string) []Batch {
	t.mu.Lock()
	defer t.mu.Unlock()

	byType := make(map[string][]writeOp)
	for _, p := range keys {
		ops, ok := t.queue[p]
		if !ok || len(ops) == 0 {
			continue
		}
		first := ops[0]
		byType[first.opType] = append(byType[first.opType], first)
		t.queue[p] = ops[1:]
		if len(t.queue[p]) == 0 {
			delete(t.queue, p)
		}
	}

	t.lastFlushed = time.Now()

	// build Batch slices
	out := make([]Batch, 0, len(byType))
	for typ, ops := range byType {
		out = append(out, Batch{
			Table:  t.name,
			OpType: typ,
			Ops:    ops,
		})
	}
	return out
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
		t.mu.Unlock()
		t.Flush()
		return
	}
	t.mu.Unlock()
}

func (t *LogWriteQueueTable) Flush() []Batch {
	t.mu.Lock()
	batchOps := t.queue
	t.queue = nil
	t.mu.Unlock()

	if len(batchOps) == 0 {
		return nil
	}
	t.lastFlushed = time.Now()
	select {
	case t.resetTimerChan <- struct{}{}:
	case <-t.stopChan:
	default:
	}
	return []Batch{{
		Table:  t.name,
		OpType: "insert", // logs are always inserts
		Ops:    batchOps,
	}}
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
