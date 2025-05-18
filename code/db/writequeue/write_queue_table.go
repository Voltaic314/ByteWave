package writequeue

import (
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/types/db"
)

type WriteQueueTableInterface interface {
	Flush()
	StopTimer()
}

// NodeWriteQueueTable manages deduplicated writes by path
type NodeWriteQueueTable struct {
	db.BaseWriteQueueTable
	queue map[string][]db.WriteOp // keyed by path
}

// LogWriteQueueTable is used for insert-only audit log-style tables
type LogWriteQueueTable struct {
	db.BaseWriteQueueTable
	queue []db.WriteOp
}

// --- Constructors ---

func NewNodeWriteQueueTable(name string, batchSize int, flushTimer time.Duration) *NodeWriteQueueTable {
	t := &NodeWriteQueueTable{
		BaseWriteQueueTable: db.BaseWriteQueueTable{
			Mu:             &sync.Mutex{},
			TableName:      name,
			LastFlushed:    time.Now(),
			BatchSize:      batchSize,
			FlushTimer:     flushTimer,
			ResetTimerChan: make(chan struct{}),
			StopChan:       make(chan struct{}),
		},
		queue: make(map[string][]db.WriteOp),
	}
	go t.startFlushTimer()
	return t
}

func NewLogWriteQueueTable(name string, batchSize int, flushTimer time.Duration) *LogWriteQueueTable {
	t := &LogWriteQueueTable{
		BaseWriteQueueTable: db.BaseWriteQueueTable{
			Mu:             &sync.Mutex{},
			TableName:      name,
			LastFlushed:    time.Now(),
			BatchSize:      batchSize,
			FlushTimer:     flushTimer,
			ResetTimerChan: make(chan struct{}),
			StopChan:       make(chan struct{}),
		},
		queue: []db.WriteOp{},
	}
	go t.startFlushTimer()
	return t
}

// --- NodeWriteQueueTable Methods ---

func (t *NodeWriteQueueTable) Name() string {
	return t.TableName
}

func (t *NodeWriteQueueTable) Add(path string, op db.WriteOp) {
	t.Mu.Lock()
	t.queue[path] = append(t.queue[path], op)

	// Check if we should mark as ready to write
	if len(t.queue) >= t.BatchSize {
		t.ReadyToWrite = true
	}
	t.Mu.Unlock()
}

func (t *NodeWriteQueueTable) Flush() []db.Batch {
	t.Mu.Lock()
	if !t.ReadyToWrite || t.IsWriting {
		t.Mu.Unlock()
		return nil
	}
	t.IsWriting = true
	t.ReadyToWrite = false
	t.Mu.Unlock()

	if len(t.queue) == 0 {
		t.Mu.Lock()
		t.IsWriting = false
		t.Mu.Unlock()
		return nil
	}
	// snapshot the keys
	keys := make([]string, 0, len(t.queue))
	for p := range t.queue {
		keys = append(keys, p)
	}

	var all []db.Batch
	// drain round-by-round
	for {
		round := t.drainRound(keys)
		if len(round) == 0 {
			break
		}
		all = append(all, round...)
	}
	select {
	case t.ResetTimerChan <- struct{}{}:
	case <-t.StopChan:
	default:
	}

	t.Mu.Lock()
	t.IsWriting = false
	t.Mu.Unlock()
	return all
}

// drainRound pops exactly one op per path, groups by opType,
// and returns a slice of Batch in arbitrary opType order.
func (t *NodeWriteQueueTable) drainRound(keys []string) []db.Batch {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	byType := make(map[string][]db.WriteOp)
	for _, p := range keys {
		ops, ok := t.queue[p]
		if !ok || len(ops) == 0 {
			continue
		}
		first := ops[0]
		byType[first.OpType] = append(byType[first.OpType], first)
		t.queue[p] = ops[1:]
		if len(t.queue[p]) == 0 {
			delete(t.queue, p)
		}
	}

	t.LastFlushed = time.Now()

	// build Batch slices
	out := make([]db.Batch, 0, len(byType))
	for typ, ops := range byType {
		out = append(out, db.Batch{
			Table:  t.TableName,
			OpType: typ,
			Ops:    ops,
		})
	}
	return out
}

func (t *NodeWriteQueueTable) startFlushTimer() {
	timer := time.NewTimer(t.FlushTimer)
	for {
		select {
		case <-timer.C:
			t.Mu.Lock()
			t.ReadyToWrite = true
			t.Mu.Unlock()
			timer.Reset(t.FlushTimer)
		case <-t.ResetTimerChan:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(t.FlushTimer)
		case <-t.StopChan:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (t *NodeWriteQueueTable) StopTimer() {
	close(t.StopChan)
}

// --- LogWriteQueueTable Methods ---

func (t *LogWriteQueueTable) Name() string {
	return t.TableName
}

func (t *LogWriteQueueTable) Add(path string, op db.WriteOp) {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	t.queue = append(t.queue, op)
	if len(t.queue) >= t.BatchSize {
		t.ReadyToWrite = true
	}
}

func (t *LogWriteQueueTable) Flush() []db.Batch {
	t.Mu.Lock()
	if !t.ReadyToWrite || t.IsWriting {
		t.Mu.Unlock()
		return nil
	}
	t.IsWriting = true
	t.ReadyToWrite = false
	queue := t.queue
	t.queue = nil
	t.Mu.Unlock()

	if len(queue) == 0 {
		t.Mu.Lock()
		t.IsWriting = false
		t.Mu.Unlock()
		return nil
	}

	// Create a batch for all operations
	batch := db.Batch{
		Table:  t.TableName,
		OpType: "insert",
		Ops:    make([]db.WriteOp, len(queue)),
	}
	copy(batch.Ops, queue)

	t.Mu.Lock()
	t.LastFlushed = time.Now()
	t.IsWriting = false
	t.Mu.Unlock()

	return []db.Batch{batch}
}

func (t *LogWriteQueueTable) startFlushTimer() {
	timer := time.NewTimer(t.FlushTimer)
	for {
		select {
		case <-timer.C:
			t.Mu.Lock()
			t.ReadyToWrite = true
			t.Mu.Unlock()
			timer.Reset(t.FlushTimer)
		case <-t.ResetTimerChan:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(t.FlushTimer)
		case <-t.StopChan:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (t *LogWriteQueueTable) StopTimer() {
	close(t.StopChan)
}
