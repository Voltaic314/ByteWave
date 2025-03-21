package processing

import (
	"fmt"
	"sync"
)

// QueueType defines whether a queue is for traversal or uploading.
type QueueType string

const (
	TraversalQueueType QueueType = "traversal"
	UploadQueueType    QueueType = "upload"
)

// QueueState defines the possible states of a queue.
type QueueState string

const (
	QueueRunning QueueState = "running"
	QueuePaused  QueueState = "paused"
)

// WorkerState defines the possible states of a worker.
type WorkerState string

const (
	WorkerIdle    WorkerState = "idle"
	WorkerActive  WorkerState = "active"
)

// Worker represents an instance of a worker assigned to this queue.
type Worker struct {
	ID    string
	State WorkerState
}

// TaskQueue manages tasks, workers, and execution settings.
type TaskQueue struct {
	QueueID             string         // Unique identifier for the queue
	Type                QueueType      // Type of queue (traversal or upload)
	Phase               int            // Current phase
	SrcOrDst            string         // "src" or "dst"
	tasks               []*Task        // List of tasks
	workers             []*Worker      // Managed by Conductor
	mu                  sync.Mutex     // Mutex for concurrent access
	PaginationSize      int            // Pagination width for folder content listing
	PaginationChan      chan int       // Channel for live updates
	GlobalSettings      map[string]any // Stores global settings
	State               QueueState     // Tracks if the queue is running or paused
	cond                *sync.Cond     // Queue-specific condition variable
	RunningLowChan      chan int       // Unique channel per queue for RunningLow signals
	RunningLowTriggered bool           // ✅ Prevents duplicate RunningLow signals
}

// NewTaskQueue initializes a new queue for traversal or upload.
func NewTaskQueue(queueType QueueType, phase int, srcOrDst string, paginationSize int, runningLowChan chan int) *TaskQueue {
	queueID := fmt.Sprintf("%s-%s-phase-%d", queueType, srcOrDst, phase)
	q := &TaskQueue{
		QueueID:        queueID,
		Type:           queueType,
		Phase:          phase,
		SrcOrDst:       srcOrDst,
		tasks:          []*Task{},
		workers:        []*Worker{}, // ✅ Workers are assigned externally by Conductor
		PaginationSize: paginationSize,
		PaginationChan: make(chan int, 1),
		GlobalSettings: make(map[string]any),
		State:          QueueRunning,  // ✅ Start in Running state
		RunningLowChan: runningLowChan, // ✅ Receive from Conductor instead of creating it
	}
	q.cond = sync.NewCond(&q.mu) // Initialize per-queue sync.Cond
	return q
}

// CheckAndTriggerQP checks the queue size and signals QP if tasks are low.
func (q *TaskQueue) CheckAndTriggerQP() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// If queue is running and task count is low, signal QP
	if q.State == QueueRunning && len(q.tasks) < q.PaginationSize {
		select {
		case q.RunningLowChan <- q.PaginationSize:
			// ✅ Sent signal to QP to start publishing
		default:
			// Prevent duplicate signals if already sent
		}
	}
}

// Pause the queue (workers will stop picking up tasks)
func (q *TaskQueue) Pause() {
	q.mu.Lock()
	q.State = QueuePaused
	q.mu.Unlock()
}

// Resume the queue (wake up all workers)
func (q *TaskQueue) Resume() {
	q.mu.Lock()
	q.State = QueueRunning
	q.cond.Broadcast() // Wake up all workers assigned to this queue
	q.mu.Unlock()
}

// AddTask adds a task to the queue.
func (q *TaskQueue) AddTask(task *Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
}

// ✅ ResetRunningLowTrigger is called by QP when new tasks are added.
func (q *TaskQueue) ResetRunningLowTrigger() {
	q.mu.Lock()
	q.RunningLowTriggered = false
	q.mu.Unlock()
}

// PopTask retrieves the next available task, pausing workers if needed.
func (q *TaskQueue) PopTask() *Task {
	q.mu.Lock()
	for q.State == QueuePaused { // If queue is paused, all workers wait here
		q.cond.Wait()
	}
	q.mu.Unlock()

	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if the queue is running low and trigger QP if needed
	if len(q.tasks) < q.PaginationSize && !q.RunningLowTriggered {
		q.RunningLowTriggered = true // Lock the trigger
		select {
		case q.RunningLowChan <- q.PaginationSize:
		default:
		}
	}

	for _, task := range q.tasks {
		if !task.Locked {
			task.Locked = true
			return task
		}
	}
	return nil
}

// UnlockTask allows reassigning failed/stalled tasks.
func (q *TaskQueue) UnlockTask(taskID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, task := range q.tasks {
		if task.ID == taskID {
			task.Locked = false
			return
		}
	}
}

// QueueSize returns the number of tasks in the queue.
func (q *TaskQueue) QueueSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// SetPaginationSize updates the pagination size and notifies workers.
func (q *TaskQueue) SetPaginationSize(newSize int) {
	q.mu.Lock()
	q.PaginationSize = newSize
	q.mu.Unlock()

	// Push new size into channel (non-blocking)
	select {
	case q.PaginationChan <- newSize:
	default:
		// Drop if channel is full (prevents deadlocks)
	}
}

// GetPaginationChan provides the pagination size stream.
func (q *TaskQueue) GetPaginationChan() <-chan int {
	return q.PaginationChan
}

// SetGlobalSetting updates a global setting dynamically.
func (q *TaskQueue) SetGlobalSetting(key string, value any) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.GlobalSettings[key] = value
}

// GetGlobalSetting retrieves a global setting dynamically.
func (q *TaskQueue) GetGlobalSetting(key string) (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	val, exists := q.GlobalSettings[key]
	return val, exists
}

// AreAllWorkersIdle checks if all workers are idle.
func (q *TaskQueue) AreAllWorkersIdle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, worker := range q.workers {
		if worker.State != WorkerIdle {
			return false
		}
	}
	return true
}