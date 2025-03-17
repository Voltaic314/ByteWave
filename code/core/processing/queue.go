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
	QueueID        string              // Unique identifier for the queue
	Type           QueueType           // Type of queue (traversal or upload)
	Phase          int                 // Current phase
	SrcOrDst       string              // "src" or "dst"
	tasks          []*Task             // List of tasks
	workers        []*Worker           // List of active workers
	mu             sync.Mutex          // Mutex for concurrent access
	PaginationSize int                 // Pagination width for folder content listing
	PaginationChan chan int            // Channel for live updates
	GlobalSettings map[string]any      // Stores global settings,
	Locked         bool                // Prevents workers from picking up tasks and QP from publishing new work.
}

// NewTaskQueue initializes a new queue for traversal or upload.
func NewTaskQueue(queueType QueueType, phase int, srcOrDst string, paginationSize int) *TaskQueue {
	queueID := fmt.Sprintf("%s-%s-phase-%d", queueType, srcOrDst, phase)
	return &TaskQueue{
		QueueID:        queueID,
		Type:           queueType,
		Phase:          phase,
		SrcOrDst:       srcOrDst,
		tasks:          []*Task{},
		workers:        []*Worker{},
		PaginationSize: paginationSize,
		PaginationChan: make(chan int, 1),
		GlobalSettings: make(map[string]any),
		Locked:         false,
	}
}

// Lock the queue (pause migration)
func (q *TaskQueue) Lock() {
	q.Locked = true
}

func (q *TaskQueue) Unlock() {
	q.Locked = false
}

// AddTask adds a task to the queue.
func (q *TaskQueue) AddTask(task *Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
}

// PopTask retrieves and locks the next available task.
func (q *TaskQueue) PopTask() *Task {
	if q.Locked {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, task := range q.tasks {
		if !task.Locked { // Ensure it's not taken by another worker
			task.Locked = true
			return task
		}
	}
	return nil // No available tasks
}

// UnlockTask allows a worker or BG worker to reassign a failed/stalled task.
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

// AddWorker registers a new worker instance.
func (q *TaskQueue) AddWorker(workerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers = append(q.workers, &Worker{ID: workerID, State: WorkerIdle})
}

// RemoveWorker removes a worker from the queue.
func (q *TaskQueue) RemoveWorker(workerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, worker := range q.workers {
		if worker.ID == workerID {
			q.workers = append(q.workers[:i], q.workers[i+1:]...)
			return
		}
	}
}

// SetWorkerState updates the state of a worker.
func (q *TaskQueue) SetWorkerState(workerID string, state WorkerState) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, worker := range q.workers {
		if worker.ID == workerID {
			worker.State = state
			return
		}
	}
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
