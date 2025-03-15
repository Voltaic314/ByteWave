package processing

import (
	"sync"
	"time"
	"fmt"
)

// QueueType defines whether a queue is for traversal or uploading.
type QueueType string

const (
	TraversalQueueType QueueType = "traversal"
	UploadQueueType    QueueType = "upload"
)

// TaskQueue manages tasks and stores global execution settings.
type TaskQueue struct {
	QueueID        string              // Unique identifier for the queue
	Type           QueueType           // Type of queue (traversal or upload)
	Phase          int                 // Current phase
	SrcOrDst       string              // "src" or "dst"
	tasks          []*Task             // List of tasks
	mu             sync.Mutex          // Mutex for concurrent access
	LastModified   time.Time           // Last modification timestamp
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
		LastModified:   time.Now(),
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
	q.LastModified = time.Now()
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
			q.LastModified = time.Now()
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
			q.LastModified = time.Now()
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
	q.LastModified = time.Now()
}

// GetGlobalSetting retrieves a global setting dynamically.
func (q *TaskQueue) GetGlobalSetting(key string) (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	val, exists := q.GlobalSettings[key]
	return val, exists
}
