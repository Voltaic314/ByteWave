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

// QueuePhase defines the phase of the queue (level-based processing).
type QueuePhase int

// TaskQueue manages tasks for either traversal or upload.
type TaskQueue struct {
	QueueID        string        // Unique identifier for the queue (e.g., "traversal-src-phase-1")
	Type           QueueType     // Type of queue (traversal or upload)
	Phase          QueuePhase    // Current phase (for level-based processing)
	SrcOrDst       string        // "src" or "dst"
	tasks          []*Task       // List of tasks
	mu             sync.Mutex    // Mutex to handle concurrent access
	workers        int           // Number of workers assigned to this queue
	LastModified   time.Time     // Timestamp of the last queue modification
	PaginationSize int           // Dynamic pagination size (default: 100)
}

// NewTaskQueue initializes a new queue for traversal or upload.
func NewTaskQueue(queueType QueueType, phase QueuePhase, srcOrDst string, paginationSize int) *TaskQueue {
	queueID := fmt.Sprintf("%s-%s-phase-%d", queueType, srcOrDst, phase)
	if paginationSize <= 0 {
		paginationSize = 100 // Default pagination size
	}
	return &TaskQueue{
		QueueID:        queueID,
		Type:           queueType,
		Phase:          phase,
		SrcOrDst:       srcOrDst,
		tasks:          []*Task{},
		workers:        0,
		LastModified:   time.Now(),
		PaginationSize: paginationSize,
	}
}

// AddTask adds a task to the queue.
func (q *TaskQueue) AddTask(task *Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.tasks = append(q.tasks, task)
	q.LastModified = time.Now()
}

// GetTask retrieves and removes the next task from the queue.
func (q *TaskQueue) GetTask() *Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tasks) == 0 {
		return nil
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	q.LastModified = time.Now()

	return task
}

// QueueSize returns the number of tasks in the queue.
func (q *TaskQueue) QueueSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// AssignWorker increments the worker count for this queue.
func (q *TaskQueue) AssignWorker() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers++
}

// ReleaseWorker decrements the worker count for this queue.
func (q *TaskQueue) ReleaseWorker() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.workers > 0 {
		q.workers--
	}
}

// GetWorkerCount returns the number of workers currently assigned to this queue.
func (q *TaskQueue) GetWorkerCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.workers
}

// SetPaginationSize updates the pagination size dynamically.
func (q *TaskQueue) SetPaginationSize(size int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if size > 0 {
		q.PaginationSize = size
		q.LastModified = time.Now()
	}
}

// GetPaginationSize retrieves the current pagination size.
func (q *TaskQueue) GetPaginationSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.PaginationSize
}
