package processing

import (
	"fmt"
	"sync"

	"github.com/Voltaic314/ByteWave/code/core"
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

// TaskQueue manages tasks, workers, and execution settings.
type TaskQueue struct {
	QueueID             string         // Unique identifier for the queue
	Type                QueueType      // Type of queue (traversal or upload)
	Phase               int            // Current phase
	SrcOrDst            string         // "src" or "dst"
	tasks               []*Task        // List of tasks
	workers             []*WorkerBase  // Managed by Conductor
	mu                  sync.Mutex     // Mutex for concurrent access
	PaginationSize      int            // Pagination width for folder content listing
	PaginationChan      chan int       // Channel for live updates
	GlobalSettings      map[string]any // Stores global settings
	State               QueueState     // Tracks if the queue is running or paused
	cond                *sync.Cond     // Queue-specific condition variable
	RunningLowChan      chan int       // Unique channel per queue for RunningLow signals
	RunningLowTriggered bool           // Prevents duplicate RunningLow signals
	RunningLowThreshold int            // Threshold for triggering RunningLow signals
}

// NewTaskQueue initializes a new queue for traversal or upload.
func NewTaskQueue(queueType QueueType, phase int, srcOrDst string, paginationSize int, runningLowChan chan int, runningLowThreshold int) *TaskQueue {
	queueID := fmt.Sprintf("%s-%s", srcOrDst, queueType)
	core.GlobalLogger.LogMessage("info", "Creating new task queue", map[string]any{
		"queueID":        queueID,
		"type":           queueType,
		"phase":          phase,
		"srcOrDst":       srcOrDst,
		"paginationSize": paginationSize,
	})

	q := &TaskQueue{
		QueueID:             queueID,
		Type:                queueType,
		Phase:               phase,
		SrcOrDst:            srcOrDst,
		tasks:               []*Task{},
		workers:             []*WorkerBase{},
		PaginationSize:      paginationSize,
		PaginationChan:      make(chan int, 1),
		GlobalSettings:      make(map[string]any),
		State:               QueueRunning,
		cond:                sync.NewCond(&sync.Mutex{}),
		RunningLowChan:      runningLowChan,
		RunningLowTriggered: false,
		RunningLowThreshold: runningLowThreshold,
	}

	core.GlobalLogger.LogMessage("info", "Task queue created successfully", map[string]any{
		"queueID": queueID,
	})
	return q
}

// Lock locks the mutex for the queue.
func (q *TaskQueue) Lock() {
	core.GlobalLogger.LogMessage("debug", "Acquiring queue lock", map[string]any{
		"queueID": q.QueueID,
	})
	q.mu.Lock()
}

// Unlock unlocks the mutex for the queue.
func (q *TaskQueue) Unlock() {
	core.GlobalLogger.LogMessage("debug", "Releasing queue lock", map[string]any{
		"queueID": q.QueueID,
	})
	q.mu.Unlock()
}

// WaitIfPaused lets a worker block until the queue is resumed.
// Intended to be called only by workers during task polling.
func (q *TaskQueue) WaitIfPaused() {
	q.Lock()
	defer q.Unlock()

	if q.State == QueuePaused {
		core.GlobalLogger.LogMessage("info", "Queue is paused, waiting for resume", map[string]any{
			"queueID": q.QueueID,
		})
		q.cond.Wait()
		core.GlobalLogger.LogMessage("info", "Queue resumed", map[string]any{
			"queueID": q.QueueID,
		})
	}
}

// CheckAndTriggerQP checks the queue size and signals QP if tasks are low.
func (q *TaskQueue) CheckAndTriggerQP() {
	q.Lock()
	defer q.Unlock()

	if q.QueueSize() <= q.RunningLowThreshold && !q.RunningLowTriggered {
		core.GlobalLogger.LogMessage("info", "Queue running low, triggering QP", map[string]any{
			"queueID":     q.QueueID,
			"currentSize": q.QueueSize(),
			"threshold":   q.RunningLowThreshold,
		})
		q.RunningLowTriggered = true
		q.RunningLowChan <- q.Phase
	}
}

// Pause the queue (workers will stop picking up tasks)
func (q *TaskQueue) Pause() {
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("info", "Pausing queue", map[string]any{
		"queueID": q.QueueID,
	})
	q.State = QueuePaused
}

// Resume the queue (wake up all workers)
func (q *TaskQueue) Resume() {
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("info", "Resuming queue", map[string]any{
		"queueID": q.QueueID,
	})
	q.State = QueueRunning
	q.cond.Broadcast() // Wake up all workers assigned to this queue
}

// AddTask adds a task to the queue.
func (q *TaskQueue) AddTask(task *Task) {
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("debug", "Adding task to queue", map[string]any{
		"queueID": q.QueueID,
		"taskID":  task.ID,
	})
	q.tasks = append(q.tasks, task)
}

// ResetRunningLowTrigger is called by QP when new tasks are added.
func (q *TaskQueue) ResetRunningLowTrigger() {
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("debug", "Resetting running low trigger", map[string]any{
		"queueID": q.QueueID,
	})
	q.RunningLowTriggered = false
}

// PopTask retrieves the next available task, pausing workers if needed.
func (q *TaskQueue) PopTask() *Task {
	q.Lock()
	defer q.Unlock()

	if len(q.tasks) < q.RunningLowThreshold && !q.RunningLowTriggered {
		q.RunningLowTriggered = true
		select {
		case q.RunningLowChan <- q.RunningLowThreshold:
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
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("debug", "Unlocking task", map[string]any{
		"queueID": q.QueueID,
		"taskID":  taskID,
	})
	for _, task := range q.tasks {
		if task.ID == taskID {
			task.Locked = false
			return
		}
	}
}

// QueueSize returns the number of tasks in the queue.
func (q *TaskQueue) QueueSize() int {
	q.Lock()
	defer q.Unlock()

	size := len(q.tasks)
	core.GlobalLogger.LogMessage("debug", "Queue size check", map[string]any{
		"queueID": q.QueueID,
		"size":    size,
	})
	return size
}

// SetPaginationSize updates the pagination size and notifies workers.
func (q *TaskQueue) SetPaginationSize(newSize int) {
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("info", "Updating pagination size", map[string]any{
		"queueID": q.QueueID,
		"oldSize": q.PaginationSize,
		"newSize": newSize,
	})
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
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("debug", "Setting global setting", map[string]any{
		"queueID": q.QueueID,
		"key":     key,
	})
	q.GlobalSettings[key] = value
}

// GetGlobalSetting retrieves a global setting dynamically.
func (q *TaskQueue) GetGlobalSetting(key string) (any, bool) {
	q.Lock()
	defer q.Unlock()

	value, exists := q.GlobalSettings[key]
	core.GlobalLogger.LogMessage("debug", "Getting global setting", map[string]any{
		"queueID": q.QueueID,
		"key":     key,
		"exists":  exists,
	})
	return value, exists
}

// AreAllWorkersIdle checks if all workers are idle.
func (q *TaskQueue) AreAllWorkersIdle() bool {
	q.Lock()
	defer q.Unlock()

	allIdle := true
	for _, worker := range q.workers {
		if worker.State != WorkerIdle {
			allIdle = false
			break
		}
	}

	core.GlobalLogger.LogMessage("debug", "Checking worker idle status", map[string]any{
		"queueID":     q.QueueID,
		"allIdle":     allIdle,
		"workerCount": len(q.workers),
	})
	return allIdle
}

// NotifyWorkers sets all idle workers to active.
func (q *TaskQueue) NotifyWorkers() {
	q.Lock()
	defer q.Unlock()

	core.GlobalLogger.LogMessage("debug", "Notifying workers", map[string]any{
		"queueID":     q.QueueID,
		"workerCount": len(q.workers),
	})

	// Set all idle workers to active
	for _, worker := range q.workers {
		if worker.State == WorkerIdle {
			worker.State = WorkerActive
			core.GlobalLogger.LogMessage("debug", "Worker state updated", map[string]any{
				"queueID":  q.QueueID,
				"workerID": worker.ID,
				"state":    worker.State,
			})
		}
	}

	q.cond.Broadcast()
}
