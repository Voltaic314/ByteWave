package processing

import (
	"fmt"
	"sync"

	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/signals"
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
	QueueRunning  QueueState = "running"
	QueuePaused   QueueState = "paused"
	QueueComplete QueueState = "complete"
)

// TaskQueue manages tasks, workers, and execution settings.
type TaskQueue struct {
	QueueID             string         // Unique identifier for the queue
	Type                QueueType      // Type of queue (traversal or upload)
	Phase               int            // Current phase
	SrcOrDst            string         // "src" or "dst"
	tasks               []Task         // List of tasks
	inProgressTasks     []Task         // Tasks currently being processed by workers
	workers             []*WorkerBase  // Managed by Conductor
	mu                  sync.Mutex     // Mutex for concurrent access
	PaginationSize      int            // Pagination width for folder content listing
	PaginationChan      chan int       // Channel for live updates
	State               QueueState     // Tracks if the queue is running or paused
	cond                *sync.Cond     // Queue-specific condition variable
	SignalTopicBase     string         // e.g. "src-traversal"
	RunningLowTriggered bool           // Prevents duplicate RunningLow signals
	RunningLowThreshold int            // Threshold for triggering RunningLow signals
	IdleTriggered       bool           // Prevents duplicate idle signals
	RetryThreshold      int            // Threshold for retrying tasks
}

// NewTaskQueue initializes a new queue for traversal or upload.
func NewTaskQueue(queueType QueueType, phase int, srcOrDst string, paginationSize int, runningLowThreshold int, retryThreshold int) *TaskQueue {
	queueID := fmt.Sprintf("%s-%s", srcOrDst, queueType)
	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Creating new task queue", map[string]any{
			"queueID":        queueID,
			"type":           queueType,
			"phase":          phase,
			"srcOrDst":       srcOrDst,
			"paginationSize": paginationSize,
			"retryThreshold": retryThreshold,
		}, "CREATE_NEW_TASK_QUEUE", "All",
	)

	q := &TaskQueue{
		QueueID:             queueID,
		Type:                queueType,
		Phase:               phase,
		SrcOrDst:            srcOrDst,
		tasks:               []Task{},
		inProgressTasks:     []Task{},
		workers:             []*WorkerBase{},
		PaginationSize:      paginationSize,
		PaginationChan:      make(chan int, 1),
		State:               QueueRunning,
		SignalTopicBase:     queueID,
		RunningLowTriggered: false,
		RunningLowThreshold: runningLowThreshold,
		IdleTriggered:       false,
		RetryThreshold:      retryThreshold,
	}
	q.cond = sync.NewCond(&q.mu)

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Task queue created successfully", map[string]any{
			"queueID": queueID,
		}, "TASK_QUEUE_CREATED_SUCCESSFULLY", srcOrDst,
	)
	return q
}

// Lock locks the mutex for the queue.
func (q *TaskQueue) Lock() {
	q.mu.Lock()
}

// Unlock unlocks the mutex for the queue.
func (q *TaskQueue) Unlock() {
	q.mu.Unlock()
}

// WaitIfPaused lets a worker block until the queue is resumed.
// Intended to be called only by workers during task polling.
func (q *TaskQueue) WaitIfPaused() {
	q.Lock()

	if q.State == QueuePaused {
		q.Unlock()
		logging.GlobalLogger.Log(
			"info", "System", "Queue", "Queue is paused, waiting for resume", map[string]any{
				"queueID": q.QueueID,
			}, "QUEUE_PAUSED_WAITING_FOR_RESUME", q.SrcOrDst,
		)
		q.cond.Wait()
		logging.GlobalLogger.Log(
			"info", "System", "Queue", "Queue resumed", map[string]any{
				"queueID": q.QueueID,
			}, "QUEUE_RESUMED", q.SrcOrDst,
		)
	} else {
		q.Unlock()
		logging.GlobalLogger.Log(
			"info", "System", "Queue", "Queue is running, no wait needed", map[string]any{
				"queueID": q.QueueID,
			}, "QUEUE_RUNNING_NO_WAIT_NEEDED", q.SrcOrDst,
		)
	}
}

// Pause the queue (workers will stop picking up tasks)
func (q *TaskQueue) Pause() {
	q.Lock()
	defer q.Unlock()

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Pausing queue", map[string]any{
			"queueID": q.QueueID,
		}, "PAUSING_QUEUE", q.SrcOrDst,
	)
	q.State = QueuePaused
}

// Resume the queue (wake up all workers)
func (q *TaskQueue) Resume() {
	q.Lock()
	defer q.Unlock()

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Resuming queue", map[string]any{
			"queueID": q.QueueID,
		}, "RESUMING_QUEUE", q.SrcOrDst,
	)
	q.State = QueueRunning
	/*
		TODO: This needs to change to workers subscribing to a signal
		and broadcasting to that signal instead
	*/
	q.cond.Broadcast() // Wake up all workers assigned to this queue
}

func (q *TaskQueue) AddTasks(tasks []Task) {
	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Adding tasks to queue", map[string]any{
			"queueID":   q.QueueID,
			"taskCount": len(tasks),
		}, "ADDING_MULTIPLE_TASKS_TO_QUEUE", q.SrcOrDst,
	)
	q.Lock()
	q.tasks = append(q.tasks, tasks...)
	q.Unlock()

	// ✅ Signal workers via Signal Router instead of condition variables
	taskTopic := "tasks_published:" + q.QueueID
	signals.GlobalSR.Publish(signals.Signal{
		Topic:   taskTopic,
		Payload: len(q.tasks),
	})

	logging.GlobalLogger.Log(
		"debug", "System", "Queue", "📡 SIGNAL: published task notification", map[string]any{
			"queueID":   q.QueueID,
			"topic":     taskTopic,
			"taskCount": len(q.tasks),
		}, "PUBLISHED_TASK_NOTIFICATION", q.SrcOrDst,
	)
}

// PopTask removes and returns the first task from the queue (FIFO).
// This implements a simple pop-and-process model for task handling.
func (q *TaskQueue) PopTask() Task {
	q.Lock()
	defer q.Unlock()

	totalTasks := len(q.tasks)

	logging.GlobalLogger.Log(
		"debug", "System", "Queue", "PopTask called - queue state", map[string]any{
			"queueID":    q.QueueID,
			"totalTasks": totalTasks,
		}, "POP_TASK_QUEUE_STATE", q.SrcOrDst,
	)

	if totalTasks == 0 {
		logging.GlobalLogger.Log(
			"debug", "System", "Queue", "No tasks available to pop", map[string]any{
				"queueID": q.QueueID,
			}, "NO_TASKS_AVAILABLE_TO_POP", q.SrcOrDst,
		)
		return nil
	}

	// Pop first task (FIFO)
	task := q.tasks[0]
	q.tasks = q.tasks[1:] // Remove from slice

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Task popped successfully", map[string]any{
			"queueID":        q.QueueID,
			"taskID":         task.GetID(),
			"path":           task.GetPath(),
			"attempts":       task.GetAttempts(),
			"remainingTasks": len(q.tasks),
		}, "TASK_POPPED_SUCCESSFULLY", q.SrcOrDst,
	)

	return task
}

// ReAddFailedTask adds a failed task back to the end of the queue for retry.
func (q *TaskQueue) ReAddFailedTask(task Task) {
	q.Lock()
	q.tasks = append(q.tasks, task) // Add to end of queue
	q.Unlock()

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Failed task re-added to queue for retry", map[string]any{
			"queueID":  q.QueueID,
			"taskID":   task.GetID(),
			"path":     task.GetPath(),
			"attempts": task.GetAttempts(),
		}, "FAILED_TASK_RE_ADDED", q.SrcOrDst,
	)

	// Signal workers that a new task is available
	taskTopic := "tasks_published:" + q.QueueID
	signals.GlobalSR.Publish(signals.Signal{
		Topic:   taskTopic,
		Payload: len(q.tasks),
	})
}

// QueueSize returns the number of tasks in the queue.
func (q *TaskQueue) QueueSize() int {
	q.Lock()
	defer q.Unlock()

	size := len(q.tasks)
	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Queue size check", map[string]any{
			"queueID": q.QueueID,
			"size":    size,
		}, "QUEUE_SIZE_CHECK", q.SrcOrDst,
	)
	return size
}

// SetPaginationSize updates the pagination size and notifies workers.
func (q *TaskQueue) SetPaginationSize(newSize int) {
	q.Lock()
	defer q.Unlock()

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Updating pagination size", map[string]any{
			"queueID": q.QueueID,
			"oldSize": q.PaginationSize,
			"newSize": newSize,
		}, "UPDATING_PAGINATION_SIZE", q.SrcOrDst,
	)
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
	q.Lock()
	defer q.Unlock()
	return q.PaginationChan
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

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Checking worker idle status", map[string]any{
			"queueID":     q.QueueID,
			"allIdle":     allIdle,
			"workerCount": len(q.workers),
		}, "CHECKING_IDLE_WORKERS", q.SrcOrDst,
	)
	return allIdle
}

// TrackedPaths returns a map of paths currently in the queue and optionally in-progress
func (q *TaskQueue) TrackedPaths(includeInProgress bool) map[string]bool {
	q.Lock()
	defer q.Unlock()

	tracked := make(map[string]bool)

	// Add all queued task paths
	for _, task := range q.tasks {
		if path := task.GetPath(); path != "" {
			tracked[path] = true
		}
	}

	// Optionally add in-progress paths (tasks that have been popped but not completed)
	if includeInProgress {
		for _, task := range q.inProgressTasks {
			if path := task.GetPath(); path != "" {
				tracked[path] = true
			}
		}
	}

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Tracked paths retrieved", map[string]any{
			"queueID":           q.QueueID,
			"includeInProgress": includeInProgress,
			"trackedCount":      len(tracked),
			"queuedCount":       len(q.tasks),
			"inProgressCount":   len(q.inProgressTasks),
		}, "TRACKED_PATHS_RETRIEVED", q.SrcOrDst,
	)

	return tracked
}

// AddInProgressTask adds a task to the in-progress list
func (q *TaskQueue) AddInProgressTask(task Task) {
	q.Lock()
	defer q.Unlock()

	q.inProgressTasks = append(q.inProgressTasks, task)

	logging.GlobalLogger.Log(
		"info", "System", "Queue", "Task added to in-progress", map[string]any{
			"queueID": q.QueueID,
			"taskID":  task.GetID(),
			"path":    task.GetPath(),
		}, "TASK_ADDED_TO_IN_PROGRESS", q.SrcOrDst,
	)
}

// RemoveInProgressTask removes a task from the in-progress list
func (q *TaskQueue) RemoveInProgressTask(taskID string) {
	q.Lock()
	defer q.Unlock()

	for i, task := range q.inProgressTasks {
		if task.GetID() == taskID {
			// Remove from slice
			q.inProgressTasks = append(q.inProgressTasks[:i], q.inProgressTasks[i+1:]...)

			logging.GlobalLogger.Log(
				"info", "System", "Queue", "Task removed from in-progress", map[string]any{
					"queueID": q.QueueID,
					"taskID":  taskID,
					"path":    task.GetPath(),
				}, "TASK_REMOVED_FROM_IN_PROGRESS", q.SrcOrDst,
			)
			return
		}
	}

	logging.GlobalLogger.Log(
		"warning", "System", "Queue", "Task not found in in-progress list", map[string]any{
			"queueID": q.QueueID,
			"taskID":  taskID,
		}, "TASK_NOT_FOUND_IN_IN_PROGRESS_LIST", q.SrcOrDst,
	)
}

// GetAllTaskPaths returns all paths from both queued and in-progress tasks
func (q *TaskQueue) GetAllTaskPaths() []string {
	q.Lock()
	defer q.Unlock()

	paths := make([]string, 0, len(q.tasks)+len(q.inProgressTasks))

	// Add queued task paths
	for _, task := range q.tasks {
		if path := task.GetPath(); path != "" {
			paths = append(paths, path)
		}
	}

	// Add in-progress task paths
	for _, task := range q.inProgressTasks {
		if path := task.GetPath(); path != "" {
			paths = append(paths, path)
		}
	}

	return paths
}

func (q *TaskQueue) ResetRunningLowFlag() {
	q.Lock()
	defer q.Unlock()

	q.RunningLowTriggered = false
}

func (q *TaskQueue) TasksEmpty() bool {
	q.Lock()
	defer q.Unlock()

	return q.QueueSize() == 0
}

func (q *TaskQueue) TasksRunningLow() bool {
	q.Lock()
	defer q.Unlock()

	if q.State != QueueRunning {
		return false
	}

	return q.QueueSize() < q.RunningLowThreshold
}
