// code/core/processing/workers/worker.go
package processing

import (
	"time"

	"github.com/google/uuid"

	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/signals"
)

// WorkerState defines the possible states of a worker.
type WorkerState string

const (
	WorkerIdle   WorkerState = "idle"
	WorkerActive WorkerState = "active"
)

// WorkerBase provides shared fields and logic for all worker types.
type WorkerBase struct {
	ID        string // Unique identifier for the worker
	Queue     *TaskQueue
	QueueType string      // "src" or "dst"
	State     WorkerState // Idle or Active
}

// NewWorkerBase initializes a new WorkerBase with a unique ID.
func NewWorkerBase(queue *TaskQueue, queueType string) *WorkerBase {
	logging.GlobalLogger.Log("info", "System", "Worker", "Creating new worker", map[string]any{
		"queueType": queueType,
	}, "CREATING_NEW_WORKER", queueType)

	workerBase := &WorkerBase{
		Queue:     queue,
		QueueType: queueType,
		State:     WorkerIdle,
	}
	workerBase.ID = workerBase.GenerateID()

	logging.GlobalLogger.Log("info", "System", "Worker", "Worker base created", map[string]any{
		"queueType": queueType,
		"state":     workerBase.State,
	}, "WORKER_BASE_CREATED", workerBase.QueueType)

	return workerBase
}

// GenerateID generates a unique identifier using UUID.
func (wb *WorkerBase) GenerateID() string {
	id := uuid.New().String()
	return id
}

// Run starts the worker using the Signal Router for clean event handling
func (wb *WorkerBase) Run(process func(Task) error) {
	// Channel to coordinate shutdown between signal handlers
	shutdownChan := make(chan struct{})

	// Subscribe to task signals using the beautiful new Signal Router! 📮
	taskTopic := "tasks_published:" + wb.Queue.QueueID
	signals.GlobalSR.On(taskTopic, func(sig signals.Signal) {
		logging.GlobalLogger.Log("debug", "System", "Worker", "Worker received task wake-up signal", map[string]any{
			"queueID": wb.Queue.QueueID,
		}, "WORKER_RECEIVED_TASK_WAKE_UP_SIGNAL", wb.QueueType)

		// 🔄 NEW: Continuous task processing loop
		for {
			task := wb.Queue.LeaseTask()
			if task == nil {
				wb.State = WorkerIdle
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Task found - process it
			wb.State = WorkerActive
			logging.GlobalLogger.Log("debug", "System", "Worker", "Worker processing leased task", map[string]any{
				"queueID":  wb.Queue.QueueID,
				"taskID":   task.GetID(),
				"path":     task.GetPath(),
				"attempts": task.GetAttempts(),
			}, "WORKER_PROCESSING_LEASED_TASK", wb.QueueType)

			// Let the specific worker implementation handle the retry logic,
			// DB writes, and task resolution (NACK vs Finish)
			if err := process(task); err != nil {
				logging.GlobalLogger.Log("error", "System", "Worker", "Task processing returned error", map[string]any{
					"taskID":   task.GetID(),
					"error":    err.Error(),
					"attempts": task.GetAttempts(),
				}, "WORKER_TASK_PROCESSING_ERROR", wb.QueueType)
			} else {
				logging.GlobalLogger.Log("debug", "System", "Worker", "Task processing completed successfully", map[string]any{
					"taskID":   task.GetID(),
					"attempts": task.GetAttempts(),
				}, "WORKER_TASK_PROCESSING_SUCCESS", wb.QueueType)
			}

			// ✨ Continue loop to check for more tasks
			logging.GlobalLogger.Log("debug", "System", "Worker", "Task processed, checking for more tasks", map[string]any{
				"queueID": wb.Queue.QueueID,
			}, "WORKER_CHECKING_FOR_MORE_TASKS", wb.QueueType)
		}
	})

	// Subscribe to stop signals via Signal Router! 🛑
	stopTopic := "stop:" + wb.Queue.QueueID
	signals.GlobalSR.On(stopTopic, func(sig signals.Signal) {
		logging.GlobalLogger.Log("info", "System", "Worker", "Worker received stop signal", map[string]any{
			"queueID": wb.Queue.QueueID,
		}, "WORKER_RECEIVED_STOP_SIGNAL", wb.QueueType)

		// Safe close - won't panic if already closed
		select {
		case <-shutdownChan:
			// Already closed, do nothing
		default:
			close(shutdownChan)
		}
	})

	// Wait for shutdown signal from stop handler
	<-shutdownChan
}
