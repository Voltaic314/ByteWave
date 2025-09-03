// code/core/processing/workers/worker.go
package processing

import (
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
	ID             string // Unique identifier for the worker
	Queue          *TaskQueue
	QueueType      string      // "src" or "dst"
	State          WorkerState // Idle or Active
	TaskReady      bool
	taskWakeupChan chan struct{} // Simple wake-up channel for task notifications
	runningLowSent bool          // Flag to prevent spamming running low signals
}

// NewWorkerBase initializes a new WorkerBase with a unique ID.
func NewWorkerBase(queue *TaskQueue, queueType string) *WorkerBase {
	logging.GlobalLogger.Log("info", "System", "Worker", "Creating new worker", map[string]any{
		"queueType": queueType,
	}, "CREATING_NEW_WORKER", queueType,
	)

	workerBase := &WorkerBase{
		Queue:          queue,
		QueueType:      queueType,
		State:          WorkerIdle,
		TaskReady:      false,
		taskWakeupChan: make(chan struct{}, 1), // Non-blocking wake-up channel
		runningLowSent: false,                  // Reset on worker creation
	}
	workerBase.ID = workerBase.GenerateID()

	// Subscribe to task publication signals using Signal Router's On method
	taskTopic := "tasks_published:" + queue.QueueID
	signals.GlobalSR.On(taskTopic, func(sig signals.Signal) {
		// Wake up the worker when tasks are published
		select {
		case workerBase.taskWakeupChan <- struct{}{}:
			logging.GlobalLogger.Log("debug", "System", "Worker", "Worker received task signal via On handler", map[string]any{
				"topic":     sig.Topic,
				"taskCount": sig.Payload,
			}, "WORKER_RECEIVED_TASK_SIGNAL_VIA_ON_HANDLER", workerBase.QueueType,
			)
		default:
			// Channel already has a wake-up signal, no need to block
		}
	})

	logging.GlobalLogger.Log("info", "System", "Worker", "Worker subscribed to task signals", map[string]any{
		"taskTopic": taskTopic,
		"queueType": workerBase.QueueType,
	}, "WORKER_SUBSCRIBED_TO_SIGNAL", workerBase.QueueType,
	)

	logging.GlobalLogger.Log("info", "System", "Worker", "Worker base created", map[string]any{
		"queueType": queueType,
		"state":     workerBase.State,
	}, "WORKER_BASE_CREATED", workerBase.QueueType,
	)
	return workerBase
}

// GenerateID generates a unique identifier using UUID.
func (wb *WorkerBase) GenerateID() string {
	id := uuid.New().String()
	return id
}

// resetRunningLowFlag resets the flag when new tasks are available
func (wb *WorkerBase) resetRunningLowFlag() {
	if wb.runningLowSent {
		wb.runningLowSent = false
		logging.GlobalLogger.Log("debug", "System", "Worker", "Worker reset running low flag", map[string]any{
			"queueID": wb.Queue.QueueID,
			"flag":    "runningLowSent",
		}, "WORKER_RESET_FLAG", wb.QueueType,
		)
	}
}

// RunMainLoop is a generic polling loop that can be called by any worker type.
func (wb *WorkerBase) Run(process func(Task) error) {
	for {
		// logging.GlobalLogger.LogMessage("info", "Worker entering polling cycle", map[string]any{
		// 	"workerID":  wb.ID,
		// 	"queueType": wb.QueueType,
		// 	"state":     wb.State,
		// })

		if wb.Queue.State != QueueRunning {
			logging.GlobalLogger.Log("info", "System", "Worker", "Queue is no longer running, stopping worker", nil, "WORKER_STOPPING", wb.QueueType,
			)
			return
		}

		wb.Queue.WaitIfPaused()

		// ✅ Check queue size once and use cached value for all checks
		logging.GlobalLogger.Log("debug", "System", "Worker", "WORKER checking queue size", map[string]any{
			"queueID": wb.Queue.QueueID,
		}, "QUEUE_SIZE_CHECK", wb.QueueType,
		)

		queueSize := wb.Queue.QueueSize()

		if queueSize == 0 {
			wb.State = WorkerIdle
			// Wait for task publication signal via Signal Router On handler
			logging.GlobalLogger.Log("debug", "System", "Worker", "WORKER waiting for task signal", map[string]any{
				"queueID": wb.Queue.QueueID,
			}, "WORKER_WAITING_FOR_TASK", wb.QueueType,
			)

			select {
			case <-wb.taskWakeupChan:
				logging.GlobalLogger.Log("debug", "System", "Worker", "WORKER received task wake-up signal", map[string]any{
					"queueID": wb.Queue.QueueID,
				}, "WORKER_RECEIVED_TASK_WAKE_UP_SIGNAL", wb.QueueType,
				)
				// Reset running low flag since new tasks are available
				wb.resetRunningLowFlag()
				continue
			case <-wb.Queue.StopChan:
				logging.GlobalLogger.Log("info", "System", "Worker", "WORKER received stop signal", map[string]any{
					"queueID": wb.Queue.QueueID,
				}, "WORKER_RECEIVED_STOP_SIGNAL", wb.QueueType,
				)
				return
			}
		}

		task := wb.Queue.PopTask()

		if task == nil {
			wb.State = WorkerIdle
			wb.TaskReady = true

			// Wait for task publication signal via Signal Router On handler
			logging.GlobalLogger.Log("debug", "System", "Worker", "WORKER waiting for task signal (PopTask returned nil)", map[string]any{
				"queueID": wb.Queue.QueueID,
			}, "WORKER_WAITING_FOR_TASK_SIGNAL", wb.QueueType,
			)

			select {
			case <-wb.taskWakeupChan:
				logging.GlobalLogger.Log("debug", "System", "Worker", "WORKER received task wake-up signal (after PopTask nil)", map[string]any{
					"queueID": wb.Queue.QueueID,
				}, "WORKER_RECEIVED_TASK_WAKE_UP_SIGNAL", wb.QueueType,
				)
				// Reset running low flag since new tasks are available
				wb.resetRunningLowFlag()
				continue
			case <-wb.Queue.StopChan:
				logging.GlobalLogger.Log("info", "System", "Worker", "WORKER received stop signal (after PopTask nil)", map[string]any{
					"queueID": wb.Queue.QueueID,
				}, "WORKER_RECEIVED_STOP_SIGNAL", wb.QueueType,
				)
				return
			}
		}

		logging.GlobalLogger.Log("info", "System", "Worker", "Worker acquired task", map[string]any{
			"taskID": task.GetID(),
		}, "WORKER_ACQUIRED_TASK", wb.QueueType,
		)

		wb.State = WorkerActive
		wb.TaskReady = false

		if err := process(task); err != nil {
			logging.GlobalLogger.Log("error", "System", "Worker", "Worker task failed", map[string]any{
				"taskID": task.GetID(),
				"error":  err.Error(),
			}, "WORKER_TASK_FAILED", wb.QueueType,
			)
		} else {
			logging.GlobalLogger.Log("info", "System", "Worker", "Worker completed task", map[string]any{
				"taskID": task.GetID(),
			}, "WORKER_TASK_COMPLETED", wb.QueueType,
			)
		}

		// Remove task from in-progress list (whether successful or failed)
		wb.Queue.RemoveInProgressTask(task.GetID())
	}
}
