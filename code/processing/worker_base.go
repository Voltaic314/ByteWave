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
	logging.GlobalLogger.LogSystem("info", "worker", "Creating new worker", map[string]any{
		"queueType": queueType,
	})

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
			logging.GlobalLogger.LogWorker("debug", workerBase.ID, "", "Worker received task signal via On handler", map[string]any{
				"topic":     sig.Topic,
				"taskCount": sig.Payload,
			})
		default:
			// Channel already has a wake-up signal, no need to block
		}
	})

	// Subscribe to QP ready signals to re-check queue status after QP startup
	qpReadyTopic := "qp_ready:" + queue.QueueID
	signals.GlobalSR.On(qpReadyTopic, func(sig signals.Signal) {
		logging.GlobalLogger.LogWorker("info", workerBase.ID, "", "Worker received QP ready signal - re-checking queue status", map[string]any{
			"topic": sig.Topic,
		})
		// Re-check if we need to send a running low signal now that QP is listening
		workerBase.sendInitialRunningLowSignalIfNeeded()
	})

	logging.GlobalLogger.LogWorker("info", workerBase.ID, "", "Worker subscribed to task and QP ready signals", map[string]any{
		"taskTopic":    taskTopic,
		"qpReadyTopic": qpReadyTopic,
	})

	logging.GlobalLogger.LogWorker("info", workerBase.ID, "", "Worker base created", map[string]any{
		"queueType": queueType,
		"state":     workerBase.State,
	})
	return workerBase
}

// GenerateID generates a unique identifier using UUID.
func (wb *WorkerBase) GenerateID() string {
	logging.GlobalLogger.LogSystem("info", "worker", "Generating worker ID using UUID", nil)

	id := uuid.New().String()

	logging.GlobalLogger.LogWorker("info", id, "", "Worker ID generated", nil)
	return id
}

// checkAndSignalRunningLowWithSize uses a cached queue size to avoid multiple mutex locks
func (wb *WorkerBase) checkAndSignalRunningLowWithSize(queueSize int) {
	// Send signal if queue is below threshold and we haven't sent one recently
	if queueSize <= wb.Queue.RunningLowThreshold && !wb.runningLowSent {
		runningLowTopic := "tasks_running_low:" + wb.Queue.QueueID

		signals.GlobalSR.Publish(signals.Signal{
			Topic:   runningLowTopic,
			Payload: queueSize,
		})

		wb.runningLowSent = true // Prevent spam until reset

		logging.GlobalLogger.LogWorker("info", wb.ID, "", "Worker sent running low signal to QP", map[string]any{
			"queueID":   wb.Queue.QueueID,
			"queueSize": queueSize,
			"threshold": wb.Queue.RunningLowThreshold,
			"topic":     runningLowTopic,
		})
	}
}

// sendInitialRunningLowSignalIfNeeded sends a running low signal after QP is ready (called after startup delay)
func (wb *WorkerBase) sendInitialRunningLowSignalIfNeeded() {
	queueSize := wb.Queue.QueueSize()

	// If queue is still below threshold after QP startup, send signal regardless of flag
	if queueSize <= wb.Queue.RunningLowThreshold {
		runningLowTopic := "tasks_running_low:" + wb.Queue.QueueID

		signals.GlobalSR.Publish(signals.Signal{
			Topic:   runningLowTopic,
			Payload: queueSize,
		})

		wb.runningLowSent = true // Set flag to prevent immediate re-sending

		logging.GlobalLogger.LogWorker("info", wb.ID, "", "Worker sent initial running low signal (post-startup)", map[string]any{
			"queueID":   wb.Queue.QueueID,
			"queueSize": queueSize,
			"threshold": wb.Queue.RunningLowThreshold,
			"topic":     runningLowTopic,
		})
	}
}

// resetRunningLowFlag resets the flag when new tasks are available
func (wb *WorkerBase) resetRunningLowFlag() {
	if wb.runningLowSent {
		wb.runningLowSent = false
		logging.GlobalLogger.LogWorker("debug", wb.ID, "", "Worker reset running low flag", map[string]any{
			"queueID": wb.Queue.QueueID,
		})
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
			logging.GlobalLogger.LogWorker("info", wb.ID, "", "Queue is no longer running, stopping worker", nil)
			return
		}

		wb.Queue.WaitIfPaused()

		// ✅ Check queue size once and use cached value for all checks
		logging.GlobalLogger.LogWorker("debug", wb.ID, "", "WORKER checking queue size", map[string]any{
			"queueID": wb.Queue.QueueID,
		})

		queueSize := wb.Queue.QueueSize()

		// Signal QP if queue is running low (using cached size)
		wb.checkAndSignalRunningLowWithSize(queueSize)

		if queueSize == 0 {
			wb.State = WorkerIdle
			// Wait for task publication signal via Signal Router On handler
			logging.GlobalLogger.LogWorker("debug", wb.ID, "", "WORKER waiting for task signal", map[string]any{
				"queueID": wb.Queue.QueueID,
			})

			select {
			case <-wb.taskWakeupChan:
				logging.GlobalLogger.LogWorker("debug", wb.ID, "", "WORKER received task wake-up signal", map[string]any{
					"queueID": wb.Queue.QueueID,
				})
				// Reset running low flag since new tasks are available
				wb.resetRunningLowFlag()
				continue
			case <-wb.Queue.StopChan:
				logging.GlobalLogger.LogWorker("info", wb.ID, "", "WORKER received stop signal", map[string]any{
					"queueID": wb.Queue.QueueID,
				})
				return
			}
		}

		logging.GlobalLogger.LogWorker("debug", wb.ID, "", "WORKER calling PopTask()", map[string]any{
			"queueID": wb.Queue.QueueID,
		})
		task := wb.Queue.PopTask()

		if task == nil {
			wb.State = WorkerIdle
			wb.TaskReady = true
			// Signal QP if queue is running low (PopTask returned nil, queue size is 0)
			wb.checkAndSignalRunningLowWithSize(0)

			// Wait for task publication signal via Signal Router On handler
			logging.GlobalLogger.LogWorker("debug", wb.ID, "", "WORKER waiting for task signal (PopTask returned nil)", map[string]any{
				"queueID": wb.Queue.QueueID,
			})

			select {
			case <-wb.taskWakeupChan:
				logging.GlobalLogger.LogWorker("debug", wb.ID, "", "WORKER received task wake-up signal (after PopTask nil)", map[string]any{
					"queueID": wb.Queue.QueueID,
				})
				// Reset running low flag since new tasks are available
				wb.resetRunningLowFlag()
				continue
			case <-wb.Queue.StopChan:
				logging.GlobalLogger.LogWorker("info", wb.ID, "", "WORKER received stop signal (after PopTask nil)", map[string]any{
					"queueID": wb.Queue.QueueID,
				})
				return
			}
		}

		logging.GlobalLogger.LogWorker("info", wb.ID, task.GetPath(), "Worker acquired task", map[string]any{
			"taskID": task.GetID(),
		})

		wb.State = WorkerActive
		wb.TaskReady = false

		if err := process(task); err != nil {
			logging.GlobalLogger.LogWorker("error", wb.ID, task.GetPath(), "Worker task failed", map[string]any{
				"taskID": task.GetID(),
				"error":  err.Error(),
			})
		} else {
			logging.GlobalLogger.LogWorker("info", wb.ID, task.GetPath(), "Worker completed task", map[string]any{
				"taskID": task.GetID(),
			})
		}

		// Remove task from in-progress list (whether successful or failed)
		wb.Queue.RemoveInProgressTask(task.GetID())
	}
}
