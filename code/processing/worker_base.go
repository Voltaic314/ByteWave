// code/core/processing/workers/worker.go
package processing

import (
	"github.com/google/uuid"

	"github.com/Voltaic314/ByteWave/code/logging"
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
	TaskReady bool
}

// NewWorkerBase initializes a new WorkerBase with a unique ID.
func NewWorkerBase(queue *TaskQueue, queueType string) *WorkerBase {
	logging.GlobalLogger.LogSystem("info", "worker", "Creating new worker", map[string]any{
		"queueType": queueType,
	})

	workerBase := &WorkerBase{
		Queue:     queue,
		QueueType: queueType,
		State:     WorkerIdle,
		TaskReady: false,
	}
	workerBase.ID = workerBase.GenerateID()

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

		// ✅ Lockless check (fast path)
		if wb.Queue.QueueSize() == 0 {
			wb.State = WorkerIdle
			// 🆕 NEW: Signal that worker is idle and may need more tasks
			wb.Queue.SignalWorkerIdle()
			// logging.GlobalLogger.LogMessage("info", "Queue empty, worker entering wait state", map[string]any{
			// 	"workerID": wb.ID,
			// 	"state":    wb.State,
			// })
			// This will block until QP calls cond.Broadcast()
			wb.Queue.WaitForWork()
			continue
		}

		task := wb.Queue.PopTask()

		if task == nil {
			wb.State = WorkerIdle
			wb.TaskReady = true
			// 🆕 NEW: Signal that worker is idle and may need more tasks
			wb.Queue.SignalWorkerIdle()
			wb.Queue.WaitForWork()
			continue // skip sleep - just like my college days...
		}

		logging.GlobalLogger.LogWorker("info", wb.ID, task.GetPath(), "Worker popped task from queue", map[string]any{
			"queueType": wb.QueueType,
		})

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
