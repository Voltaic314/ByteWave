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
	logging.GlobalLogger.LogMessage("info", "Creating new worker", map[string]any{
		"queueType": queueType,
	})

	workerBase := &WorkerBase{
		Queue:     queue,
		QueueType: queueType,
		State:     WorkerIdle,
		TaskReady: false,
	}
	workerBase.ID = workerBase.GenerateID()

	logging.GlobalLogger.LogMessage("info", "Worker base created", map[string]any{
		"workerID":  workerBase.ID,
		"queueType": queueType,
		"state":     workerBase.State,
	})
		return workerBase
	}
	
// GenerateID generates a unique identifier using UUID.
func (wb *WorkerBase) GenerateID() string {
	logging.GlobalLogger.LogMessage("info", "Generating worker ID using UUID", nil)

	id := uuid.New().String()

	logging.GlobalLogger.LogMessage("info", "Worker ID generated", map[string]any{
		"workerID": id,
	})
	return id
}

// RunMainLoop is a generic polling loop that can be called by any worker type.
func (wb *WorkerBase) Run(process func(*Task) error) {
	for {
		// logging.GlobalLogger.LogMessage("info", "Worker entering polling cycle", map[string]any{
		// 	"workerID":  wb.ID,
		// 	"queueType": wb.QueueType,
		// 	"state":     wb.State,
		// })

		if wb.Queue.State != QueueRunning {
			logging.GlobalLogger.LogMessage("info", "Queue is no longer running, stopping worker", map[string]any{
				"workerID": wb.ID,
			})
			return
		}

		wb.Queue.WaitIfPaused()

		// ✅ Lockless check (fast path)
		if wb.Queue.QueueSize() == 0 {
			wb.State = WorkerIdle
			// logging.GlobalLogger.LogMessage("info", "Queue empty, worker entering wait state", map[string]any{
			// 	"workerID": wb.ID,
			// 	"state":    wb.State,
			// })
			// This will block until QP calls cond.Broadcast()
			wb.Queue.WaitForWork()
			continue
		}

		task := wb.Queue.PopTask()

		logging.GlobalLogger.LogMessage("info", "Worker popped task from queue", map[string]any{
			"workerID":  wb.ID,
			"task":      task,
			"queueType": wb.QueueType,
		})

		if task == nil {
			wb.State = WorkerIdle
			wb.TaskReady = true
			wb.Queue.WaitForWork()
			continue // skip sleep - just like my college days...
		}

		logging.GlobalLogger.LogMessage("info", "Worker acquired task", map[string]any{
			"workerID": wb.ID,
			"taskID":   task.ID,
			"path":     task.GetPath(),
		})

		wb.State = WorkerActive
		wb.TaskReady = false

		if err := process(task); err != nil {
			logging.GlobalLogger.LogMessage("error", "Worker task failed", map[string]any{
				"workerID": wb.ID,
				"taskID":   task.ID,
				"error":    err.Error(),
			})
		} else {
			logging.GlobalLogger.LogMessage("info", "Worker completed task", map[string]any{
				"workerID": wb.ID,
				"taskID":   task.ID,
			})
		}
	}
}
