// code/core/processing/workers/worker.go
package processing

import (
	"math/rand"
	"strings"

	"github.com/Voltaic314/ByteWave/code/core"
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
	core.GlobalLogger.LogMessage("info", "Creating new worker", map[string]any{
		"queueType": queueType,
	})

	workerBase := &WorkerBase{
		Queue:     queue,
		QueueType: queueType,
		State:     WorkerIdle,
		TaskReady: false,
	}
	workerBase.ID = workerBase.GenerateID()

	core.GlobalLogger.LogMessage("info", "Worker base created", map[string]any{
		"workerID":  workerBase.ID,
		"queueType": queueType,
		"state":     workerBase.State,
	})
	return workerBase
}

// GenerateID generates a random string of 5 alphanumeric characters.
func (wb *WorkerBase) GenerateID() string {
	core.GlobalLogger.LogMessage("info", "Generating worker ID", nil)

	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	var sb strings.Builder
	for i := 0; i < 5; i++ {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}
	id := sb.String()

	core.GlobalLogger.LogMessage("info", "Worker ID generated", map[string]any{
		"workerID": id,
	})
	return id
}

// RunMainLoop is a generic polling loop that can be called by any worker type.
func (wb *WorkerBase) Run(process func(*Task) error) {
	for {
		// core.GlobalLogger.LogMessage("info", "Worker entering polling cycle", map[string]any{
		// 	"workerID":  wb.ID,
		// 	"queueType": wb.QueueType,
		// 	"state":     wb.State,
		// })

		wb.Queue.WaitIfPaused() 

		// ✅ Lockless check (fast path)
		if wb.Queue.QueueSize() == 0 {
			wb.State = WorkerIdle
			// core.GlobalLogger.LogMessage("info", "Queue empty, worker entering wait state", map[string]any{
			// 	"workerID": wb.ID,
			// 	"state":    wb.State,
			// })
			// This will block until QP calls cond.Broadcast()
			wb.Queue.WaitForWork() 
			continue
		}

		task := wb.Queue.PopTask()

		core.GlobalLogger.LogMessage("info", "Worker popped task from queue", map[string]any{
			"workerID":  wb.ID,
			"task":      task,
			"queueType": wb.QueueType,
		})

		if task == nil {
			wb.State = WorkerIdle
			continue // skip sleep - just like my college days...
		}

		core.GlobalLogger.LogMessage("info", "Worker acquired task", map[string]any{
			"workerID": wb.ID,
			"taskID":   task.ID,
			"path":     task.GetPath(),
		})

		wb.State = WorkerActive
		wb.TaskReady = false

		if err := process(task); err != nil {
			core.GlobalLogger.LogMessage("error", "Worker task failed", map[string]any{
				"workerID": wb.ID,
				"taskID":   task.ID,
				"error":    err.Error(),
			})
		} else {
			core.GlobalLogger.LogMessage("info", "Worker completed task", map[string]any{
				"workerID": wb.ID,
				"taskID":   task.ID,
			})
		}
	}
}
