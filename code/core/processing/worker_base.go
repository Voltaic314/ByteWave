// code/core/processing/workers/worker.go
package processing

import (
	"time"
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
	ID        string       // Unique identifier for the worker
	Logger    *core.Logger
	Queue     *TaskQueue
	QueueType string       // "src" or "dst"
	State     WorkerState  // Idle or Active
	TaskReady bool
}

// NewWorkerBase initializes a new WorkerBase with a unique ID.
func NewWorkerBase(logger *core.Logger, queue *TaskQueue, queueType string) *WorkerBase {
	workerBase := &WorkerBase{
		Logger:    logger,
		Queue:     queue,
		QueueType: queueType,
		State:     WorkerIdle,
		TaskReady: false,
	}
	workerBase.ID = workerBase.GenerateID() // Generate a unique ID for the worker
	return workerBase
}

// GenerateID generates a random string of 5 alphanumeric characters.
func (wb *WorkerBase) GenerateID() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	var sb strings.Builder
	for i := 0; i < 5; i++ {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}
	return sb.String()
}

// RunMainLoop is a generic polling loop that can be called by any worker type.
func (wb *WorkerBase) RunMainLoop(fetchAndProcess func() bool) {
	for {
		wb.Queue.WaitIfPaused(wb.Logger)

		taskAvailable := fetchAndProcess()
		if taskAvailable {
			wb.TaskReady = false
			continue
		}

		// No task available
		if wb.TaskReady {
			wb.Logger.LogMessage("info", "Worker still ready but no task available", map[string]any{
				"queue_type": wb.QueueType,
				"worker_id":  wb.ID,
			})
			wb.TaskReady = false
		}

		time.Sleep(2 * time.Second)
	}
}
