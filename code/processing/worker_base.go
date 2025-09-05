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
	signalActorID  signals.ActorID
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
	}
	workerBase.ID = workerBase.GenerateID()

	// Subscribe to signals
	workerBase.SubscribeToSignals()

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

func (wb *WorkerBase) SubscribeToSignals() {
	taskTopic := "tasks_published:" + wb.Queue.QueueID
	wb.signalActorID = signals.GlobalSR.On(taskTopic, func(sig signals.Signal) {
		select {
		case wb.taskWakeupChan <- struct{}{}:
			logging.GlobalLogger.Log("debug", "System", "Worker", "Worker received task signal via On handler", map[string]any{
				"topic":     sig.Topic,
				"taskCount": sig.Payload,
			}, "WORKER_RECEIVED_TASK_SIGNAL_VIA_ON_HANDLER", wb.QueueType)
		default:
			// Channel already has a wake-up signal, no need to block
		}
	})
	logging.GlobalLogger.Log("info", "System", "Worker", "Worker subscribed to task signals", map[string]any{
		"taskTopic": taskTopic,
		"queueType": wb.QueueType,
	}, "WORKER_SUBSCRIBED_TO_SIGNAL", wb.QueueType)
}

// Run is a generic polling loop that can be called by any worker type.
func (wb *WorkerBase) Run(process func(Task) error) {
	for {
		select {
		case <-wb.taskWakeupChan:
			logging.GlobalLogger.Log("debug", "System", "Worker", "Worker received task wake-up signal", map[string]any{
				"queueID": wb.Queue.QueueID,
			}, "WORKER_RECEIVED_TASK_WAKE_UP_SIGNAL", wb.QueueType)

			task := wb.Queue.PopTask()
			if task == nil {
				continue
			}

			wb.State = WorkerActive
			if err := process(task); err != nil {
				logging.GlobalLogger.Log("error", "System", "Worker", "Worker task failed", map[string]any{
					"taskID": task.GetID(),
					"error":  err.Error(),
				}, "WORKER_TASK_FAILED", wb.QueueType)
			} else {
				logging.GlobalLogger.Log("info", "System", "Worker", "Worker completed task", map[string]any{
					"taskID": task.GetID(),
				}, "WORKER_TASK_COMPLETED", wb.QueueType)
			}
			wb.Queue.RemoveInProgressTask(task.GetID())
			wb.State = WorkerIdle

		case <-wb.Queue.StopChan:
			logging.GlobalLogger.Log("info", "System", "Worker", "Worker received stop signal", map[string]any{
				"queueID": wb.Queue.QueueID,
			}, "WORKER_RECEIVED_STOP_SIGNAL", wb.QueueType)
			return
		}
	}
}
