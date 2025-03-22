package processing

import (
	"github.com/Voltaic314/ByteWave/code/core/db"
)

// The Boss 😎 - Responsible for setting up QP, queues, and workers
type Conductor struct {
	DB           *db.DB
	QP           *QueuePublisher
}

// NewConductor initializes and wires everything up
func NewConductor(db *db.DB, retryThreshold, batchSize int) *Conductor {
	conductor := &Conductor{
		DB:          db,
		QP:          NewQueuePublisher(db, retryThreshold, batchSize),
	}

	return conductor
}

// SetupQueue initializes a queue and registers it with QP
func (c *Conductor) SetupQueue(name string, queueType QueueType, phase int, srcOrDst string, paginationSize int) {
	// Create signal channels for QP communication
	runningLowChan := make(chan int, 1) // Unique channel for RunningLow signal
	c.QP.RunningLowChans[name] = runningLowChan

	// Initialize queue board with its channels
	queue := NewTaskQueue(queueType, phase, srcOrDst, paginationSize, runningLowChan)
	c.QP.Queues[name] = queue
	c.QP.QueueLevels[name] = phase
	c.QP.QueueBoardChans[name] = make(chan int, 1)
	c.QP.PublishSignals[name] = make(chan bool, 1)
}

// AddWorker assigns a new worker to a queue
func (c *Conductor) AddWorker(queueName string, workerID string) {
	queue, exists := c.QP.Queues[queueName]
	if !exists {
		return // Queue doesn't exist
	}

	worker := &Worker{ID: workerID, State: WorkerIdle}
	queue.mu.Lock()
	queue.workers = append(queue.workers, worker)
	queue.mu.Unlock()
}

// RemoveWorker removes a worker from a queue
func (c *Conductor) RemoveWorker(queueName string, workerID string) {
	queue, exists := c.QP.Queues[queueName]
	if !exists {
		return
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	for i, worker := range queue.workers {
		if worker.ID == workerID {
			queue.workers = append(queue.workers[:i], queue.workers[i+1:]...)
			return
		}
	}
}

// SetWorkerState updates a worker's state
func (c *Conductor) SetWorkerState(queueName string, workerID string, state WorkerState) {
	queue, exists := c.QP.Queues[queueName]
	if !exists {
		return
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, worker := range queue.workers {
		if worker.ID == workerID {
			worker.State = state
			return
		}
	}
}

// StartAll starts QP and any other necessary processes
func (c *Conductor) StartAll() {
	c.QP.StartListening()
}
