package processing

import (
	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
)

// The Boss 😎 - Responsible for setting up QP, queues, and workers
type Conductor struct {
	DB           *db.DB
	QP           *QueuePublisher
	retryThreshold int
	batchSize      int 
	logger       *core.Logger
}

// NewConductor initializes with DB config, but defers QP setup
func NewConductor(dbPath string, retryThreshold, batchSize int) *Conductor {
	dbInstance, err := db.NewDB(dbPath) // Creates DB connection
	if err != nil {
		core.GlobalLogger.LogMessage("error", "Failed to initialize DB", map[string]any{
			"error": err.Error(),
		})
		return nil
	}
	return &Conductor{
		DB:             dbInstance,
		retryThreshold: retryThreshold,
		batchSize:      batchSize,
	}
}

// StartTraversal initializes QP, sets up queues/workers, and starts the system
func (c *Conductor) StartTraversal() {
	// Initialize QP after DB and other components are ready
	c.QP = NewQueuePublisher(c.DB, c.retryThreshold, c.batchSize)

	// Setup queues
	c.SetupQueue("src-traversal", TraversalQueueType, 0, "src", 100)

	// Add workers (example: 1 worker)
	for i := 0; i < 1; i++ {
		worker := c.AddWorker("src-traversal", "src")
		go worker.RunMainLoop(func() bool {
			// Worker logic placeholder (replace with real task handler)
			return false
		})
	}

	// Start QP listening loop
	c.StartAll()
}

// SetupQueue initializes a queue and registers it with QP
func (c *Conductor) SetupQueue(name string, queueType QueueType, phase int, srcOrDst string, paginationSize int) {
	runningLowChan := make(chan int, 1)
	RunningLowThreshold := 100
	queue := NewTaskQueue(queueType, phase, srcOrDst, paginationSize, runningLowChan, RunningLowThreshold)

	c.QP.Queues[name] = queue
	c.QP.QueueLevels[name] = phase
	c.QP.QueueBoardChans[name] = make(chan int, 1)
	c.QP.PublishSignals[name] = make(chan bool, 1)
	c.QP.RunningLowChans[name] = runningLowChan
	c.QP.LastPathCursors[name] = "" // 🆕 Add path-based cursor for this queue
}

// AddWorker assigns a new worker to a queue
func (c *Conductor) AddWorker(queueName string, queueType string) *WorkerBase {
	queue, exists := c.QP.Queues[queueName]
	if !exists {
		return nil
	}

	worker := NewWorkerBase(queue, queueType)

	queue.mu.Lock()
	queue.workers = append(queue.workers, worker)
	queue.mu.Unlock()

	return worker
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
