package processing

import (
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/pv"
	"github.com/Voltaic314/ByteWave/code/core/services"
)

// The Boss 😎 - Responsible for setting up QP, queues, and workers
type Conductor struct {
	DB           *db.DB
	QP           *QueuePublisher
	retryThreshold int
	batchSize      int 
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
	name := "src-traversal"
	c.SetupQueue(name, TraversalQueueType, 0, "src", 100)
	c.QP.TraversalCompleteSignals[name] = make(chan string, 1)

	c.DB.InitWriteQueueTable("source_nodes", 10, 5*time.Second)

	var os_svc services.BaseServiceInterface = services.NewOSService()
	pv_obj := pv.NewPathValidator()

	// Add workers (example: 1 worker)
	for range 1 {
		tw := &TraverserWorker{
			WorkerBase: c.AddWorker("src-traversal", "src"),
			DB:         c.DB,
			Service:    os_svc, // Or whatever service you need
			pv:         pv_obj, // Or load actual rules if available
		}
		// register worker to the queue
		
		go tw.Run(tw.ProcessTraversalTask)
	}

	// Start QP listening loop
	c.StartAll()

	time.Sleep(100 * time.Millisecond) // Give QP a moment to initialize

	// kick start the first phase manually
	c.QP.PhaseUpdated <- 0

	// Listen for traversal complete signals
	// This is a separate goroutine to handle traversal completion signals
	go func() {
		signalChan, exists := c.QP.TraversalCompleteSignals["src-traversal"]
		if !exists {
			core.GlobalLogger.LogMessage("error", "No signal channel found for traversal complete", nil)
			return
		}
	
		for queueName := range signalChan {
			core.GlobalLogger.LogMessage("info", "Conductor received traversal complete signal", map[string]any{
				"queue": queueName,
			})
			c.TeardownQueue(queueName)
		}
	}()
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
		core.GlobalLogger.LogMessage("error", "Queue not found", map[string]any{
			"queueName": queueName,
		})
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

func (c *Conductor) TeardownQueue(queueName string) {
	queue, exists := c.QP.Queues[queueName]
	if !exists {
		return
	}

	// Stop polling if active
	if controller, ok := c.QP.PollingControllers[queueName]; ok {
		controller.Mutex.Lock()
		if controller.IsPolling {
			controller.CancelFunc()
		}
		controller.Mutex.Unlock()
		delete(c.QP.PollingControllers, queueName)
	}

	// Remove idle workers
	queue.mu.Lock()
	idleWorkers := queue.workers[:0]
	for _, w := range queue.workers {
		if w.State == WorkerIdle {
			continue // skip deleting active ones just in case
		}
		idleWorkers = append(idleWorkers, w)
	}
	queue.workers = idleWorkers
	queue.cond.Broadcast() // Notify all workers to stop waiting
	queue.mu.Unlock()

	// Delete from all QP maps
	delete(c.QP.Queues, queueName)
	delete(c.QP.QueueLevels, queueName)
	delete(c.QP.QueueBoardChans, queueName)
	delete(c.QP.PublishSignals, queueName)
	delete(c.QP.LastPathCursors, queueName)
	delete(c.QP.RunningLowChans, queueName)
	delete(c.QP.ScanModes, queueName)
	delete(c.QP.QueriesPerPhase, queueName)

	core.GlobalLogger.LogMessage("info", "Queue teardown complete", map[string]any{
		"queueID": queueName,
	})
}

// StartAll starts QP and any other necessary processes
func (c *Conductor) StartAll() {
	go c.QP.StartListening()
}
