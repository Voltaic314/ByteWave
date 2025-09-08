package processing

import (
	"time"

	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/pv"
	"github.com/Voltaic314/ByteWave/code/services"
	"github.com/Voltaic314/ByteWave/code/signals"
	typesdb "github.com/Voltaic314/ByteWave/code/types/db"
)

// TODO: This stuff about thresholds should be done on a queue level,
// not at the conductor level. We should update this when we get into 
// more dynamic queue setups. For our little tests it's fine though.
// The Boss 😎 - Responsible for setting up QP, queues, and workers
type Conductor struct {
	DB             *db.DB
	QP             *QueuePublisher
	runningLowThreshold int
	retryThreshold int
	batchSize      int
}

// NewConductor initializes with DB config, but defers QP setup
func NewConductor(dbPath string, runningLowThreshold, retryThreshold, batchSize int) *Conductor {
	dbInstance, err := db.NewDB(dbPath) // Creates DB connection
	if err != nil {
		logging.GlobalLogger.Log("error", "System", "Conductor", "Failed to initialize DB", map[string]any{
			"error": err.Error(),
		}, "CREATE_DB", "All")
		return nil
	}
	return &Conductor{
		DB:             dbInstance,
		retryThreshold: retryThreshold,
		batchSize:      batchSize,
	}
}

// StartTraversal initializes QP, sets up queues/workers, and starts the system
func (c *Conductor) StartTraversal(num_of_workers ...int) {
	// Initialize QP after DB and other components are ready
	c.QP = NewQueuePublisher(c.DB, c.retryThreshold, c.batchSize)
	c.QP.SetConductor(c) // Set conductor reference for dynamic queue creation

	// Setup source queue only - destination queue will be created when source reaches level 1
	srcQueueName := "src-traversal"
	c.SetupQueue(srcQueueName, TraversalQueueType, 0, "src", 1000, c.runningLowThreshold, c.retryThreshold)

	// Create workers FIRST before starting QP to avoid startup race condition
	var os_svc services.BaseServiceInterface = services.NewOSService()
	pv_obj := pv.NewPathValidator()
	nWorkers := 1
	if len(num_of_workers) > 0 {
		nWorkers = num_of_workers[0]
	}

	// Create source workers only - destination workers will be created when dst queue is set up
	for i := 0; i < nWorkers; i++ {
		tw := &TraverserWorker{
			WorkerBase: c.AddWorker(srcQueueName, "src"),
			DB:         c.DB,
			Service:    os_svc,
			pv:         pv_obj,
		}
		go tw.Run(tw.ProcessTraversalTask)
	}

	// Small delay to let workers settle into their WaitForWork state
	time.Sleep(50 * time.Millisecond)

	// Now start QP main control loop after workers are ready
	go c.QP.Run()

	// Start monitoring for queue completion
	go c.monitorQueueCompletion()
}

// SetupDestinationQueue creates the destination queue and workers when source reaches level 1
func (c *Conductor) SetupDestinationQueue(num_of_workers ...int) {
	dstQueueName := "dst-traversal"

	// Create destination queue
	c.SetupQueue(dstQueueName, TraversalQueueType, 0, "dst", 1000, c.runningLowThreshold, c.retryThreshold)

	// Create destination workers
	var os_svc services.BaseServiceInterface = services.NewOSService()
	pv_obj := pv.NewPathValidator()
	nWorkers := 1
	if len(num_of_workers) > 0 {
		nWorkers = num_of_workers[0]
	}

	for range nWorkers {
		tw := &TraverserWorker{
			WorkerBase: c.AddWorker(dstQueueName, "dst"),
			DB:         c.DB,
			Service:    os_svc,
			pv:         pv_obj, // TODO: dst doesn't need pv, this should be removed when we build out the actual pv logic.
		}
		go tw.Run(tw.ProcessTraversalTask)
	}

	logging.GlobalLogger.Log("info", "System", "Conductor", "Destination queue and workers created", map[string]any{
		"queue":   dstQueueName,
		"workers": num_of_workers,
	}, "CREATE_QUEUE", "dst")
}

// monitorQueueCompletion polls for queue completion instead of using signals
func (c *Conductor) monitorQueueCompletion() {
	for {
		// Check if QP exists
		if c.QP == nil {
			break
		}

		// Poll for completed queues
		c.QP.Mutex.Lock()
		completedQueues := make([]string, 0)
		for queueName, queue := range c.QP.Queues {
			if queue.State == QueueComplete {
				completedQueues = append(completedQueues, queueName)
			}
		}
		c.QP.Mutex.Unlock()

		queueAcronyms := map[string]string{
			"src-traversal": "src",
			"dst-traversal": "dst",
		}

		// Teardown completed queues
		for _, queueName := range completedQueues {
			logging.GlobalLogger.Log("info", "System", "Conductor", "Queue completion detected", map[string]any{
				"queue": queueName,
			}, "CHECK_QUEUE_COMPLETION", queueAcronyms[queueName])
			c.TeardownQueue(queueName)
		}

		// Stop monitoring if no queues remain
		c.QP.Mutex.Lock()
		queueCount := len(c.QP.Queues)
		c.QP.Mutex.Unlock()

		if queueCount == 0 {
			logging.GlobalLogger.Log("info", "System", "Conductor", "All queues completed, stopping monitor", map[string]any{},
				"LOG_COMPLETION", "All")
			c.QP.Mutex.Lock()
			c.QP.Running = false
			c.QP.Mutex.Unlock()
			break
		}

		time.Sleep(200 * time.Millisecond) // Poll every 200ms
	}
}

// SetupQueue initializes a queue and registers it with QP
func (c *Conductor) SetupQueue(name string, queueType QueueType, phase int, srcOrDst string, paginationSize int, runningLowThreshold int, retryThreshold int) {
	queue := NewTaskQueue(queueType, phase, srcOrDst, paginationSize, runningLowThreshold, retryThreshold)

	c.QP.Queues[name] = queue

	// Only set initial queue level for source queues
	// Destination queues should start when triggered by source progress
	if srcOrDst == "src" {
		c.QP.QueueLevels[name] = phase
	}

	c.QP.LastPathCursors[name] = "" // Add path-based cursor for this queue

	var tableName string
	switch srcOrDst {
	case "src":
		tableName = "source_nodes"
	default:
		tableName = "destination_nodes"
	}

	// init a new write queue for batch ops
	c.DB.InitWriteQueue(tableName, typesdb.NodeWriteQueue, 100, 5*time.Second)
}

// AddWorker assigns a new worker to a queue
func (c *Conductor) AddWorker(queueName string, queueType string) *WorkerBase {

	queue, exists := c.QP.Queues[queueName]
	if !exists {
		subtopic := logging.QueueAcronyms[queueName]
		if subtopic == "" {
			subtopic = queueName // fallback if not mapped
		}
		logging.GlobalLogger.Log(
			"error",                                // level
			"System",                               // entity
			"Conductor",                            // entityID
			"Queue not found",                      // message
			map[string]any{"queueName": queueName}, // details
			"ADD_WORKER",                           // action
			logging.QueueAcronyms[queueName],       // queue
		)
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
	c.QP.Mutex.Lock()
	queue, exists := c.QP.Queues[queueName]
	c.QP.Mutex.Unlock()

	if !exists {
		return
	}

	// Send stop signal via Signal Router to all workers 🛑
	stopTopic := "stop:" + queue.QueueID
	signals.GlobalSR.Publish(signals.Signal{
		Topic:   stopTopic,
		Payload: "teardown",
	})

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
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	} else if queue.SrcOrDst == "dst" {
		table = "destination_nodes"
	}
	queue.mu.Unlock()

	// Delete from all QP maps

	// 🔒 Locking...
	c.QP.Mutex.Lock()

	// but before we do that...
	// Flush any remaining data out from the corresponding write queues.
	c.QP.DB.ForceFlushTable("audit_log") // justttttt to be safe. :)
	c.QP.DB.ForceFlushTable(table)

	// Okay now delete the stuff! \o/
	delete(c.QP.Queues, queueName)
	delete(c.QP.QueueLevels, queueName)
	delete(c.QP.LastPathCursors, queueName)
	delete(c.QP.QueriesPerPhase, queueName)

	// 🔓 Unlocking...
	c.QP.Mutex.Unlock()

}
