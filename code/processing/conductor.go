package processing

import (
	"fmt"
	"time"

	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/pv"
	"github.com/Voltaic314/ByteWave/code/services"
	"github.com/Voltaic314/ByteWave/code/signals"
)

// The Boss 😎 - Responsible for setting up QP, queues, and workers
type Conductor struct {
	DB             *db.DB
	QP             *QueuePublisher
	retryThreshold int
	batchSize      int
}

// NewConductor initializes with DB config, but defers QP setup
func NewConductor(dbPath string, retryThreshold, batchSize int) *Conductor {
	dbInstance, err := db.NewDB(dbPath) // Creates DB connection
	if err != nil {
		logging.GlobalLogger.LogMessage("error", "Failed to initialize DB", map[string]any{
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

	// Setup source queue
	srcQueueName := "src-traversal"
	c.SetupQueue(srcQueueName, TraversalQueueType, 0, "src", 1000)

	// Setup destination queue (but don't kickstart it yet)
	dstQueueName := "dst-traversal"
	c.SetupQueue(dstQueueName, TraversalQueueType, 0, "dst", 1000)

	var os_svc services.BaseServiceInterface = services.NewOSService()
	pv_obj := pv.NewPathValidator()

	// Create workers for source queue
	num_of_workers := 10
	for range num_of_workers {
		tw := &TraverserWorker{
			WorkerBase: c.AddWorker(srcQueueName, "src"),
			DB:         c.DB,
			Service:    os_svc,
			pv:         pv_obj,
			QP:         c.QP,
		}
		go tw.Run(tw.ProcessTraversalTask)
	}

	// Create workers for destination queue
	for range num_of_workers {
		tw := &TraverserWorker{
			WorkerBase: c.AddWorker(dstQueueName, "dst"),
			DB:         c.DB,
			Service:    os_svc,
			pv:         pv_obj,
			QP:         c.QP,
		}
		go tw.Run(tw.ProcessTraversalTask)
	}

	// Start QP listening loop
	c.StartAll()

	time.Sleep(25 * time.Millisecond) // Give QP a moment to initialize

	// kick start the first phase manually for source only
	signals.GlobalSR.Publish(signals.Signal{
		Topic:   "qp:phase_updated:src-traversal",
		Payload: 0,
	})

	// Listen for traversal complete signals
	// Source traversal completion
	signals.GlobalSR.On("qp:traversal_complete:src-traversal", func(sig signals.Signal) {
		queueName, ok := sig.Payload.(string)
		if !ok {
			logging.GlobalLogger.LogSystem("error", "Conductor", "Invalid traversal.complete payload", map[string]any{
				"payload_type": fmt.Sprintf("%T", sig.Payload),
			})
			return
		}

		logging.GlobalLogger.LogSystem("info", "Conductor", "Source traversal complete", map[string]any{
			"queue": queueName,
		})

		c.TeardownQueue(queueName)
	})

	// Destination traversal completion
	signals.GlobalSR.On("qp:traversal_complete:dst-traversal", func(sig signals.Signal) {
		queueName, ok := sig.Payload.(string)
		if !ok {
			logging.GlobalLogger.LogSystem("error", "Conductor", "Invalid traversal.complete payload", map[string]any{
				"payload_type": fmt.Sprintf("%T", sig.Payload),
			})
			return
		}

		logging.GlobalLogger.LogSystem("info", "Conductor", "Destination traversal complete", map[string]any{
			"queue": queueName,
		})

		c.TeardownQueue(queueName)
	})
}

// SetupQueue initializes a queue and registers it with QP
func (c *Conductor) SetupQueue(name string, queueType QueueType, phase int, srcOrDst string, paginationSize int) {
	RunningLowThreshold := 100
	queue := NewTaskQueue(queueType, phase, srcOrDst, paginationSize, RunningLowThreshold)

	c.QP.Queues[name] = queue
	c.QP.QueueLevels[name] = phase
	c.QP.LastPathCursors[name] = "" // 🆕 Add path-based cursor for this queue
}

// AddWorker assigns a new worker to a queue
func (c *Conductor) AddWorker(queueName string, queueType string) *WorkerBase {
	queue, exists := c.QP.Queues[queueName]
	if !exists {
		logging.GlobalLogger.LogMessage("error", "Queue not found", map[string]any{
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

	// Close StopChan to signal goroutines to stop
	close(queue.StopChan)

	// SignalRouter handles cleanup for signal-based communication

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
	delete(c.QP.LastPathCursors, queueName)
	delete(c.QP.ScanModes, queueName)
	delete(c.QP.QueriesPerPhase, queueName)

	logging.GlobalLogger.LogMessage("info", "Queue teardown complete", map[string]any{
		"queueID": queueName,
	})
}

// StartAll starts QP and any other necessary processes
func (c *Conductor) StartAll() {
	go c.QP.StartListening()
}
