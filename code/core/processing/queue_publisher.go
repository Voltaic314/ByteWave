package processing

import (
	"context"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
)

// -----------------------------------------------------------------------------
// Scan‑mode enum (first pass vs. retry pass)
// -----------------------------------------------------------------------------
type scanMode int

const (
	firstPass scanMode = iota // forward scan with path cursor
	retryPass                 // second sweep for failed rows
)

// PollingController manages soft polling state per queue.
type PollingController struct {
	IsPolling  bool
	CancelFunc context.CancelFunc
	Interval   time.Duration
	Mutex      sync.Mutex
}

// QueuePublisher manages multiple queues dynamically.
type QueuePublisher struct {
	DB                 *db.DB
	Queues             map[string]*TaskQueue
	QueueLevels        map[string]int
	QueueBoardChans    map[string]chan int
	PublishSignals     map[string]chan bool
	PhaseUpdated       chan int
	Mutex              sync.Mutex
	Running            bool
	LastPathCursors    map[string]string
	RetryThreshold     int
	BatchSize          int
	RunningLowChans    map[string]chan int
	PollingControllers map[string]*PollingController
	QueriesPerPhase map[string]int // queueName -> queryCount
	TraversalCompleteSignals map[string]chan string // queueName -> signal
	ScanModes map[string]scanMode // queueName -> scanMode
}

func NewQueuePublisher(db *db.DB, retryThreshold, batchSize int) *QueuePublisher {
	core.GlobalLogger.LogMessage("info", "Initializing new QueuePublisher", map[string]any{
		"retryThreshold": retryThreshold,
		"batchSize":      batchSize,
	})

	return &QueuePublisher{
		DB:                 db,
		Queues:             make(map[string]*TaskQueue),
		QueueLevels:        make(map[string]int),
		QueueBoardChans:    make(map[string]chan int),
		PublishSignals:     make(map[string]chan bool),
		PhaseUpdated:       make(chan int, 1),
		Running:            false,
		LastPathCursors:    make(map[string]string),
		RetryThreshold:     retryThreshold,
		BatchSize:          batchSize,
		RunningLowChans:    make(map[string]chan int),
		PollingControllers: make(map[string]*PollingController),
		QueriesPerPhase:    make(map[string]int), // queueName -> queryCount
		TraversalCompleteSignals: make(map[string]chan string),
		ScanModes:          make(map[string]scanMode),
	}
}

func (qp *QueuePublisher) StartListening() {
	core.GlobalLogger.LogMessage("info", "Starting QueuePublisher listening loop", nil)
	qp.Running = true

	for qp.Running {
		level := <-qp.PhaseUpdated

		core.GlobalLogger.LogMessage("info", "Phase updated", map[string]any{
			"newLevel": level,
		})

		for name := range qp.Queues {
			qp.QueueLevels[name] = level
			qp.ScanModes[name] = firstPass
			qp.PublishTasks(name)

			go func(queueName string, signalChan chan int) {
				for phase := range signalChan {
					qp.Mutex.Lock()
					queue, exists := qp.Queues[queueName]
					qp.Mutex.Unlock()

					if !exists || queue.State != QueueRunning {
						continue
					}

					qp.QueueLevels[queueName] = phase
					qp.ScanModes[queueName] = firstPass
					qp.PublishTasks(queueName)
					queue.ResetRunningLowTrigger()
				}
			}(name, qp.RunningLowChans[name])

			go func(queueName string) {
				for {
					time.Sleep(3 * time.Second)

					qp.Mutex.Lock()
					queue, exists := qp.Queues[queueName]
					qp.Mutex.Unlock()

					if !exists || queue.State != QueueRunning {
						return
					}

					if queue.QueueSize() < queue.RunningLowThreshold {
						qp.PublishTasks(queueName)
					}
				}
			}(name)
		}
	}
}

func (qp *QueuePublisher) PublishTasks(queueName string) {
	qp.Mutex.Lock()
	defer qp.Mutex.Unlock()

	queue, exists := qp.Queues[queueName]
	if !exists {
		core.GlobalLogger.LogMessage("error", "Queue not found", map[string]any{"queueName": queueName})
		return
	}

	// Determine the correct table
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	}

	mode := qp.ScanModes[queueName]
	currentLevel := qp.QueueLevels[queueName]

	var lastPath string
	if mode == firstPass {
		lastPath = qp.LastPathCursors[queueName]
	}


	tasks := qp.FetchTasksFromDB(table, queue.Type, currentLevel, lastPath, queueName)

	if len(tasks) > 0 {
		queue.AddTasks(tasks)
	
		if mode == firstPass {
			qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath()
		}
	} else {
		if qp.checkTraversalComplete(queueName, len(tasks)) {
			if ch, ok := qp.TraversalCompleteSignals[queueName]; ok {
				ch <- queueName
			}
			return
		}
	}

	core.GlobalLogger.LogMessage("info", "Tasks fetched successfully", map[string]any{
		"queueName": queueName,
		"taskCount": len(tasks),
		"scanMode":  mode,
	})

	if len(tasks) > 0 {
		queue.AddTasks(tasks)
		if mode == firstPass {
			qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath()
		}
	}

	// -------------------------------------------------------------------------
	// Scan‑mode / phase transitions
	// -------------------------------------------------------------------------
	if mode == firstPass && len(tasks) < qp.BatchSize {
		// Tail of first pass: flip to retry mode
		qp.ScanModes[queueName] = retryPass
		qp.LastPathCursors[queueName] = ""
		return
	}
	if mode == retryPass && len(tasks) == 0 {
		// No retries left → round done
		qp.ScanModes[queueName] = firstPass
		qp.LastPathCursors[queueName] = ""
		queue.Phase++
		qp.QueueLevels[queueName] = queue.Phase
		qp.QueriesPerPhase[queueName] = 0
		core.GlobalLogger.LogMessage("info", "Advancing to next phase", map[string]any{
			"queue": queueName,
			"phase": queue.Phase,
		})
		qp.PhaseUpdated <- queue.Phase
		return
	}

	// Trigger soft polling if batch was short
	if len(tasks) < qp.BatchSize {
		qp.startPolling(queueName)
	}
}

func (qp *QueuePublisher) isRoundComplete(queue *TaskQueue) bool {
	return queue.QueueSize() == 0 && queue.AreAllWorkersIdle() && queue.State == QueueRunning
}

func (qp *QueuePublisher) checkTraversalComplete(queueName string, queryResultSize int) bool {
	qp.Mutex.Lock()
	defer qp.Mutex.Unlock()
	_, exists := qp.Queues[queueName]
	if !exists {
		core.GlobalLogger.LogMessage("error", "Queue not found", map[string]any{
			"queueName": queueName,
		})
		return false
	}
	if queryResultSize == 0 && qp.QueriesPerPhase[queueName] == 0 {
		core.GlobalLogger.LogMessage("info", "Traversal complete — all phases exhausted", map[string]any{
			"queue": queueName,
		})
	
		return true
	}
	return false
}


func (qp *QueuePublisher) startPolling(queueName string) {
	core.GlobalLogger.LogMessage("info", "Starting polling for queue", map[string]any{
		"queueName": queueName,
	})

	controller, exists := qp.PollingControllers[queueName]
	if exists {
		controller.Mutex.Lock()
		if controller.IsPolling {
			controller.Mutex.Unlock()
			return
		}
		controller.Mutex.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	controller = &PollingController{
		IsPolling:  true,
		CancelFunc: cancel,
		Interval:   2 * time.Second,
	}
	controller.Mutex = sync.Mutex{}
	qp.PollingControllers[queueName] = controller

	go func() {
		ticker := time.NewTicker(controller.Interval)
		defer ticker.Stop()
		defer func() {
			controller.Mutex.Lock()
			controller.IsPolling = false
			controller.Mutex.Unlock()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				qp.Mutex.Lock()
				queue := qp.Queues[queueName]
				if qp.isRoundComplete(queue) {
					queue.Phase++
					qp.QueueLevels[queueName] = queue.Phase
					core.GlobalLogger.LogMessage("info", "Polling triggered phase bump", map[string]any{
						"queue": queueName,
						"phase": queue.Phase,
					})
					qp.PhaseUpdated <- queue.Phase
					qp.Mutex.Unlock()
					return
				}
				qp.Mutex.Unlock()

				qp.PublishTasks(queueName)
			}
		}
	}()
}

func (qp *QueuePublisher) FetchTasksFromDB(table string, queueType QueueType, currentLevel int, lastSeenPath string, queueName string) []*Task {
	core.GlobalLogger.LogMessage("info", "Fetching tasks from DB", map[string]any{
		"table":        table,
		"queueType":    queueType,
		"currentLevel": currentLevel,
		"lastSeenPath": lastSeenPath,
	})

	statusColumn := "traversal_status"
	retryColumn := "traversal_attempts"

	if queueType == UploadQueueType {
		statusColumn = "upload_status"
		retryColumn = "upload_attempts"
	}

	// Determine filtering type
	onlyFolders := queueType == TraversalQueueType
	onlyFiles := queueType == UploadQueueType

	query := `SELECT type, path, parent_id, last_modified, identifier
	          FROM ` + table + ` WHERE `
	var params []any

	if 	qp.ScanModes[queueName] == firstPass {
		query += `(` + statusColumn + ` = 'pending'
		           OR (` + statusColumn + ` = 'failed' AND ` + retryColumn + ` < ?))
		           AND level = ?`
		params = []any{qp.RetryThreshold, currentLevel}

		if lastSeenPath != "" {
			query += ` AND path > ?`
			params = append(params, lastSeenPath)
		}
	} else { // retryPass
		query += statusColumn + ` = 'failed'
		          AND ` + retryColumn + ` < ?
		          AND level = ?`
		params = []any{qp.RetryThreshold, currentLevel}
	}

	if onlyFolders {
		query += ` AND type = 'folder'`
	} else if onlyFiles {
		query += ` AND type = 'file'`
	}

	query += ` ORDER BY path LIMIT ?`
	params = append(params, qp.BatchSize)

	return qp.runTaskQuery(table, query, params, currentLevel, queueName)
}

// runTaskQuery executes the query and returns a list of tasks.
func (qp *QueuePublisher) runTaskQuery(table, query string, params []any, currentLevel int, queueName string) []*Task {
	rows, err := qp.DB.Query(table, query, params...)
	qp.QueriesPerPhase[queueName]++
	if err != nil {
		core.GlobalLogger.LogMessage("error", "DB query failed", map[string]any{
			"table": table,
			"err":   err.Error(),
		})
		return nil
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		var nodeType, path, parentID, lastModified, identifier string
		if err := rows.Scan(&nodeType, &path, &parentID, &lastModified, &identifier); err != nil {
			continue
		}

		if nodeType == "folder" {
			folder := &filesystem.Folder{
				Name:         path,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        currentLevel,
			}

			task, err := NewTraversalTask(path, folder, &parentID)
			if err == nil {
				tasks = append(tasks, task)
			}
		} else if nodeType == "file" {
			file := &filesystem.File{
				Name:         path,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Size:         0,
				Level:        currentLevel,
			}

			task, err := NewUploadTask(path, file, nil, &parentID)
			if err == nil {
				tasks = append(tasks, task)
			}
		}
	}
	return tasks
}

// StopListening stops QP operations.
func (qp *QueuePublisher) StopListening() {
	core.GlobalLogger.LogMessage("info", "Stopping QueuePublisher listening loop", nil)
	qp.Running = false
}
