package processing

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
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
	DB                       *db.DB
	Queues                   map[string]*TaskQueue
	QueueLevels              map[string]int
	QueueBoardChans          map[string]chan int
	PublishSignals           map[string]chan bool
	PhaseUpdated             chan int
	Mutex                    sync.Mutex
	Running                  bool
	LastPathCursors          map[string]string
	RetryThreshold           int
	BatchSize                int
	RunningLowChans          map[string]chan int
	PollingControllers       map[string]*PollingController
	QueriesPerPhase          map[string]int         // queueName -> queryCount
	TraversalCompleteSignals map[string]chan string // queueName -> signal
	ScanModes                map[string]scanMode    // queueName -> scanMode
}

func NewQueuePublisher(db *db.DB, retryThreshold, batchSize int) *QueuePublisher {
	logging.GlobalLogger.LogSystem("info", "QP", "Initializing new QueuePublisher", map[string]any{
		"retryThreshold": retryThreshold,
		"batchSize":      batchSize,
	})

	return &QueuePublisher{
		DB:                       db,
		Queues:                   make(map[string]*TaskQueue),
		QueueLevels:              make(map[string]int),
		QueueBoardChans:          make(map[string]chan int),
		PublishSignals:           make(map[string]chan bool),
		PhaseUpdated:             make(chan int, 1),
		Running:                  false,
		LastPathCursors:          make(map[string]string),
		RetryThreshold:           retryThreshold,
		BatchSize:                batchSize,
		RunningLowChans:          make(map[string]chan int),
		PollingControllers:       make(map[string]*PollingController),
		QueriesPerPhase:          make(map[string]int), // queueName -> queryCount
		TraversalCompleteSignals: make(map[string]chan string),
		ScanModes:                make(map[string]scanMode),
	}
}

func (qp *QueuePublisher) FlushTable(table string) {
	qp.Mutex.Lock()
	defer qp.Mutex.Unlock()

	wq := qp.DB.GetWriteQueue(table)

	if wq != nil {
		logging.GlobalLogger.LogSystem("debug", "QP", "Flushing write queue", map[string]any{
			"table": table,
		})

		batches := wq.Flush(true)
		if len(batches) > 0 {
			logging.GlobalLogger.LogSystem("debug", "QP", "Executing batch commands", map[string]any{
				"table":      table,
				"batchCount": len(batches),
			})
			qp.DB.ExecuteBatchCommands(batches)
		} else {
			logging.GlobalLogger.LogSystem("debug", "QP", "No batches to flush", map[string]any{
				"table": table,
			})
		}
	} else {
		logging.GlobalLogger.LogMessage("error", "WriteQueue not found", map[string]any{
			"table": table,
		})
	}
}

func (qp *QueuePublisher) StartListening() {
	logging.GlobalLogger.LogSystem("info", "QP", "Starting QueuePublisher listening loop", nil)
	qp.Running = true

	for qp.Running {
		level := <-qp.PhaseUpdated

		logging.GlobalLogger.LogSystem("info", "QP", "Phase updated", map[string]any{
			"newLevel": level,
		})

		for name := range qp.Queues {
			qp.QueueLevels[name] = level
			qp.ScanModes[name] = firstPass
			qp.PublishTasks(name)

			qp.Mutex.Lock()
			signalChan := qp.RunningLowChans[name]
			qp.Mutex.Unlock()

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
			}(name, signalChan)

			// 🆕 NEW: Listen for idle signals from workers
			qp.Mutex.Lock()
			idleChan := qp.Queues[name].IdleChan
			qp.Mutex.Unlock()

			go func(queueName string, idleChan chan int) {
				for phase := range idleChan {
					qp.Mutex.Lock()
					queue, exists := qp.Queues[queueName]
					qp.Mutex.Unlock()

					if !exists || queue.State != QueueRunning {
						continue
					}

					logging.GlobalLogger.LogQP("info", queueName, "", "Received idle signal from worker", map[string]any{
						"phase": phase,
					})

					// Check if we need to publish more tasks
					if queue.QueueSize() == 0 {
						qp.PublishTasks(queueName)
					}
					queue.ResetIdleTrigger()
				}
			}(name, idleChan)

			go func(queueName string) {
				for {
					time.Sleep(3 * time.Second)

					qp.Mutex.Lock()
					queue, exists := qp.Queues[queueName]
					qp.Mutex.Unlock()

					if !exists || queue.State != QueueRunning {
						return
					}

					// Check if we should stop
					select {
					case <-queue.StopChan:
						return
					default:
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
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()
	if !exists {
		logging.GlobalLogger.LogQP("error", queueName, "", "Queue not found", nil)
		return
	}

	// Determine the correct table
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	}

	qp.Mutex.Lock()
	mode := qp.ScanModes[queueName]
	currentLevel := qp.QueueLevels[queueName]

	var lastPath string
	if mode == firstPass {
		lastPath = qp.LastPathCursors[queueName]
	}
	qp.Mutex.Unlock()

	// force a flush in the WQ before trying to fetch anything
	qp.FlushTable(table)

	tasks := qp.FetchTasksFromDB(table, queue.Type, currentLevel, lastPath, queueName)

	if len(tasks) > 0 {
		logging.GlobalLogger.LogQP("debug", queueName, "", "Tasks fetched from DB", map[string]any{
			"taskCount":    len(tasks),
			"scanMode":     mode,
			"level":        currentLevel,
			"lastSeenPath": lastPath,
		})
	} else {
		logging.GlobalLogger.LogQP("debug", queueName, "", "No tasks fetched from DB", map[string]any{
			"scanMode":     mode,
			"level":        currentLevel,
			"lastSeenPath": lastPath,
		})
	}

	if len(tasks) <= 0 {
		if qp.checkTraversalComplete(queueName, len(tasks)) {
			if ch, ok := qp.TraversalCompleteSignals[queueName]; ok {
				ch <- queueName
			}
			return
		}
	}

	if len(tasks) > 0 {
		queue.AddTasks(tasks)
		// 🆕 NEW: Reset idle trigger when new tasks are added
		queue.ResetIdleTrigger()
		qp.QueriesPerPhase[queueName]++
		if mode == firstPass {
			qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath()
		}
	}

	// Switch to retry mode if the first pass ends
	if mode == firstPass && len(tasks) < qp.BatchSize {
		qp.ScanModes[queueName] = retryPass
	}

	// Start polling if short or empty batch
	if len(tasks) < qp.BatchSize {
		qp.startPolling(queueName)
	}
}

func (qp *QueuePublisher) isRoundComplete(queue *TaskQueue) bool {
	logging.GlobalLogger.LogMessage("info", "Checking round completion", map[string]any{
		"queueID":        queue.QueueID,
		"queueSize":      queue.QueueSize(),
		"allWorkersIdle": queue.AreAllWorkersIdle(),
		"state":          queue.State,
	})
	return queue.QueueSize() == 0 && queue.AreAllWorkersIdle() && queue.State == QueueRunning
}

func (qp *QueuePublisher) advancePhase(queueName string) {
	queue := qp.Queues[queueName]
	queue.Phase++
	qp.QueueLevels[queueName] = queue.Phase
	qp.LastPathCursors[queueName] = ""
	qp.ScanModes[queueName] = firstPass
	qp.QueriesPerPhase[queueName] = 0

	logging.GlobalLogger.LogMessage("info", "Advancing to next phase", map[string]any{
		"queue": queueName,
		"phase": queue.Phase,
	})

	qp.PhaseUpdated <- queue.Phase
}

func (qp *QueuePublisher) checkTraversalComplete(queueName string, queryResultSize int) bool {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	if !exists {
		qp.Mutex.Unlock()
		logging.GlobalLogger.LogMessage("error", "Queue not found", map[string]any{
			"queueName": queueName,
		})
		return false
	}
	currentLevel := qp.QueueLevels[queueName]
	qp.Mutex.Unlock()

	// log the result size and the queries per phase for debug purposes
	logging.GlobalLogger.LogMessage("info", "Checking traversal completion", map[string]any{
		"queue":           queueName,
		"queryResultSize": queryResultSize,
		"queriesPerPhase": qp.QueriesPerPhase[queueName],
		"currentLevel":    currentLevel,
	})

	// Only check for completion if we're not at level 0 and got no results
	if queryResultSize == 0 && currentLevel != 0 {
		// Check if there are any pending tasks at the current level in the database
		table := "source_nodes"
		if queue.Type == UploadQueueType {
			table = "destination_nodes"
		}

		statusColumn := "traversal_status"
		if queue.Type == UploadQueueType {
			statusColumn = "upload_status"
		}

		// Force flush the write queue to ensure all pending inserts are committed
		qp.FlushTable(table)

		// Query to check if there are any pending tasks at current level OR higher levels
		checkQuery := `SELECT COUNT(*) FROM ` + table + ` WHERE ` + statusColumn + ` = 'pending' AND level >= ?`
		rows, err := qp.DB.Query(table, checkQuery, currentLevel)
		if err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to check pending tasks count", map[string]any{
				"queue": queueName,
				"error": err.Error(),
			})
			return false
		}
		defer rows.Close()

		var count int
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				logging.GlobalLogger.LogMessage("error", "Failed to scan pending tasks count", map[string]any{
					"queue": queueName,
					"error": err.Error(),
				})
				return false
			}
		}

		logging.GlobalLogger.LogMessage("info", "Pending tasks check", map[string]any{
			"queue":        queueName,
			"currentLevel": currentLevel,
			"pendingCount": count,
		})

		// Only mark complete if there are no pending tasks at current level OR higher
		if count == 0 {
			logging.GlobalLogger.LogMessage("info", "Traversal complete — no pending tasks at current level or higher", map[string]any{
				"queue":        queueName,
				"currentLevel": currentLevel,
			})
			return true
		}
	}
	return false
}

func (qp *QueuePublisher) startPolling(queueName string) {

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
		Interval:   100 * time.Millisecond,
	}
	controller.Mutex = sync.Mutex{}
	qp.PollingControllers[queueName] = controller

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.GlobalLogger.LogMessage("error", "Recovered from panic in polling goroutine", map[string]any{
					"queue": queueName,
					"error": r,
				})
			}
			controller.Mutex.Lock()
			controller.IsPolling = false
			controller.Mutex.Unlock()
		}()

		ticker := time.NewTicker(controller.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

				queue, exists := qp.Queues[queueName]
				if !exists || queue.State != QueueRunning {
					return
				}

				shouldAdvance := false
				if qp.isRoundComplete(queue) {
					// First advance to the next phase
					qp.advancePhase(queueName)

					// Then check if the new phase has no tasks (traversal complete)
					if qp.checkTraversalComplete(queueName, 0) {
						// Flush the correct table, not the queue name
						table := "source_nodes"
						if queue.Type == UploadQueueType {
							table = "destination_nodes"
						}
						qp.FlushTable(table)

						// Send completion signal AFTER flushing
						if ch, ok := qp.TraversalCompleteSignals[queueName]; ok {
							ch <- queueName
						}

						// Exit immediately to prevent further operations
						return
					}
					return
				}

				if shouldAdvance {
					qp.advancePhase(queueName)
					return
				}
				qp.PublishTasks(queueName)
			}
		}
	}()
}

func (qp *QueuePublisher) FetchTasksFromDB(table string, queueType QueueType, currentLevel int, lastSeenPath string, queueName string) []*Task {
	logging.GlobalLogger.LogMessage("info", "Fetching tasks from DB", map[string]any{
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

	// ✅ SELECT order now matches the DB schema and runTaskQuery's scan order
	query := `SELECT path, name, identifier, parent_id, type, last_modified
	          FROM ` + table + ` WHERE `

	var params []any

	qp.Mutex.Lock()
	if qp.ScanModes[queueName] == firstPass {
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

		// Add NOT IN filtering for retry mode to prevent re-processing active paths
		qp.Mutex.Unlock()
		queue, exists := qp.Queues[queueName]
		if exists {
			trackedPaths := queue.TrackedPaths(true) // Include both queued and in-progress paths
			if len(trackedPaths) > 0 {
				placeholders := make([]string, len(trackedPaths))
				pathList := make([]string, 0, len(trackedPaths))
				for path := range trackedPaths {
					pathList = append(pathList, path)
				}

				for i := range placeholders {
					placeholders[i] = "?"
				}
				query += ` AND path NOT IN (` + strings.Join(placeholders, ", ") + `)`

				// Add the tracked paths as parameters
				for _, path := range pathList {
					params = append(params, path)
				}

				logging.GlobalLogger.LogMessage("info", "Retry mode filtering applied", map[string]any{
					"queueName":     queueName,
					"excludedPaths": len(pathList),
				})
			}
		}
		qp.Mutex.Lock()
	}
	qp.Mutex.Unlock()

	if onlyFolders {
		query += ` AND type = 'folder'`
	} else if onlyFiles {
		query += ` AND type = 'file'`
	}

	query += ` ORDER BY path LIMIT ?`
	params = append(params, qp.BatchSize)

	// Debug log the query
	// logging.GlobalLogger.LogMessage("info", "Executing query", map[string]any{
	// 	"query":  query,
	// 	"params": params,
	// })

	return qp.runTaskQuery(table, query, params, currentLevel, queueName)
}

// runTaskQuery executes the query and returns a list of tasks.
func (qp *QueuePublisher) runTaskQuery(table, query string, params []any, currentLevel int, queueName string) []*Task {
	rows, err := qp.DB.Query(table, query, params...)
	if err != nil {
		logging.GlobalLogger.LogMessage("error", "DB query failed", map[string]any{
			"table": table,
			"err":   err.Error(),
		})
		return nil
	}
	qp.Mutex.Lock()
	qp.QueriesPerPhase[queueName]++
	qp.Mutex.Unlock()

	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		var path, name, identifier, parentID, nodeType, lastModified string
		if err := rows.Scan(&path, &name, &identifier, &parentID, &nodeType, &lastModified); err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to scan row", map[string]any{
				"err": err.Error(),
			})
			continue
		}

		normalizedPath := filepath.ToSlash(path)

		if nodeType == "folder" {
			folder := &filesystem.Folder{
				Name:         name,
				Path:         normalizedPath,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        currentLevel,
			}
			task, err := NewTraversalTask(normalizedPath, folder, &folder.ParentID)
			if err == nil {
				tasks = append(tasks, task)
			}
		}
	}

	return tasks
}

// StopListening stops QP operations.
func (qp *QueuePublisher) StopListening() {
	logging.GlobalLogger.LogMessage("info", "Stopping QueuePublisher listening loop", nil)
	qp.Running = false
}

// DebugPendingFolders is a helper method to investigate pending folders
func (qp *QueuePublisher) DebugPendingFolders(table string, paths []string) {
	if len(paths) == 0 {
		return
	}

	placeholders := make([]string, len(paths))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := `SELECT path, traversal_status, traversal_attempts, level, type FROM ` + table + ` WHERE path IN (` + strings.Join(placeholders, ", ") + `)`

	rows, err := qp.DB.Query(table, query, func() []any {
		params := make([]any, len(paths))
		for i, path := range paths {
			params[i] = path
		}
		return params
	}()...)

	if err != nil {
		logging.GlobalLogger.LogMessage("error", "Failed to debug pending folders", map[string]any{
			"error": err.Error(),
		})
		return
	}
	defer rows.Close()

	for rows.Next() {
		var path, status, nodeType string
		var attempts, level int
		if err := rows.Scan(&path, &status, &attempts, &level, &nodeType); err != nil {
			continue
		}

		logging.GlobalLogger.LogMessage("debug", "Pending folder details", map[string]any{
			"path":     path,
			"status":   status,
			"attempts": attempts,
			"level":    level,
			"type":     nodeType,
		})
	}
}
