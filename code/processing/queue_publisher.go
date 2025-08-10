package processing

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/signals"
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
	DB              *db.DB
	Queues          map[string]*TaskQueue
	QueueLevels     map[string]int
	Mutex           sync.Mutex
	Running         bool
	LastPathCursors map[string]string
	// LastDBPaths stores the last raw DB path fetched per queue for stable cursoring
	LastDBPaths        map[string]string
	RetryThreshold     int
	BatchSize          int
	PollingControllers map[string]*PollingController
	QueriesPerPhase    map[string]int      // queueName -> queryCount
	ScanModes          map[string]scanMode // queueName -> scanMode
	// HandlersInstalled ensures per-queue handlers/goroutines are registered once
	HandlersInstalled map[string]bool
	// RootPaths stores the root folder path for each queue (for path normalization)
	RootPaths map[string]string // queueName -> rootPath
}

func NewQueuePublisher(db *db.DB, retryThreshold, batchSize int) *QueuePublisher {
	logging.GlobalLogger.LogSystem("info", "QP", "Initializing new QueuePublisher", map[string]any{
		"retryThreshold": retryThreshold,
		"batchSize":      batchSize,
	})

	return &QueuePublisher{
		DB:                 db,
		Queues:             make(map[string]*TaskQueue),
		QueueLevels:        make(map[string]int),
		Running:            false,
		LastPathCursors:    make(map[string]string),
		LastDBPaths:        make(map[string]string),
		RetryThreshold:     retryThreshold,
		BatchSize:          batchSize,
		PollingControllers: make(map[string]*PollingController),
		QueriesPerPhase:    make(map[string]int), // queueName -> queryCount
		ScanModes:          make(map[string]scanMode),
		HandlersInstalled:  make(map[string]bool),
		RootPaths:          make(map[string]string), // queueName -> rootPath
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

	// Subscribe to queue-specific phase updates using SignalRouter
	signals.GlobalSR.On("qp:phase_updated:src-traversal", func(sig signals.Signal) {
		level, ok := sig.Payload.(int)
		if !ok {
			logging.GlobalLogger.LogSystem("error", "QP", "Invalid data type in phase_updated signal", map[string]any{
				"payload_type": fmt.Sprintf("%T", sig.Payload),
				"payload":      sig.Payload,
			})
			return
		}

		logging.GlobalLogger.LogSystem("info", "QP", "Source phase updated", map[string]any{
			"newLevel": level,
		})

		// Handle source queue
		srcQueueName := "src-traversal"
		qp.Mutex.Lock()
		qp.QueueLevels[srcQueueName] = level
		qp.ScanModes[srcQueueName] = firstPass
		qp.Mutex.Unlock()
		qp.PublishTasks(srcQueueName)

		// Check if destination should be started or advanced
		qp.checkDestinationStart(level)

		// Install per-queue handlers for source queue if not already installed
		qp.Mutex.Lock()
		needInstall := !qp.HandlersInstalled[srcQueueName]
		qp.Mutex.Unlock()
		if needInstall {
			qp.installQueueHandlers(srcQueueName)
			qp.Mutex.Lock()
			qp.HandlersInstalled[srcQueueName] = true
			qp.Mutex.Unlock()
		}
	})

	// Keep the main loop running
	for qp.Running {
		time.Sleep(100 * time.Millisecond)
	}
}

// installQueueHandlers sets up the running_low, idle, and refresh handlers for a queue
func (qp *QueuePublisher) installQueueHandlers(queueName string) {
	signals.GlobalSR.On("qp:running_low:"+queueName, func(sig signals.Signal) {
		phase, ok := sig.Payload.(int)
		if !ok {
			logging.GlobalLogger.LogSystem("error", "QP", "Invalid data for running_low signal", map[string]any{
				"queue": queueName, "data": sig.Payload,
			})
			return
		}

		qp.Mutex.Lock()
		queue, exists := qp.Queues[queueName]
		qp.Mutex.Unlock()

		if !exists || queue.State != QueueRunning {
			return
		}

		qp.Mutex.Lock()
		qp.QueueLevels[queueName] = phase
		qp.ScanModes[queueName] = firstPass
		qp.Mutex.Unlock()
		qp.PublishTasks(queueName)
		queue.ResetRunningLowTrigger()
	})

	// Listen for idle signals from workers
	signals.GlobalSR.On("qp:idle:"+queueName, func(sig signals.Signal) {
		phase, ok := sig.Payload.(int)
		if !ok {
			logging.GlobalLogger.LogSystem("error", "QP", "Invalid data for idle signal", map[string]any{
				"queue": queueName, "data": sig.Payload,
			})
			return
		}

		qp.Mutex.Lock()
		queue, exists := qp.Queues[queueName]
		qp.Mutex.Unlock()

		if !exists || queue.State != QueueRunning {
			return
		}

		logging.GlobalLogger.LogQP("info", queueName, "", "Received idle signal from worker", map[string]any{
			"phase": phase,
		})

		// Check if we need to publish more tasks
		if queue.QueueSize() == 0 {
			qp.PublishTasks(queueName)
		}
		queue.ResetIdleTrigger()
	})

	// Background refresh goroutine for this queue
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
	}(queueName)
}

// checkDestinationStart checks if destination traversal should be started or advanced
func (qp *QueuePublisher) checkDestinationStart(srcLevel int) {
	dstQueueName := "dst-traversal"

	qp.Mutex.Lock()
	dstQueue, dstExists := qp.Queues[dstQueueName]
	currentDstLevel, dstLevelExists := qp.QueueLevels[dstQueueName]
	qp.Mutex.Unlock()

	if !dstExists {
		return // Destination queue not set up yet
	}

	// Check if we should start destination (source is at level 2 or higher)
	if !dstLevelExists && srcLevel >= 2 {
		logging.GlobalLogger.LogSystem("info", "QP", "Starting destination traversal", map[string]any{
			"srcLevel": srcLevel,
		})

		// Initialize destination queue handlers if not already done
		qp.Mutex.Lock()
		needInstall := !qp.HandlersInstalled[dstQueueName]
		qp.Mutex.Unlock()
		if needInstall {
			qp.installQueueHandlers(dstQueueName)
			qp.Mutex.Lock()
			qp.HandlersInstalled[dstQueueName] = true
			qp.Mutex.Unlock()
		}

		// Start destination at level 0
		qp.Mutex.Lock()
		qp.QueueLevels[dstQueueName] = 0
		qp.ScanModes[dstQueueName] = firstPass
		qp.Mutex.Unlock()
		qp.PublishDestinationTasks(dstQueueName)
		return
	}

	// Check if destination can advance (source must be N+2 ahead)
	if dstLevelExists && srcLevel >= currentDstLevel+2 {
		logging.GlobalLogger.LogSystem("info", "QP", "Advancing destination phase", map[string]any{
			"srcLevel": srcLevel,
			"dstLevel": currentDstLevel,
		})

		// Check if current destination phase is complete before advancing
		if qp.isDstPhaseComplete(dstQueue, currentDstLevel) {
			newDstLevel := currentDstLevel + 1
			qp.Mutex.Lock()
			qp.QueueLevels[dstQueueName] = newDstLevel
			qp.ScanModes[dstQueueName] = firstPass
			qp.Mutex.Unlock()
			qp.PublishDestinationTasks(dstQueueName)
		}
	}
}

// isDstPhaseComplete checks if the current destination phase is complete
func (qp *QueuePublisher) isDstPhaseComplete(queue *TaskQueue, level int) bool {
	return queue.QueueSize() == 0 && queue.AreAllWorkersIdle() && queue.State == QueueRunning
}

// PublishDestinationTasks creates destination traversal tasks by querying both source and destination tables
func (qp *QueuePublisher) PublishDestinationTasks(queueName string) {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	currentLevel := qp.QueueLevels[queueName]
	qp.Mutex.Unlock()

	if !exists {
		logging.GlobalLogger.LogQP("error", queueName, "", "Destination queue not found", nil)
		return
	}

	// Discover and set root path for destination queue if not already done
	if err := qp.discoverAndSetRootPath(queueName, "destination_nodes"); err != nil {
		logging.GlobalLogger.LogQP("error", queueName, "", "Failed to discover destination root path", map[string]any{
			"error": err.Error(),
		})
		return
	}

	// Force flush destination table to ensure all pending writes are committed
	qp.FlushTable("destination_nodes")

	// Step 1: Query destination table for pending folders at current level
	dstFolders := qp.fetchDestinationFolders(currentLevel)
	if len(dstFolders) == 0 {
		logging.GlobalLogger.LogQP("info", queueName, "", "No pending destination folders found", map[string]any{
			"level": currentLevel,
		})
		return
	}

	logging.GlobalLogger.LogQP("info", queueName, "", "Found destination folders to process", map[string]any{
		"count": len(dstFolders),
		"level": currentLevel,
	})

	// Step 2: For each destination folder, get expected source children and create tasks
	var tasks []Task
	for _, dstFolder := range dstFolders {
		expectedSrcChildren, expectedSrcFiles := qp.fetchExpectedSourceChildren(dstFolder.Path)

		// Create destination traversal task with expected source children
		taskID := fmt.Sprintf("dst-%d-%s", currentLevel, dstFolder.Identifier)
		dstTask, err := NewDstTraversalTask(taskID, dstFolder, nil, expectedSrcChildren, expectedSrcFiles)
		if err != nil {
			logging.GlobalLogger.LogQP("error", queueName, dstFolder.Path, "Failed to create destination task", map[string]any{
				"error": err.Error(),
			})
			continue
		}

		tasks = append(tasks, dstTask)
	}

	if len(tasks) > 0 {
		queue.AddTasks(tasks)
		logging.GlobalLogger.LogQP("info", queueName, "", "Added destination tasks to queue", map[string]any{
			"taskCount": len(tasks),
			"level":     currentLevel,
		})
	}
}

// fetchDestinationFolders queries the destination table for pending folders at the specified level
func (qp *QueuePublisher) fetchDestinationFolders(level int) []*filesystem.Folder {
	query := `SELECT path, name, identifier, parent_id, last_modified 
	          FROM destination_nodes 
	          WHERE traversal_status = 'pending' AND level = ? AND type = 'folder'
	          ORDER BY path`

	rows, err := qp.DB.Query("destination_nodes", query, level)
	if err != nil {
		logging.GlobalLogger.LogSystem("error", "QP", "Failed to query destination folders", map[string]any{
			"error": err.Error(),
			"level": level,
		})
		return nil
	}
	defer rows.Close()

	var folders []*filesystem.Folder
	for rows.Next() {
		var path, name, identifier, parentID, lastModified string
		if err := rows.Scan(&path, &name, &identifier, &parentID, &lastModified); err != nil {
			logging.GlobalLogger.LogSystem("error", "QP", "Failed to scan destination folder row", map[string]any{
				"error": err.Error(),
			})
			continue
		}

		folder := &filesystem.Folder{
			Name:         name,
			Path:         path,
			ParentID:     parentID,
			Identifier:   identifier,
			LastModified: lastModified,
			Level:        level,
		}
		folders = append(folders, folder)
	}

	return folders
}

// fetchExpectedSourceChildren queries the source table for expected children of the given path
func (qp *QueuePublisher) fetchExpectedSourceChildren(dstPath string) ([]*filesystem.Folder, []*filesystem.File) {
	// Query source table for children of this path
	query := `SELECT path, name, identifier, parent_id, type, last_modified, size
	          FROM source_nodes 
	          WHERE parent_id = ?
	          ORDER BY type, name`

	rows, err := qp.DB.Query("source_nodes", query, dstPath)
	if err != nil {
		logging.GlobalLogger.LogSystem("error", "QP", "Failed to query expected source children", map[string]any{
			"error": err.Error(),
			"path":  dstPath,
		})
		return nil, nil
	}
	defer rows.Close()

	var expectedFolders []*filesystem.Folder
	var expectedFiles []*filesystem.File

	for rows.Next() {
		var path, name, identifier, parentID, nodeType, lastModified string
		var size int64
		if err := rows.Scan(&path, &name, &identifier, &parentID, &nodeType, &lastModified, &size); err != nil {
			logging.GlobalLogger.LogSystem("error", "QP", "Failed to scan source child row", map[string]any{
				"error": err.Error(),
			})
			continue
		}

		if nodeType == "folder" {
			folder := &filesystem.Folder{
				Name:         name,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        0, // Will be set properly when used
			}
			expectedFolders = append(expectedFolders, folder)
		} else if nodeType == "file" {
			file := &filesystem.File{
				Name:         name,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Size:         size,
			}
			expectedFiles = append(expectedFiles, file)
		}
	}

	return expectedFolders, expectedFiles
}

// discoverAndSetRootPath finds the root folder (level 0) for the given table and stores it
func (qp *QueuePublisher) discoverAndSetRootPath(queueName string, table string) error {
	qp.Mutex.Lock()
	// Check if root path is already discovered for this queue
	if _, exists := qp.RootPaths[queueName]; exists {
		qp.Mutex.Unlock()
		return nil
	}
	qp.Mutex.Unlock()

	// Query for the root folder (level 0)
	query := `SELECT path FROM ` + table + ` WHERE level = 0 AND type = 'folder' LIMIT 1`
	rows, err := qp.DB.Query(table, query)
	if err != nil {
		logging.GlobalLogger.LogSystem("error", "QP", "Failed to discover root path", map[string]any{
			"error": err.Error(),
			"table": table,
			"queue": queueName,
		})
		return err
	}
	defer rows.Close()

	var rootPath string
	if rows.Next() {
		if err := rows.Scan(&rootPath); err != nil {
			logging.GlobalLogger.LogSystem("error", "QP", "Failed to scan root path", map[string]any{
				"error": err.Error(),
				"table": table,
				"queue": queueName,
			})
			return err
		}
	} else {
		return fmt.Errorf("no root folder (level 0) found in table %s", table)
	}

	// Store the discovered root path
	qp.Mutex.Lock()
	qp.RootPaths[queueName] = rootPath
	qp.Mutex.Unlock()

	logging.GlobalLogger.LogSystem("info", "QP", "Discovered root path for queue", map[string]any{
		"queue":    queueName,
		"rootPath": rootPath,
		"table":    table,
	})

	return nil
}

// normalizePathForJoin converts an absolute path to a root-relative path for cross-table joins
func (qp *QueuePublisher) normalizePathForJoin(queueName, absolutePath string) string {
	qp.Mutex.Lock()
	rootPath, exists := qp.RootPaths[queueName]
	qp.Mutex.Unlock()

	if !exists || rootPath == "" {
		// Fallback: return the absolute path if no root is known
		return absolutePath
	}

	// If this is the root path itself, return "/"
	if absolutePath == rootPath {
		return "/"
	}

	// Remove the root prefix and ensure it starts with "/"
	if strings.HasPrefix(absolutePath, rootPath) {
		relativePath := strings.TrimPrefix(absolutePath, rootPath)
		if !strings.HasPrefix(relativePath, "/") {
			relativePath = "/" + relativePath
		}
		return relativePath
	}

	// Fallback: return absolute path if it doesn't match the root
	return absolutePath
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

	// Discover and set root path for this queue if not already done
	if err := qp.discoverAndSetRootPath(queueName, table); err != nil {
		logging.GlobalLogger.LogQP("error", queueName, "", "Failed to discover root path", map[string]any{
			"error": err.Error(),
		})
		return
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
			signals.GlobalSR.Publish(signals.Signal{
				Topic:   "qp:traversal_complete:" + queueName,
				Payload: queueName,
			})
			return
		}
	}

	if len(tasks) > 0 {
		queue.AddTasks(tasks)
		// 🆕 NEW: Reset idle trigger when new tasks are added
		queue.ResetIdleTrigger()
		qp.QueriesPerPhase[queueName]++
		if mode == firstPass {
			if lastRaw, ok := qp.LastDBPaths[queueName]; ok && lastRaw != "" {
				qp.LastPathCursors[queueName] = lastRaw
			} else if len(tasks) > 0 {
				// Fallback to task path if we don't have a recorded raw DB path yet
				qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath()
			}
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

	signals.GlobalSR.Publish(signals.Signal{
		Topic:   "qp:phase_updated",
		Payload: queue.Phase,
	})
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

				if qp.isRoundComplete(queue) {
					// Flush and verify no pending rows at current level before advancing
					table := "source_nodes"
					if queue.Type == UploadQueueType {
						table = "destination_nodes"
					}
					qp.FlushTable(table)

					if qp.hasPendingAtLevel(queueName, queue.Phase) {
						// Still work to publish at this level
						qp.PublishTasks(queueName)
						continue
					}

					// Safe to advance
					qp.advancePhase(queueName)

					// Then check if the new phase has no tasks (traversal complete)
					if qp.checkTraversalComplete(queueName, 0) {
						qp.FlushTable(table)
						signals.GlobalSR.Publish(signals.Signal{
							Topic:   "qp:traversal_complete:" + queueName,
							Payload: queueName,
						})
						// Exit immediately to prevent further operations
						return
					}
					return
				}
				qp.PublishTasks(queueName)
			}
		}
	}()
}

// hasPendingAtLevel checks if there are any pending rows at the specified level for the queue's table
func (qp *QueuePublisher) hasPendingAtLevel(queueName string, level int) bool {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()
	if !exists {
		return false
	}

	table := "source_nodes"
	statusColumn := "traversal_status"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
		statusColumn = "upload_status"
	}

	// Ensure pending writes are flushed before checking
	qp.FlushTable(table)

	query := `SELECT COUNT(*) FROM ` + table + ` WHERE ` + statusColumn + ` = 'pending' AND level = ?`
	rows, err := qp.DB.Query(table, query, level)
	if err != nil {
		logging.GlobalLogger.LogMessage("error", "hasPendingAtLevel query failed", map[string]any{
			"queue": queueName,
			"level": level,
			"error": err.Error(),
		})
		// Be conservative: assume pending exists so we don't advance too early
		return true
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			logging.GlobalLogger.LogMessage("error", "hasPendingAtLevel scan failed", map[string]any{
				"queue": queueName,
				"level": level,
				"error": err.Error(),
			})
			return true
		}
	}

	logging.GlobalLogger.LogMessage("info", "hasPendingAtLevel result", map[string]any{
		"queue":   queueName,
		"level":   level,
		"pending": count,
	})
	return count > 0
}

func (qp *QueuePublisher) FetchTasksFromDB(table string, queueType QueueType, currentLevel int, lastSeenPath string, queueName string) []Task {
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
func (qp *QueuePublisher) runTaskQuery(table, query string, params []any, currentLevel int, queueName string) []Task {
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

	var tasks []Task
	for rows.Next() {
		var path, name, identifier, parentID, nodeType, lastModified string
		if err := rows.Scan(&path, &name, &identifier, &parentID, &nodeType, &lastModified); err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to scan row", map[string]any{
				"err": err.Error(),
			})
			continue
		}

		// Record last raw DB path for stable cursor updates
		qp.Mutex.Lock()
		qp.LastDBPaths[queueName] = path
		qp.Mutex.Unlock()

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
			task, err := NewSrcTraversalTask(normalizedPath, folder, &folder.ParentID)
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
