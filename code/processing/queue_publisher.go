package processing

import (
	"strings"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/signals"
)

// queueSnapshot holds queue data captured under mutex
type queueSnapshot struct {
	queueName       string
	queue           *TaskQueue
	currentLevel    int
	queriesPerPhase int
	srcOrDst        string
	state           QueueState
}

// ConductorInterface defines the methods QP needs from Conductor
type ConductorInterface interface {
	SetupDestinationQueue(num_of_workers ...int)
}

// QueuePublisher manages multiple queues dynamically.
type QueuePublisher struct {
	DB              *db.DB
	Queues          map[string]*TaskQueue
	QueueLevels     map[string]int
	Mutex           sync.Mutex
	Running         bool
	RetryThreshold  int
	BatchSize       int
	QueriesPerPhase map[string]int // queueName -> queryCount
	// Conductor reference for dynamic queue creation
	Conductor ConductorInterface
}

func NewQueuePublisher(db *db.DB, retryThreshold, batchSize int) *QueuePublisher {
	logging.GlobalLogger.Log(
		"info",                            // level
		"System",                          // entity
		"QP",                              // entityID
		"Initializing new QueuePublisher", // message
		map[string]any{
			"retryThreshold": retryThreshold,
			"batchSize":      batchSize,
		},
		"INIT", // action
		"All",  // topic
	)

	return &QueuePublisher{
		DB:              db,
		Queues:          make(map[string]*TaskQueue),
		QueueLevels:     make(map[string]int),
		Running:         true,
		RetryThreshold:  retryThreshold,
		BatchSize:       batchSize,
		QueriesPerPhase: make(map[string]int),
		Conductor:       nil, // Will be set by Conductor after creation
	}
}

// SetConductor sets the conductor reference for dynamic queue creation
func (qp *QueuePublisher) SetConductor(conductor ConductorInterface) {
	qp.Conductor = conductor
}

func (qp *QueuePublisher) FlushTable(table string) {
	qp.Mutex.Lock()
	defer qp.Mutex.Unlock()

	wq := qp.DB.GetWriteQueue(table)

	if wq != nil {
		logging.GlobalLogger.Log(
			"debug", "QueuePublisher", "QP", "Flushing write queue", map[string]any{
				"table": table,
			}, "FLUSH", logging.TableAcronyms[table],
		)

		batches := wq.Flush(true)
		if len(batches) > 0 {
			logging.GlobalLogger.Log(
				"debug", "QueuePublisher", "QP", "Executing batch commands", map[string]any{
					"table":      table,
					"batchCount": len(batches),
				}, "EXECUTE", logging.TableAcronyms[table],
			)
			qp.DB.ExecuteBatchCommands(batches)
		} else {
			logging.GlobalLogger.Log(
				"debug", "QueuePublisher", "QP", "No batches to flush", map[string]any{
					"table": table,
				}, "NO_FLUSH", logging.TableAcronyms[table],
			)
		}
	} else {
		logging.GlobalLogger.Log(
			"error", "QueuePublisher", "QP", "WriteQueue not found", map[string]any{
				"table": table,
			}, "ERROR", logging.TableAcronyms[table],
		)
	}
}

func (qp *QueuePublisher) Run() {
	logging.GlobalLogger.Log(
		"info", "QueuePublisher", "QP", "Starting QueuePublisher event-driven control", nil, "START", "All",
	)

	qp.Mutex.Lock()
	qp.Running = true
	qp.Mutex.Unlock()

	// Bootstrap: Set up signal listeners
	qp.bootstrap()

	// This section of lock to get queue names portion is to avoid double locks
	// because publish tasks also calls a lock on QP. Maybe we should do a refactor
	// or cleanup sometime soon. Soon™.
	qp.Mutex.Lock()
	queueNames := make([]string, 0, len(qp.Queues))
	for queueName := range qp.Queues {
		queueNames = append(queueNames, queueName)
	}
	qp.Mutex.Unlock()

	// Publish initial tasks to all queues at startup
	for _, queueName := range queueNames {
		go qp.PublishTasks(queueName)
	}

	// Main event loop - respond to worker signals and monitor completion
	for qp.Running {
		qp.manageQueueCompletion()
		time.Sleep(200 * time.Millisecond)
	}
}

// bootstrap handles startup: initial tasks + signal setup + ready signal
func (qp *QueuePublisher) bootstrap() {
	qp.Mutex.Lock()
	queueNames := make([]string, 0, len(qp.Queues))
	for queueName := range qp.Queues {
		queueNames = append(queueNames, queueName)
	}
	qp.Mutex.Unlock()

	// Set up signal listeners for all queues
	logging.GlobalLogger.Log(
		"info", "QueuePublisher", "QP", "Setting up worker signal listeners", nil, "SETUP", "All",
	)
	for _, queueName := range queueNames {
		qp.setupQueueSignals(queueName)
	}

	logging.GlobalLogger.Log(
		"info", "QueuePublisher", "QP", "QP fully initialized - now responding to worker signals", nil, "INIT", "All",
	)
}

// setupQueueSignals sets up signal listeners for a specific queue
func (qp *QueuePublisher) setupQueueSignals(queueName string) {
	runningLowTopic := "tasks_running_low:" + queueName
	signals.GlobalSR.On(runningLowTopic, func(sig signals.Signal) {
		logging.GlobalLogger.Log(
			"info", "QueuePublisher", "QP", "Received running low signal from workers", map[string]any{
				"queue": queueName,
			}, "SIGNAL", logging.QueueAcronyms[queueName],
		)
		go qp.PublishTasks(queueName)
	})

	logging.GlobalLogger.Log(
		"info", "QueuePublisher", "QP", "Subscribed to worker signals", map[string]any{
			"queue": queueName,
			"topic": runningLowTopic,
		}, "SUBSCRIBE", logging.QueueAcronyms[queueName],
	)
}

// manageQueueCompletion handles phase advancement and completion detection for all queues
func (qp *QueuePublisher) manageQueueCompletion() {
	// Step 1: Capture all queue data under mutex (snapshot pattern)
	qp.Mutex.Lock()
	var snapshots []queueSnapshot
	for queueName, queue := range qp.Queues {
		if queue.State != QueueRunning {
			continue
		}
		snapshots = append(snapshots, queueSnapshot{
			queueName:       queueName,
			queue:           queue,
			currentLevel:    qp.QueueLevels[queueName],
			queriesPerPhase: qp.QueriesPerPhase[queueName],
			srcOrDst:        queue.SrcOrDst,
			state:           queue.State,
		})
	}
	qp.Mutex.Unlock()

	// Step 2: Process snapshots without holding mutex
	for _, snap := range snapshots {
		logging.GlobalLogger.Log(
			"debug", "System", "QP", "Checking queue advancement eligibility", map[string]any{
				"queue":           snap.queueName,
				"currentLevel":    snap.currentLevel,
				"queriesPerPhase": snap.queriesPerPhase,
			}, "CHECK_QUEUE_ADVANCEMENT", logging.QueueAcronyms[snap.queueName],
		)

		// 1. Check if entire traversal is complete
		if qp.isTraversalComplete(snap.queueName, snap.queue, snap.currentLevel, snap.queriesPerPhase) {
			// Safely set queue state to complete
			qp.Mutex.Lock()
			if queue, exists := qp.Queues[snap.queueName]; exists && queue.State == QueueRunning {
				queue.State = QueueComplete
				logging.GlobalLogger.Log(
					"info", "System", "QP", "Queue traversal complete", map[string]any{
						"queue": snap.queueName,
						"type":  snap.srcOrDst,
					}, "TRAVERSAL_COMPLETE", logging.QueueAcronyms[snap.queueName],
				)
			}
			qp.Mutex.Unlock()
			continue
		}

		// 2. If not complete, check if queue is ready to advance
		if qp.isQueueReadyToAdvance(snap.queueName) {
			logging.GlobalLogger.Log(
				"debug", "System", "QP", "Queue ready to advance phase", map[string]any{
					"queue":        snap.queueName,
					"currentLevel": snap.currentLevel,
				}, "ADVANCE_READY", logging.QueueAcronyms[snap.queueName],
			)
			qp.advancePhaseWithMutex(snap.queueName)
		}
	}
}

// isQueueReadyToAdvance determines if a queue (src or dst) is ready to advance to the next phase.
func (qp *QueuePublisher) isQueueReadyToAdvance(queueName string) bool {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	currentLevel := qp.QueueLevels[queueName]
	srcLevel := qp.QueueLevels["src-traversal"]
	qp.Mutex.Unlock()
	if !exists || queue.State != QueueRunning {
		return false
	}

	queue.mu.Lock()
	queueSize := len(queue.tasks)
	allWorkersIdle := true
	for _, worker := range queue.workers {
		if worker.State != WorkerIdle {
			allWorkersIdle = false
			break
		}
	}
	queue.mu.Unlock()

	hasPending := qp.hasPendingAtLevel(queueName, queue.Phase)
	if queueSize > 0 || !allWorkersIdle || hasPending {
		return false
	}

	// For dst, enforce N+2 rule
	if queue.SrcOrDst == "dst" {
		requiredSrcLevel := currentLevel + 2
		srcQueue, srcExists := qp.Queues["src-traversal"]
		if srcLevel < requiredSrcLevel && srcExists && srcQueue.State == QueueRunning {
			return false
		}
	}
	return true
}

// isTraversalComplete checks if the entire traversal is complete for a queue
func (qp *QueuePublisher) isTraversalComplete(queueName string, queue *TaskQueue, currentLevel int, queriesPerPhase int) bool {
	// logging.GlobalLogger.Log(
	// 	"debug", "QueuePublisher", "QP", "Checking traversal completion", map[string]any{
	// 		"queue":           queueName,
	// 		"currentLevel":    currentLevel,
	// 		"queriesPerPhase": queriesPerPhase,
	// 	}, "CHECK", logging.QueueAcronyms[queueName],
	// )

	table := "source_nodes"
	statusColumn := "traversal_status"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
		statusColumn = "upload_status"
	} else if queue.SrcOrDst == "dst" {
		table = "destination_nodes"
	}

	checkQuery := `SELECT COUNT(*) FROM ` + table + ` WHERE ` + statusColumn + ` = 'pending' AND level >= ?`
	logging.GlobalLogger.Log(
		"debug", "System", "QP", "Checking for pending tasks in database", map[string]any{
			"queue":        queueName,
			"query":        checkQuery,
			"currentLevel": currentLevel,
			"table":        table,
		}, "QUERY", logging.QueueAcronyms[queueName],
	)

	rows, err := qp.DB.Query(table, checkQuery, currentLevel)
	if err != nil {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Failed to check pending tasks", map[string]any{
				"queue": queueName,
				"error": err.Error(),
			}, "ERROR", logging.QueueAcronyms[queueName],
		)
		return false
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}

	logging.GlobalLogger.Log(
		"debug", "System", "QP", "Pending tasks check result", map[string]any{
			"queue":        queueName,
			"currentLevel": currentLevel,
			"pendingCount": count,
			"isComplete":   count == 0,
		}, "RESULT", logging.QueueAcronyms[queueName],
	)

	/*
	 This means we advanced the round and didn't see anything, which means we have exhausted max depth.

	 NOTE:
	 Technically there are scenarios where we didn't actually traverse the previous items, and thus didn't
	 list out the children to traverse, thus it *thinks* we are done. But they should be caught in the
	 retry logic and if it still failed after N retries then that's between you and god at that point. lol

	 It will at least show this in the path review phase for traversal issues and post migration report
	 for traversal and / or upload issues.
	*/
	return count == 0 && queriesPerPhase < 2 && currentLevel > 0
}

// createDestinationQueueNow creates destination queue immediately (called when source advances to level 2)
func (qp *QueuePublisher) createDestinationQueueNow() {
	qp.Mutex.Lock()

	// Double-check that destination queue doesn't exist (race condition protection)
	if _, dstExists := qp.QueueLevels["dst-traversal"]; dstExists {
		qp.Mutex.Unlock()
		return // Someone else already created it
	}

	srcLevel := qp.QueueLevels["src-traversal"]

	logging.GlobalLogger.Log(
		"info", "System", "QP", "Source reached level 2, creating destination queue", map[string]any{
			"srcLevel":            srcLevel,
			"trigger":             "event-driven",
			"dstCanStartAtLevel0": true,
		}, "CREATE", logging.QueueAcronyms["dst-traversal"],
	)

	// Set the queue level IMMEDIATELY to prevent race condition
	qp.QueueLevels["dst-traversal"] = 0
	qp.QueriesPerPhase["dst-traversal"] = 0

	qp.Mutex.Unlock()

	// Create queue and workers (outside of mutex to avoid blocking)
	qp.Conductor.SetupDestinationQueue()
	qp.setupQueueSignals("dst-traversal")
	go qp.PublishTasks("dst-traversal")
}

// PublishDestinationTasks creates destination traversal tasks using simplified leasing model
func (qp *QueuePublisher) PublishDestinationTasks(queueName string) {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	currentLevel := qp.QueueLevels[queueName]
	qp.Mutex.Unlock()

	if !exists {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Destination queue not found", map[string]any{
				"queue": queueName,
			}, "ERROR", logging.QueueAcronyms[queueName],
		)
		return
	}

	// Publish destination tasks one batch at a time, respecting running low threshold
	for {
		tasks := qp.FetchDestinationTasksFromDB(currentLevel, queueName)

		if len(tasks) == 0 {
			logging.GlobalLogger.Log("debug", "System", "QP", "No more destination tasks at current level", map[string]any{
				"level": currentLevel,
				"queue": queueName,
			}, "NO_MORE_DST_TASKS_AT_LEVEL", logging.QueueAcronyms[queueName])
			break // No more tasks available at this level
		}

		logging.GlobalLogger.Log("debug", "System", "QP", "Publishing batch of destination tasks", map[string]any{
			"taskCount": len(tasks),
			"level":     currentLevel,
		}, "PUBLISHING_DST_TASK_BATCH", logging.QueueAcronyms[queueName])

		queue.AddTasks(tasks)
		qp.Mutex.Lock()
		qp.QueriesPerPhase[queueName]++
		qp.Mutex.Unlock()

		// Check if queue is still below running low threshold
		if queue.TasksRunningLow() {
			logging.GlobalLogger.Log("debug", "System", "QP", "Destination queue still running low, fetching another batch", map[string]any{
				"queueSize": queue.QueueSize(),
				"threshold": queue.RunningLowThreshold,
			}, "DST_QUEUE_STILL_RUNNING_LOW", logging.QueueAcronyms[queueName])
			continue // Fetch another batch
		} else {
			logging.GlobalLogger.Log("debug", "System", "QP", "Destination queue threshold satisfied, stopping batch publishing", map[string]any{
				"queueSize": queue.QueueSize(),
				"threshold": queue.RunningLowThreshold,
			}, "DST_QUEUE_THRESHOLD_SATISFIED", logging.QueueAcronyms[queueName])
			break // Queue has enough tasks now
		}
	}
}

// FetchDestinationTasksFromDB efficiently fetches destination tasks using simplified leasing model
func (qp *QueuePublisher) FetchDestinationTasksFromDB(currentLevel int, queueName string) []Task {
	logging.GlobalLogger.Log(
		"debug", "System", "QP", "Fetching destination tasks from DB", map[string]any{
			"currentLevel": currentLevel,
			"queueName":    queueName,
		}, "FETCH_TASKS", logging.QueueAcronyms[queueName],
	)

	// QUERY 1: Get dst folders at current level with TrackedPaths filtering
	query1 := `SELECT path, name, identifier, parent_id, last_modified 
	          FROM destination_nodes WHERE 
	          (traversal_status = 'pending' OR (traversal_status = 'failed' AND traversal_attempts < ?)) 
	          AND level = ? AND type = 'folder'`

	params1 := []any{qp.RetryThreshold, currentLevel}

	// Add TrackedPaths filtering to prevent re-processing active tasks
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()

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
			query1 += ` AND path NOT IN (` + strings.Join(placeholders, ", ") + `)`

			// Add the tracked paths as parameters
			for _, path := range pathList {
				params1 = append(params1, path)
			}

			logging.GlobalLogger.Log(
				"debug", "System", "QP", "TrackedPaths filtering applied (dst)", map[string]any{
					"queueName":     queueName,
					"excludedPaths": len(pathList),
				}, "TRACKED_PATHS_FILTERING", logging.QueueAcronyms[queueName],
			)
		}
	}

	query1 += ` ORDER BY path LIMIT ?`
	params1 = append(params1, qp.BatchSize)

	// Debug log the query
	logging.GlobalLogger.Log(
		"info", "System", "QP", "Executing dst query", map[string]any{
			"query":  query1,
			"params": params1,
		}, "EXECUTE_QUERY", logging.QueueAcronyms[queueName],
	)

	// ADDITIONAL DEBUG: Check what's actually in the dst table at this level
	debugQuery := `SELECT path, name, type, traversal_status, level FROM destination_nodes WHERE level = ?`
	debugRows, debugErr := qp.DB.Query("destination_nodes", debugQuery, currentLevel)
	if debugErr == nil {
		defer debugRows.Close()
		var debugItems []map[string]any
		for debugRows.Next() {
			var path, name, nodeType, status string
			var level int
			if err := debugRows.Scan(&path, &name, &nodeType, &status, &level); err == nil {
				debugItems = append(debugItems, map[string]any{
					"path":   path,
					"name":   name,
					"type":   nodeType,
					"status": status,
					"level":  level,
				})
			}
		}
		logging.GlobalLogger.Log(
			"debug", "System", "QP", "Dst table contents at level", map[string]any{
				"level": currentLevel,
				"items": debugItems,
			}, "DEBUG_TABLE_CONTENTS", logging.QueueAcronyms[queueName],
		)
	}

	// Execute Query 1
	rows, err := qp.DB.Query("destination_nodes", query1, params1...)
	if err != nil {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Dst DB query failed", map[string]any{
				"error": err.Error(),
			}, "ERROR", logging.QueueAcronyms[queueName],
		)
		return nil
	}
	defer rows.Close()

	var dstFolders []*filesystem.Folder
	var dstPaths []string

	for rows.Next() {
		var path, name, identifier, parentID, lastModified string
		if err := rows.Scan(&path, &name, &identifier, &parentID, &lastModified); err != nil {
			logging.GlobalLogger.Log(
				"error", "System", "QP", "Failed to scan dst folder row", map[string]any{
					"err": err.Error(),
				}, "ERROR", logging.QueueAcronyms[queueName],
			)
			continue
		}

		folder := &filesystem.Folder{
			Name:         name,
			Path:         path,
			ParentID:     parentID,
			Identifier:   identifier,
			LastModified: lastModified,
			Level:        currentLevel,
		}
		dstFolders = append(dstFolders, folder)
		dstPaths = append(dstPaths, path) // Collect paths for batch query
	}

	if len(dstFolders) == 0 {
		logging.GlobalLogger.Log(
			"warning", "System", "QP", "No dst folders found despite hasPendingAtLevel returning true", map[string]any{
				"currentLevel": currentLevel,
				"query":        query1,
				"params":       params1,
			}, "NO_FOLDERS_FOUND", logging.QueueAcronyms[queueName],
		)
		return nil
	}

	// QUERY 2 & 3 COMBINED: Get all expected children for all dst paths in one efficient batch query
	expectedChildrenMap := qp.fetchExpectedSourceChildrenBatch(dstPaths)

	// Create tasks with expected children
	var tasks []Task
	for _, dstFolder := range dstFolders {
		expectedChildren := expectedChildrenMap[dstFolder.Path]
		var expectedSrcChildren []*filesystem.Folder
		var expectedSrcFiles []*filesystem.File
		if expectedChildren != nil {
			expectedSrcChildren = expectedChildren.Folders
			expectedSrcFiles = expectedChildren.Files
		}

		dstTask, err := NewDstTraversalTask(dstFolder.Identifier, dstFolder, nil, expectedSrcChildren, expectedSrcFiles)
		if err != nil {
			logging.GlobalLogger.Log(
				"error", "System", "QP", "Failed to create dst task", map[string]any{
					"error": err.Error(),
				}, "ERROR", logging.QueueAcronyms[queueName],
			)
			continue
		}

		tasks = append(tasks, dstTask)
	}

	qp.Mutex.Lock()
	qp.QueriesPerPhase[queueName]++
	qp.Mutex.Unlock()

	logging.GlobalLogger.Log(
		"info", "System", "QP", "Created destination tasks", map[string]any{
			"dstFoldersFound": len(dstFolders),
			"tasksCreated":    len(tasks),
			"totalQueries":    3, // Efficient: 1 for dst folders + 2 for expected children lookup (path match + children)
		}, "TASKS_CREATED", logging.QueueAcronyms[queueName],
	)

	return tasks
}

// ExpectedChildren holds the expected folders and files for a path
type ExpectedChildren struct {
	Folders []*filesystem.Folder
	Files   []*filesystem.File
}

// fetchExpectedSourceChildrenBatch efficiently queries source table for expected children using 2-step process:
// 1. Find src items with matching paths (path = foreign key)
// 2. Get children of those src items using their identifiers as parent_id
func (qp *QueuePublisher) fetchExpectedSourceChildrenBatch(dstPaths []string) map[string]*ExpectedChildren {
	if len(dstPaths) == 0 {
		return make(map[string]*ExpectedChildren)
	}

	// STEP 1: Find src items with matching paths to get their identifiers
	placeholders := make([]string, len(dstPaths))
	params := make([]any, len(dstPaths))
	for i, path := range dstPaths {
		placeholders[i] = "?"
		params[i] = path
	}

	// Query src table to find items with matching paths
	srcQuery := `SELECT path, identifier FROM source_nodes WHERE path IN (` + strings.Join(placeholders, ", ") + `)`

	srcRows, err := qp.DB.Query("source_nodes", srcQuery, params...)
	if err != nil {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Failed to find matching src items", map[string]any{
				"error": err.Error(),
				"paths": dstPaths,
			}, "ERROR", logging.QueueAcronyms["src-traversal"],
		)
		return make(map[string]*ExpectedChildren)
	}
	defer srcRows.Close()

	// Collect src identifiers and map them back to dst paths
	var srcIdentifiers []string
	pathToIdentifier := make(map[string]string) // dst path -> src identifier

	for srcRows.Next() {
		var srcPath, srcIdentifier string
		if err := srcRows.Scan(&srcPath, &srcIdentifier); err != nil {
			continue
		}
		srcIdentifiers = append(srcIdentifiers, srcIdentifier)
		pathToIdentifier[srcPath] = srcIdentifier
	}

	if len(srcIdentifiers) == 0 {
		logging.GlobalLogger.Log(
			"info", "System", "QP", "No matching src items found for dst paths", map[string]any{
				"dstPaths": dstPaths,
			}, "NO_MATCHING_SRC_ITEMS", logging.QueueAcronyms["src-traversal"],
		)
		return make(map[string]*ExpectedChildren)
	}

	// STEP 2: Get children of those src items using their identifiers as parent_id
	childPlaceholders := make([]string, len(srcIdentifiers))
	childParams := make([]any, len(srcIdentifiers))
	for i, identifier := range srcIdentifiers {
		childPlaceholders[i] = "?"
		childParams[i] = identifier
	}

	// Query for children of the matched src items
	query := `SELECT path, name, identifier, parent_id, type, level, last_modified, size
	          FROM source_nodes 
	          WHERE parent_id IN (` + strings.Join(childPlaceholders, ", ") + `)
	          ORDER BY parent_id, type, name`

	rows, err := qp.DB.Query("source_nodes", query, childParams...)
	if err != nil {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Failed to get children of matched src items", map[string]any{
				"error":          err.Error(),
				"srcIdentifiers": srcIdentifiers,
			}, "ERROR", logging.QueueAcronyms["src-traversal"],
		)
		return make(map[string]*ExpectedChildren)
	}
	defer rows.Close()

	// Group results by parent identifier, then map back to dst path
	childrenByParentId := make(map[string]*ExpectedChildren)

	for rows.Next() {
		var path, name, identifier, parentID, nodeType, lastModified string
		var size int64
		var level int
		if err := rows.Scan(&path, &name, &identifier, &parentID, &nodeType, &level, &lastModified, &size); err != nil {
			logging.GlobalLogger.Log(
				"error", "System", "QP", "Failed to scan expected child row", map[string]any{
					"err": err.Error(),
				}, "ERROR", logging.QueueAcronyms["src-traversal"],
			)
			continue
		}

		// Initialize map entry if needed
		if _, exists := childrenByParentId[parentID]; !exists {
			childrenByParentId[parentID] = &ExpectedChildren{
				Folders: []*filesystem.Folder{},
				Files:   []*filesystem.File{},
			}
		}

		switch nodeType {
		case "folder":
			folder := &filesystem.Folder{
				Name:         name,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        level, // Use the actual source level from database
			}
			childrenByParentId[parentID].Folders = append(childrenByParentId[parentID].Folders, folder)
		case "file":
			file := &filesystem.File{
				Name:         name,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Size:         size,
				Level:        level, // Use the actual source level from database
			}
			childrenByParentId[parentID].Files = append(childrenByParentId[parentID].Files, file)
		}
	}

	// Map results back to dst paths (the key we need for the caller)
	result := make(map[string]*ExpectedChildren)
	for dstPath, srcIdentifier := range pathToIdentifier {
		if children, exists := childrenByParentId[srcIdentifier]; exists {
			result[dstPath] = children
		}
	}

	return result
}

func (qp *QueuePublisher) PublishTasks(queueName string) {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()
	if !exists {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Queue not found", map[string]any{},
			"ERROR", logging.QueueAcronyms[queueName],
		)
		return
	}

	// Destination traversal uses special cross-table logic
	if queue.Type == TraversalQueueType && queue.SrcOrDst == "dst" {
		qp.PublishDestinationTasks(queueName)
		return
	}

	// Determine the correct table based on queue type and src/dst designation
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	}

	qp.Mutex.Lock()
	currentLevel := qp.QueueLevels[queueName]
	qp.Mutex.Unlock()

	// Publish one batch at a time, respecting running low threshold
	for {
		tasks := qp.FetchTasksFromDB(table, queue.Type, currentLevel, queueName)

		if len(tasks) == 0 {
			logging.GlobalLogger.Log("debug", "System", "QP", "No more tasks at current level", map[string]any{
				"level": currentLevel,
				"queue": queueName,
			}, "NO_MORE_TASKS_AT_LEVEL", logging.QueueAcronyms[queueName])
			break // No more tasks available at this level
		}

		logging.GlobalLogger.Log("debug", "System", "QP", "Publishing batch of tasks", map[string]any{
			"taskCount": len(tasks),
			"level":     currentLevel,
		}, "PUBLISHING_TASK_BATCH", logging.QueueAcronyms[queueName])

		queue.AddTasks(tasks)
		qp.Mutex.Lock()
		qp.QueriesPerPhase[queueName]++
		qp.Mutex.Unlock()

		// Check if queue is still below running low threshold
		if queue.TasksRunningLow() {
			logging.GlobalLogger.Log("debug", "System", "QP", "Queue still running low, fetching another batch", map[string]any{
				"queueSize": queue.QueueSize(),
				"threshold": queue.RunningLowThreshold,
			}, "QUEUE_STILL_RUNNING_LOW", logging.QueueAcronyms[queueName])
			continue // Fetch another batch
		} else {
			logging.GlobalLogger.Log("debug", "System", "QP", "Queue threshold satisfied, stopping batch publishing", map[string]any{
				"queueSize": queue.QueueSize(),
				"threshold": queue.RunningLowThreshold,
			}, "QUEUE_THRESHOLD_SATISFIED", logging.QueueAcronyms[queueName])
			break // Queue has enough tasks now
		}
	}
}

// advancePhaseWithMutex advances a queue phase (SAFE - acquires its own mutex)
func (qp *QueuePublisher) advancePhaseWithMutex(queueName string) {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	if !exists {
		qp.Mutex.Unlock()
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Cannot advance phase - queue not found", map[string]any{
				"queue": queueName,
			}, "ERROR", logging.QueueAcronyms[queueName],
		)
		return
	}

	// Advance the phase
	queue.Phase++
	qp.QueueLevels[queueName] = queue.Phase
	qp.QueriesPerPhase[queueName] = 0
	syncedLevel := qp.QueueLevels[queueName]

	// ✅ Event-driven destination queue creation: Create when source advances to level 2
	_, dstExists := qp.QueueLevels["dst-traversal"]
	shouldCreateDestQueue := (queueName == "src-traversal" && syncedLevel == 2 &&
		qp.Conductor != nil && !dstExists)

	qp.Mutex.Unlock()

	logging.GlobalLogger.Log(
		"info", "System", "QP", "Advancing to next phase", map[string]any{
			"queue": queueName,
			"phase": queue.Phase,
			"level": syncedLevel,
		}, "PHASE_ADVANCED", logging.QueueAcronyms[queueName],
	)

	// ✅ Create destination queue exactly when source reaches level 2
	if shouldCreateDestQueue {
		qp.createDestinationQueueNow()
	}

	// Publish tasks for the new phase
	go qp.PublishTasks(queueName)
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
	typeFilter := ""

	if queue.Type == UploadQueueType {
		table = "destination_nodes"
		statusColumn = "upload_status"
		typeFilter = " AND type = 'file'" // Upload queues only process files
	} else if queue.Type == TraversalQueueType && queue.SrcOrDst == "dst" {
		table = "destination_nodes"
		statusColumn = "traversal_status"
		typeFilter = " AND type = 'folder'" // Dst traversal only processes folders
	} else if queue.Type == TraversalQueueType {
		// Src traversal only processes folders
		typeFilter = " AND type = 'folder'"
	}

	// Make the query consistent with the actual fetch query
	query := `SELECT COUNT(*) FROM ` + table + ` WHERE ` + statusColumn + ` = 'pending' AND level = ?` + typeFilter

	logging.GlobalLogger.Log(
		"debug", "System", "QP", "hasPendingAtLevel query", map[string]any{
			"queue":      queueName,
			"level":      level,
			"table":      table,
			"query":      query,
			"typeFilter": typeFilter,
		}, "HAS_PENDING_AT_LEVEL_QUERY", logging.QueueAcronyms[queueName],
	)

	rows, err := qp.DB.Query(table, query, level)
	if err != nil {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "hasPendingAtLevel query failed", map[string]any{
				"queue": queueName,
				"level": level,
				"error": err.Error(),
				"query": query,
			}, "ERROR", logging.QueueAcronyms[queueName],
		)
		// Be conservative: assume pending exists so we don't advance too early
		return true
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			logging.GlobalLogger.Log(
				"error", "System", "QP", "hasPendingAtLevel scan failed", map[string]any{
					"queue": queueName,
					"level": level,
					"error": err.Error(),
				}, "ERROR", logging.QueueAcronyms[queueName],
			)
			return true
		}
	}

	logging.GlobalLogger.Log(
		"info", "System", "QP", "hasPendingAtLevel result", map[string]any{
			"queue":    queueName,
			"level":    level,
			"pending":  count,
			"table":    table,
			"hasItems": count > 0,
		}, "HAS_PENDING_AT_LEVEL_RESULT", logging.QueueAcronyms[queueName],
	)
	return count > 0
}

func (qp *QueuePublisher) FetchTasksFromDB(table string, queueType QueueType, currentLevel int, queueName string) []Task {
	logging.GlobalLogger.Log(
		"info", "System", "QP", "Fetching tasks from DB", map[string]any{
			"table":        table,
			"queueType":    queueType,
			"currentLevel": currentLevel,
		}, "FETCH_TASKS_FROM_DB", logging.QueueAcronyms[queueName],
	)

	// Simplified query logic using TrackedPaths filtering
	statusColumn := "traversal_status"
	retryColumn := "traversal_attempts"

	if queueType == UploadQueueType {
		statusColumn = "upload_status"
		retryColumn = "upload_attempts"
	}

	// Determine filtering type
	onlyFolders := queueType == TraversalQueueType
	onlyFiles := queueType == UploadQueueType

	// Simplified query that combines pending and failed (below retry threshold) items
	query := `SELECT path, name, identifier, parent_id, type, last_modified
	          FROM ` + table + ` WHERE 
	          (` + statusColumn + ` = 'pending' OR (` + statusColumn + ` = 'failed' AND ` + retryColumn + ` < ?))
	          AND level = ?`

	params := []any{qp.RetryThreshold, currentLevel}

	// Add TrackedPaths filtering to prevent re-processing active tasks
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()

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

			logging.GlobalLogger.Log(
				"debug", "System", "QP", "TrackedPaths filtering applied", map[string]any{
					"queueName":     queueName,
					"excludedPaths": len(pathList),
				}, "TRACKED_PATHS_FILTERING_APPLIED", logging.QueueAcronyms[queueName],
			)
		}
	}

	if onlyFolders {
		query += ` AND type = 'folder'`
	} else if onlyFiles {
		query += ` AND type = 'file'`
	}

	query += ` ORDER BY path LIMIT ?`
	params = append(params, qp.BatchSize)

	results := qp.runTaskQuery(table, query, params, currentLevel, queueName)
	return results
}

// runTaskQuery executes the query and returns a list of tasks.
func (qp *QueuePublisher) runTaskQuery(table, query string, params []any, currentLevel int, queueName string) []Task {
	rows, err := qp.DB.Query(table, query, params...)
	if err != nil {
		logging.GlobalLogger.Log(
			"error", "System", "QP", "DB query failed", map[string]any{
				"table": table,
				"err":   err.Error(),
			}, "ERROR", logging.QueueAcronyms[queueName],
		)
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
			logging.GlobalLogger.Log(
				"error", "System", "QP", "Failed to scan row", map[string]any{
					"err": err.Error(),
				}, "ERROR", logging.QueueAcronyms[queueName],
			)
			continue
		}

		if nodeType == "folder" {
			folder := &filesystem.Folder{
				Name:         name,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        currentLevel,
			}
			task, err := NewSrcTraversalTask(identifier, folder, &folder.ParentID)
			if err == nil {
				tasks = append(tasks, task)
			}
		}
	}

	return tasks
}

// Stop stops QP operations.
func (qp *QueuePublisher) Stop() {
	logging.GlobalLogger.Log(
		"info", "System", "QP", "Stopping QueuePublisher main loop", map[string]any{},
		"STOPPING_QUEUE_PUBLISHER_MAIN_LOOP", "All",
	)
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
		logging.GlobalLogger.Log(
			"error", "System", "QP", "Failed to debug pending folders", map[string]any{
				"error": err.Error(),
			}, "ERROR", "All",
		)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var path, status, nodeType string
		var attempts, level int
		if err := rows.Scan(&path, &status, &attempts, &level, &nodeType); err != nil {
			continue
		}

		logging.GlobalLogger.Log(
			"debug", "System", "QP", "Pending folder details", map[string]any{
				"path":     path,
				"status":   status,
				"attempts": attempts,
				"level":    level,
				"type":     nodeType,
			}, "PENDING_FOLDER_DETAILS", "All",
		)
	}
}
