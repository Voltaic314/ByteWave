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
	// DstCanAdvance is the simple red-light/green-light flag for destination
	DstCanAdvance  bool
	DstTargetLevel int // What level destination should advance to when green-lit
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
		DstCanAdvance:      false,                   // Start with red light
		DstTargetLevel:     0,
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
		if srcQueue, exists := qp.Queues[srcQueueName]; exists {
			srcQueue.Phase = level // Keep queue.Phase in sync
		}
		qp.ScanModes[srcQueueName] = firstPass

		// Simple red-light/green-light logic for destination
		qp.updateDestinationGreenLight(level)

		qp.Mutex.Unlock()
		qp.PublishTasks(srcQueueName)

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

	// Destination phase updates are now handled by the simple green-light logic
	// No complex signal handling needed

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
		if queue, exists := qp.Queues[queueName]; exists {
			queue.Phase = phase // Keep queue.Phase in sync
		}
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

// updateDestinationGreenLight sets the destination green light based on source progress
func (qp *QueuePublisher) updateDestinationGreenLight(srcLevel int) {
	dstQueueName := "dst-traversal"

	// Check if destination queue exists
	_, dstExists := qp.Queues[dstQueueName]
	if !dstExists {
		return // No destination queue to coordinate
	}

	currentDstLevel, dstLevelExists := qp.QueueLevels[dstQueueName]

	// Start destination when source reaches level 1 (completed level 0)
	if !dstLevelExists && srcLevel >= 1 {
		logging.GlobalLogger.LogSystem("info", "QP", "Green light: Starting destination", map[string]any{
			"srcLevel": srcLevel,
		})
		qp.DstCanAdvance = true
		qp.DstTargetLevel = 0
		return
	}

	// Allow destination to advance if source is N+2 ahead
	if dstLevelExists && srcLevel >= currentDstLevel+2 {
		logging.GlobalLogger.LogSystem("info", "QP", "Green light: Destination can advance", map[string]any{
			"srcLevel":    srcLevel,
			"currentDst":  currentDstLevel,
			"targetLevel": currentDstLevel + 1,
		})
		qp.DstCanAdvance = true
		qp.DstTargetLevel = currentDstLevel + 1
		return
	}

	// Otherwise, red light
	qp.DstCanAdvance = false
}

// StartDestinationPolling starts the destination polling loop so it's ready to respond to green lights
func (qp *QueuePublisher) StartDestinationPolling() {
	dstQueueName := "dst-traversal"

	// Check if destination queue exists
	qp.Mutex.Lock()
	_, exists := qp.Queues[dstQueueName]
	qp.Mutex.Unlock()

	if !exists {
		logging.GlobalLogger.LogSystem("warning", "QP", "Cannot start destination polling - queue doesn't exist", nil)
		return
	}

	// Start polling for destination so it can respond to green lights
	qp.startPolling(dstQueueName)

	logging.GlobalLogger.LogSystem("info", "QP", "Destination polling started - ready for green lights", nil)
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

	// Force flush both tables before cross-table queries to ensure data consistency
	qp.DB.ForceFlushTable("source_nodes")      // Need to see source children
	qp.DB.ForceFlushTable("destination_nodes") // Need to see destination folders

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

		// Create destination traversal task with expected source children (use identifier as task ID)
		dstTask, err := NewDstTraversalTask(dstFolder.Identifier, dstFolder, nil, expectedSrcChildren, expectedSrcFiles)
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
	// Convert destination path to equivalent source path for cross-table join
	srcPath := qp.convertPathBetweenRoots(dstPath, "dst-traversal", "src-traversal")

	// Query source table for children of this equivalent path
	query := `SELECT path, name, identifier, parent_id, type, last_modified, size
	          FROM source_nodes 
	          WHERE parent_id = ?
	          ORDER BY type, name`

	rows, err := qp.DB.Query("source_nodes", query, srcPath)
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

// discoverAndSetRootPath finds the root folder from the dedicated root table and stores it
func (qp *QueuePublisher) discoverAndSetRootPath(queueName string, table string) error {
	qp.Mutex.Lock()
	// Check if root path is already discovered for this queue
	if _, exists := qp.RootPaths[queueName]; exists {
		qp.Mutex.Unlock()
		return nil
	}
	qp.Mutex.Unlock()

	// Determine the root table based on the node table
	var rootTable string
	if table == "source_nodes" {
		rootTable = "source_root"
	} else if table == "destination_nodes" {
		rootTable = "destination_root"
	} else {
		return fmt.Errorf("unknown table %s for root discovery", table)
	}

	// Query the dedicated root table
	query := `SELECT path FROM ` + rootTable + ` LIMIT 1`
	rows, err := qp.DB.Query(rootTable, query)
	if err != nil {
		logging.GlobalLogger.LogSystem("error", "QP", "Failed to discover root path", map[string]any{
			"error":     err.Error(),
			"rootTable": rootTable,
			"queue":     queueName,
		})
		return err
	}
	defer rows.Close()

	var rootPath string
	if rows.Next() {
		if err := rows.Scan(&rootPath); err != nil {
			logging.GlobalLogger.LogSystem("error", "QP", "Failed to scan root path", map[string]any{
				"error":     err.Error(),
				"rootTable": rootTable,
				"queue":     queueName,
			})
			return err
		}
	} else {
		return fmt.Errorf("no root found in table %s", rootTable)
	}

	// Store the discovered root path
	qp.Mutex.Lock()
	qp.RootPaths[queueName] = rootPath
	qp.Mutex.Unlock()

	logging.GlobalLogger.LogSystem("info", "QP", "Discovered root path for queue", map[string]any{
		"queue":     queueName,
		"rootPath":  rootPath,
		"rootTable": rootTable,
	})

	return nil
}

// normalizePathForJoin converts paths from OS service to database format
// In the new model, OS service provides absolute paths via identifiers,
// but we store relative paths in the database starting with "/"
func (qp *QueuePublisher) normalizePathForJoin(queueName, pathFromOSService string) string {
	qp.Mutex.Lock()
	rootPath, exists := qp.RootPaths[queueName]
	qp.Mutex.Unlock()

	if !exists || rootPath == "" {
		// If no root known, assume it's already relative
		if !strings.HasPrefix(pathFromOSService, "/") {
			return "/" + pathFromOSService
		}
		return pathFromOSService
	}

	// If this is the root path itself, return "/"
	if pathFromOSService == rootPath {
		return "/"
	}

	// Remove the root prefix and ensure it starts with "/"
	if strings.HasPrefix(pathFromOSService, rootPath) {
		relativePath := strings.TrimPrefix(pathFromOSService, rootPath)
		// Handle both / and \ path separators
		relativePath = strings.TrimPrefix(relativePath, "/")
		relativePath = strings.TrimPrefix(relativePath, "\\")
		if relativePath == "" {
			return "/"
		}
		return "/" + strings.ReplaceAll(relativePath, "\\", "/")
	}

	// If path doesn't match root, assume it's already relative
	if !strings.HasPrefix(pathFromOSService, "/") {
		return "/" + pathFromOSService
	}
	return pathFromOSService
}

// convertPathBetweenRoots converts a path from one queue's root system to another queue's root system
func (qp *QueuePublisher) convertPathBetweenRoots(fromPath, fromQueueName, toQueueName string) string {
	qp.Mutex.Lock()
	fromRoot, fromExists := qp.RootPaths[fromQueueName]
	toRoot, toExists := qp.RootPaths[toQueueName]
	qp.Mutex.Unlock()

	if !fromExists || !toExists {
		// Fallback: assume paths are equivalent if roots not known
		return fromPath
	}

	// Extract the relative portion from the fromPath
	var relativePart string
	if fromPath == fromRoot {
		// This is the root itself
		relativePart = ""
	} else if strings.HasPrefix(fromPath, fromRoot) {
		// Extract relative portion
		relativePart = strings.TrimPrefix(fromPath, fromRoot)
		// Remove leading slash if present
		relativePart = strings.TrimPrefix(relativePart, "/")
		relativePart = strings.TrimPrefix(relativePart, "\\")
	} else {
		// Path doesn't match expected root, return as-is
		return fromPath
	}

	// Construct equivalent path in target root system
	if relativePart == "" {
		// This maps to the target root
		return toRoot
	}

	// Combine target root with relative part
	return filepath.Join(toRoot, relativePart)
}

// createRootTask creates the initial root task from the root table for level 0
func (qp *QueuePublisher) createRootTask(queueName string, table string) ([]Task, error) {
	// Determine the root table based on the node table
	var rootTable string
	if table == "source_nodes" {
		rootTable = "source_root"
	} else if table == "destination_nodes" {
		rootTable = "destination_root"
	} else {
		return nil, fmt.Errorf("unknown table %s for root task creation", table)
	}

	// Query the root table for root information
	query := `SELECT path, name, identifier, last_modified FROM ` + rootTable + ` LIMIT 1`
	rows, err := qp.DB.Query(rootTable, query)
	if err != nil {
		logging.GlobalLogger.LogSystem("error", "QP", "Failed to query root table", map[string]any{
			"error":     err.Error(),
			"rootTable": rootTable,
			"queue":     queueName,
		})
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var rootPath, name, identifier, lastModified string
		if err := rows.Scan(&rootPath, &name, &identifier, &lastModified); err != nil {
			logging.GlobalLogger.LogSystem("error", "QP", "Failed to scan root data", map[string]any{
				"error": err.Error(),
				"queue": queueName,
			})
			return nil, err
		}

		// Create root task with path="/" and absolute identifier for OS service
		folder := &filesystem.Folder{
			Name:         name,
			Path:         "/",        // Always "/" for root in the new model
			ParentID:     "",         // Root has no parent
			Identifier:   identifier, // Keep absolute path for OS service
			LastModified: lastModified,
			Level:        0,
		}

		var task Task
		var err error

		// Create the appropriate task type based on the table
		if table == "destination_nodes" {
			// For destination root task, create with empty expected children/files
			task, err = NewDstTraversalTask(identifier, folder, nil, nil, nil)
		} else {
			// For source root task
			task, err = NewSrcTraversalTask(identifier, folder, nil)
		}

		if err != nil {
			return nil, err
		}

		logging.GlobalLogger.LogSystem("info", "QP", "Created root task", map[string]any{
			"queue":      queueName,
			"path":       "/",
			"identifier": identifier,
			"taskType":   task.GetType(),
		})

		return []Task{task}, nil
	}

	return nil, fmt.Errorf("no root found in table %s", rootTable)
}

func (qp *QueuePublisher) PublishTasks(queueName string) {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()
	if !exists {
		logging.GlobalLogger.LogQP("error", queueName, "", "Queue not found", nil)
		return
	}

	// Determine the correct table based on queue type and src/dst designation
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	} else if queue.Type == TraversalQueueType && queue.SrcOrDst == "dst" {
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

	var tasks []Task
	var err error

	// Handle level 0 specially - create root task from root table
	if currentLevel == 0 && mode == firstPass && lastPath == "" {
		tasks, err = qp.createRootTask(queueName, table)
		if err != nil {
			logging.GlobalLogger.LogQP("error", queueName, "", "Failed to create root task", map[string]any{
				"error": err.Error(),
			})
			return
		}
	} else {
		// Normal task fetching from node tables (level 1+)
		tasks = qp.FetchTasksFromDB(table, queue.Type, currentLevel, lastPath, queueName)
	}

	if len(tasks) > 0 {
		logging.GlobalLogger.LogQP("debug", queueName, "", "Tasks fetched from DB", map[string]any{
			"taskCount":    len(tasks),
			"scanMode":     mode,
			"level":        currentLevel,
			"lastSeenPath": lastPath,
		})

		queue.AddTasks(tasks)
		// 🆕 NEW: Reset idle trigger when new tasks are added
		queue.ResetIdleTrigger()
		qp.QueriesPerPhase[queueName]++
		if mode == firstPass {
			if lastRaw, ok := qp.LastDBPaths[queueName]; ok && lastRaw != "" {
				qp.LastPathCursors[queueName] = lastRaw
			} else {
				// Fallback to task path if we don't have a recorded raw DB path yet
				qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath()
			}
		}

		// Start polling if short batch
		if len(tasks) < qp.BatchSize {
			qp.startPolling(queueName)
			// Switch to retry mode only if we were already in firstPass and got less than full batch
			// Don't switch if we just advanced phases and are starting fresh
			if mode == firstPass {
				qp.ScanModes[queueName] = retryPass
			}
		}
	} else {
		logging.GlobalLogger.LogQP("debug", queueName, "", "No tasks fetched from DB", map[string]any{
			"scanMode":     mode,
			"level":        currentLevel,
			"lastSeenPath": lastPath,
		})

		// Always start polling if no tasks
		qp.startPolling(queueName)
		// Switch to retry mode only if we were already in firstPass and found no tasks
		// Don't switch if we just advanced phases and are starting fresh
		if mode == firstPass {
			qp.ScanModes[queueName] = retryPass
		}
	}
}

func (qp *QueuePublisher) isRoundComplete(queue *TaskQueue) bool {
	logging.GlobalLogger.LogMessage("info", "Checking round completion", map[string]any{
		"queueID":        queue.QueueID,
		"queueSize":      queue.QueueSize(),
		"allWorkersIdle": queue.AreAllWorkersIdle(),
		"state":          queue.State,
	})
	if queue.Type == TraversalQueueType && queue.SrcOrDst == "src" {
		qp.DB.ForceFlushTable("source_nodes")
	} else if queue.Type == UploadQueueType && queue.SrcOrDst == "dst" {
		qp.DB.ForceFlushTable("destination_nodes")
		qp.DB.ForceFlushTable("source_nodes")
	}
	return queue.QueueSize() == 0 && queue.AreAllWorkersIdle() && queue.State == QueueRunning
}

func (qp *QueuePublisher) advancePhase(queueName string) {
	queue := qp.Queues[queueName]

	// For destination queues, check the simple green light flag
	if queue.SrcOrDst == "dst" {
		if !qp.DstCanAdvance {
			logging.GlobalLogger.LogMessage("info", "Destination red light - cannot advance", map[string]any{
				"queue":        queueName,
				"currentPhase": queue.Phase,
				"canAdvance":   qp.DstCanAdvance,
				"targetLevel":  qp.DstTargetLevel,
			})
			return // Red light - don't advance destination yet
		}

		// Check if this is the initial startup (target level 0)
		if qp.DstTargetLevel == 0 && queue.Phase == 0 {
			// Initialize destination for first time
			qp.QueueLevels[queueName] = 0
			qp.ScanModes[queueName] = firstPass

			// Install handlers if needed
			needInstall := !qp.HandlersInstalled[queueName]
			if needInstall {
				qp.installQueueHandlers(queueName)
				qp.HandlersInstalled[queueName] = true
			}

			logging.GlobalLogger.LogMessage("info", "Destination initialized at level 0", map[string]any{
				"queue": queueName,
			})
			qp.DstCanAdvance = false // Turn back to red light

			// Publish initial tasks for level 0 immediately
			go qp.PublishTasks(queueName)
			return
		}

		// Check if we should advance to the target level
		if qp.DstTargetLevel != queue.Phase+1 {
			logging.GlobalLogger.LogMessage("info", "Destination target level mismatch", map[string]any{
				"queue":        queueName,
				"currentPhase": queue.Phase,
				"targetLevel":  qp.DstTargetLevel,
				"expectedNext": queue.Phase + 1,
			})
			return // Target level doesn't match expected next level
		}

		// Green light - advance and turn back to red
		logging.GlobalLogger.LogMessage("info", "Destination green light - advancing", map[string]any{
			"queue":        queueName,
			"currentPhase": queue.Phase,
			"targetLevel":  qp.DstTargetLevel,
		})
		qp.DstCanAdvance = false // Turn back to red light after advancing
	}

	// Advance the phase
	queue.Phase++
	qp.QueueLevels[queueName] = queue.Phase
	qp.LastPathCursors[queueName] = ""
	qp.ScanModes[queueName] = firstPass // Ensure new phase starts with firstPass
	qp.QueriesPerPhase[queueName] = 0

	logging.GlobalLogger.LogMessage("info", "Advancing to next phase", map[string]any{
		"queue":       queueName,
		"phase":       queue.Phase,
		"syncedLevel": qp.QueueLevels[queueName],
	})

	// Publish queue-specific phase update signal
	signals.GlobalSR.Publish(signals.Signal{
		Topic:   "qp:phase_updated:" + queueName,
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

	// Only check for completion if we didn't get any tasks this round
	// Allow completion check regardless of scan mode when advancing phases
	if queryResultSize == 0 && qp.QueriesPerPhase[queueName] <= 0 {
		// Check if there are any pending tasks at the current level in the database
		table := "source_nodes"
		if queue.Type == UploadQueueType {
			table = "destination_nodes"
		}

		statusColumn := "traversal_status"
		if queue.Type == UploadQueueType {
			statusColumn = "upload_status"
		}

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
					} else if queue.Type == TraversalQueueType && queue.SrcOrDst == "dst" {
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
	} else if queue.Type == TraversalQueueType && queue.SrcOrDst == "dst" {
		table = "destination_nodes"
		statusColumn = "traversal_status"
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

	// Force flush in retry mode to see latest failure states
	qp.Mutex.Lock()
	isRetryMode := qp.ScanModes[queueName] == retryPass
	qp.Mutex.Unlock()
	if isRetryMode {
		qp.DB.ForceFlushTable(table)
	}

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
		qp.Mutex.Unlock()
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
	}

	if onlyFolders {
		query += ` AND type = 'folder'`
	} else if onlyFiles {
		query += ` AND type = 'file'`
	}

	query += ` ORDER BY path LIMIT ?`
	params = append(params, qp.BatchSize)

	// Debug log the query
	logging.GlobalLogger.LogMessage("info", "Executing query", map[string]any{
		"query":    query,
		"params":   params,
		"scanMode": qp.ScanModes[queueName],
	})

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

		// Paths from DB are now consistently relative, just normalize slashes
		normalizedPath := filepath.ToSlash(path)

		// Record path for stable cursor updates (paths are already in correct relative format)
		qp.Mutex.Lock()
		qp.LastDBPaths[queueName] = normalizedPath
		qp.Mutex.Unlock()

		if nodeType == "folder" {
			folder := &filesystem.Folder{
				Name:         name,
				Path:         normalizedPath,
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
