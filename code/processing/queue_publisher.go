package processing

import (
	"fmt"
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

// ConductorInterface defines the methods QP needs from Conductor
type ConductorInterface interface {
	SetupDestinationQueue()
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
	LastDBPaths     map[string]string
	RetryThreshold  int
	BatchSize       int
	QueriesPerPhase map[string]int      // queueName -> queryCount
	ScanModes       map[string]scanMode // queueName -> scanMode
	// RootPaths stores the root folder path for each queue (for path normalization)
	RootPaths map[string]string // queueName -> rootPath
	// Conductor reference for dynamic queue creation
	Conductor ConductorInterface
}

func NewQueuePublisher(db *db.DB, retryThreshold, batchSize int) *QueuePublisher {
	logging.GlobalLogger.LogSystem("info", "QP", "Initializing new QueuePublisher", map[string]any{
		"retryThreshold": retryThreshold,
		"batchSize":      batchSize,
	})

	return &QueuePublisher{
		DB:              db,
		Queues:          make(map[string]*TaskQueue),
		QueueLevels:     make(map[string]int),
		Running:         false,
		LastPathCursors: make(map[string]string),
		LastDBPaths:     make(map[string]string),
		RetryThreshold:  retryThreshold,
		BatchSize:       batchSize,
		QueriesPerPhase: make(map[string]int),
		ScanModes:       make(map[string]scanMode),
		RootPaths:       make(map[string]string),
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

func (qp *QueuePublisher) Run() {
	logging.GlobalLogger.LogSystem("info", "QP", "Starting QueuePublisher main control loop", nil)
	qp.Running = true

	// Main control loop - proactive task management
	for qp.Running {
		qp.manageAllQueues()
		time.Sleep(100 * time.Millisecond)
	}
}

// manageAllQueues handles all queue management in one place - task publishing and phase advancement
func (qp *QueuePublisher) manageAllQueues() {
	// Create a snapshot of queue data while holding the mutex, then release it before spawning goroutines
	qp.Mutex.Lock()
	type queueSnapshot struct {
		name              string
		queue             *TaskQueue
		needsTaskPublish  bool
		srcLevel          int
		isRoundComplete   bool
		traversalComplete bool
	}

	var snapshots []queueSnapshot
	srcLevel := qp.QueueLevels["src-traversal"] // Cache src level for dst decisions

	for queueName, queue := range qp.Queues {
		if queue.State != QueueRunning {
			continue
		}

		snapshot := queueSnapshot{
			name:             queueName,
			queue:            queue,
			needsTaskPublish: queue.QueueSize() < queue.RunningLowThreshold,
			srcLevel:         srcLevel,
			isRoundComplete:  qp.isRoundComplete(queue),
		}

		// Check traversal completion while we have the mutex
		if snapshot.isRoundComplete {
			currentLevel := qp.QueueLevels[queueName]
			queriesPerPhase := qp.QueriesPerPhase[queueName]
			snapshot.traversalComplete = qp.checkTraversalCompleteWithData(queueName, 0, queue, currentLevel, queriesPerPhase)
		}

		snapshots = append(snapshots, snapshot)
	}
	qp.Mutex.Unlock() // Release mutex before spawning any goroutines

	// Now process the snapshots without holding the mutex
	for _, snapshot := range snapshots {
		// Proactively publish tasks if queue is running low
		if snapshot.needsTaskPublish {
			go qp.PublishTasks(snapshot.name) // Safe - no mutex held
		}

		// Handle source phase advancement (runs independently)
		if snapshot.queue.SrcOrDst == "src" && snapshot.isRoundComplete {
			if snapshot.traversalComplete {
				// Mark queue as complete for Conductor to detect
				qp.Mutex.Lock()
				snapshot.queue.State = QueueComplete
				qp.Mutex.Unlock()
				logging.GlobalLogger.LogSystem("info", "QP", "Source traversal complete", map[string]any{
					"queue": snapshot.name,
				})
			} else {
				qp.advancePhase(snapshot.name)
				// Check if we should create destination queue after source advancement
				qp.checkAndCreateDestinationQueue()
			}
		}

		// Handle destination advancement (only if dst queue exists)
		if snapshot.queue.SrcOrDst == "dst" {
			qp.handleDestinationQueue(snapshot.name, snapshot.queue)
		}
	}

	// Also check for destination queue creation on each iteration (in case source just advanced)
	qp.checkAndCreateDestinationQueue()
}

// checkAndCreateDestinationQueue checks if destination queue should be created when source advances
func (qp *QueuePublisher) checkAndCreateDestinationQueue() {
	qp.Mutex.Lock()
	srcLevel, srcExists := qp.QueueLevels["src-traversal"]
	_, dstExists := qp.QueueLevels["dst-traversal"]
	qp.Mutex.Unlock()

	// Create destination queue when source reaches level 1 (has completed level 0)
	if srcExists && !dstExists && srcLevel >= 1 && qp.Conductor != nil {
		logging.GlobalLogger.LogSystem("info", "QP", "Source reached level 1, creating destination queue", map[string]any{
			"srcLevel": srcLevel,
		})

		// Let Conductor create the destination queue and workers
		qp.Conductor.SetupDestinationQueue()

		// Initialize destination queue state in QP
		qp.Mutex.Lock()
		qp.QueueLevels["dst-traversal"] = 0
		qp.ScanModes["dst-traversal"] = firstPass
		qp.LastPathCursors["dst-traversal"] = ""
		qp.QueriesPerPhase["dst-traversal"] = 0
		qp.Mutex.Unlock()

		// Start destination task publishing
		go qp.PublishTasks("dst-traversal")
	}
}

// handleDestinationQueue manages destination queue advancement (only called if dst queue exists)
func (qp *QueuePublisher) handleDestinationQueue(queueName string, queue *TaskQueue) {
	// Check for destination phase advancement
	if qp.isRoundComplete(queue) {
		if qp.checkTraversalComplete(queueName, 0) {
			// Mark queue as complete for Conductor to detect
			qp.Mutex.Lock()
			queue.State = QueueComplete
			qp.Mutex.Unlock()
			logging.GlobalLogger.LogSystem("info", "QP", "Destination traversal complete", map[string]any{
				"queue": queueName,
			})
		} else {
			qp.tryAdvanceDestination(queueName)
		}
	}
}

// tryAdvanceDestination advances destination only if source is ahead enough
func (qp *QueuePublisher) tryAdvanceDestination(queueName string) {
	qp.Mutex.Lock()
	srcLevel, srcExists := qp.QueueLevels["src-traversal"]
	dstLevel, dstExists := qp.QueueLevels["dst-traversal"]
	qp.Mutex.Unlock()

	// Destination can advance to level N only if source is on level N+1 or greater
	if srcExists && dstExists && srcLevel > dstLevel {
		logging.GlobalLogger.LogSystem("info", "QP", "Advancing destination phase", map[string]any{
			"srcLevel": srcLevel,
			"dstLevel": dstLevel,
		})
		qp.advancePhase(queueName)
	} else {
		logging.GlobalLogger.LogSystem("debug", "QP", "Destination cannot advance - waiting for source", map[string]any{
			"srcLevel": srcLevel,
			"dstLevel": dstLevel,
		})
	}
}

// PublishDestinationTasks creates destination traversal tasks by querying both source and destination tables
func (qp *QueuePublisher) PublishDestinationTasks(queueName string) {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	currentLevel := qp.QueueLevels[queueName]
	mode := qp.ScanModes[queueName]
	var lastPath string
	if mode == firstPass {
		lastPath = qp.LastPathCursors[queueName]
	}
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

	// Handle level 0 specially - create root task from destination_nodes table
	if currentLevel == 0 && mode == firstPass && lastPath == "" {
		tasks, err := qp.createRootTask(queueName, "destination_nodes")
		if err != nil {
			logging.GlobalLogger.LogQP("error", queueName, "", "Failed to create destination root task", map[string]any{
				"error": err.Error(),
			})
			return
		}

		if len(tasks) > 0 {
			queue.AddTasks(tasks)
			qp.QueriesPerPhase[queueName]++

			qp.Mutex.Lock()
			qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath() // Set to "/" to mark root as processed
			qp.Mutex.Unlock()

			logging.GlobalLogger.LogQP("info", queueName, "", "Created destination root task", map[string]any{
				"taskCount": len(tasks),
				"level":     currentLevel,
				"cursor":    tasks[len(tasks)-1].GetPath(),
			})
		}
		return
	}

	// Force flush both tables before cross-table queries to ensure data consistency
	qp.DB.ForceFlushTable("source_nodes")      // Need to see source children
	qp.DB.ForceFlushTable("destination_nodes") // Need to see destination folders

	// Step 1: Query destination table for pending folders at current level (level 1+)
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

		switch nodeType {
		case "folder":
			folder := &filesystem.Folder{
				Name:         name,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        0, // Will be set properly when used
			}
			expectedFolders = append(expectedFolders, folder)
		case "file":
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

// discoverAndSetRootPath finds the root folder from the nodes table (level 0) and stores it
func (qp *QueuePublisher) discoverAndSetRootPath(queueName string, table string) error {
	qp.Mutex.Lock()
	// Check if root path is already discovered for this queue
	if rootPath, exists := qp.RootPaths[queueName]; exists {
		qp.Mutex.Unlock()
		logging.GlobalLogger.LogSystem("debug", "QP", "Root path already cached for queue", map[string]any{
			"queue":    queueName,
			"rootPath": rootPath,
		})
		return nil
	}
	qp.Mutex.Unlock()

	// Query the nodes table for level 0 entry (use identifier for absolute path)
	query := `SELECT identifier FROM ` + table + ` WHERE level = 0 LIMIT 1`
	logging.GlobalLogger.LogSystem("debug", "QP", "Querying nodes table for root", map[string]any{
		"query": query,
		"table": table,
		"queue": queueName,
	})

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
		logging.GlobalLogger.LogSystem("debug", "QP", "Found root path", map[string]any{
			"rootPath": rootPath,
			"table":    table,
			"queue":    queueName,
		})
	} else {
		logging.GlobalLogger.LogSystem("error", "QP", "No root found in table", map[string]any{
			"table": table,
			"queue": queueName,
		})
		return fmt.Errorf("no root found in table %s", table)
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

// createRootTask creates the initial root task from the nodes table for level 0
func (qp *QueuePublisher) createRootTask(queueName string, table string) ([]Task, error) {
	logging.GlobalLogger.LogSystem("debug", "QP", "Creating root task", map[string]any{
		"queueName": queueName,
		"table":     table,
	})

	// Query the nodes table for level 0 root information
	query := `SELECT path, name, identifier, last_modified FROM ` + table + ` WHERE level = 0 LIMIT 1`
	logging.GlobalLogger.LogSystem("debug", "QP", "Querying nodes table for root task creation", map[string]any{
		"query": query,
		"table": table,
	})

	rows, err := qp.DB.Query(table, query)
	if err != nil {
		logging.GlobalLogger.LogSystem("error", "QP", "Failed to query nodes table", map[string]any{
			"error": err.Error(),
			"table": table,
			"queue": queueName,
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

	return nil, fmt.Errorf("no root found in table %s", table)
}

func (qp *QueuePublisher) PublishTasks(queueName string) {
	logging.GlobalLogger.LogQP("debug", queueName, "", "PublishTasks called", nil)

	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	qp.Mutex.Unlock()
	if !exists {
		logging.GlobalLogger.LogQP("error", queueName, "", "Queue not found", nil)
		return
	}

	logging.GlobalLogger.LogQP("debug", queueName, "", "Queue exists, checking type", map[string]any{
		"queueType": queue.Type,
		"srcOrDst":  queue.SrcOrDst,
	})

	// Destination traversal uses special cross-table logic
	if queue.Type == TraversalQueueType && queue.SrcOrDst == "dst" {
		logging.GlobalLogger.LogQP("debug", queueName, "", "Routing to PublishDestinationTasks", nil)
		qp.PublishDestinationTasks(queueName)
		return
	}

	// Determine the correct table based on queue type and src/dst designation
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	}

	logging.GlobalLogger.LogQP("debug", queueName, "", "Calling discoverAndSetRootPath", map[string]any{
		"table": table,
	})

	// Discover and set root path for this queue if not already done
	if err := qp.discoverAndSetRootPath(queueName, table); err != nil {
		logging.GlobalLogger.LogQP("error", queueName, "", "Failed to discover root path", map[string]any{
			"error": err.Error(),
		})
		return
	}

	logging.GlobalLogger.LogQP("debug", queueName, "", "Root path discovery completed", nil)

	qp.Mutex.Lock()
	mode := qp.ScanModes[queueName]
	currentLevel := qp.QueueLevels[queueName]
	var lastPath string
	if mode == firstPass {
		lastPath = qp.LastPathCursors[queueName]
	}
	qp.Mutex.Unlock()

	logging.GlobalLogger.LogQP("debug", queueName, "", "Checking root task creation conditions", map[string]any{
		"currentLevel": currentLevel,
		"mode":         mode,
		"lastPath":     lastPath,
		"condition":    currentLevel == 0 && mode == firstPass && lastPath == "",
	})

	var tasks []Task
	var err error

	// Handle level 0 specially - create root task from root table
	if currentLevel == 0 && mode == firstPass && lastPath == "" {
		logging.GlobalLogger.LogQP("debug", queueName, "", "Creating root task", map[string]any{
			"currentLevel": currentLevel,
			"mode":         mode,
			"lastPath":     lastPath,
			"table":        table,
		})
		tasks, err = qp.createRootTask(queueName, table)
		if err != nil {
			logging.GlobalLogger.LogQP("error", queueName, "", "Failed to create root task", map[string]any{
				"error": err.Error(),
			})
			return
		}
	} else {
		logging.GlobalLogger.LogQP("debug", queueName, "", "Fetching tasks from DB", map[string]any{
			"currentLevel": currentLevel,
			"mode":         mode,
			"lastPath":     lastPath,
			"table":        table,
		})
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
		qp.Mutex.Lock()
		qp.QueriesPerPhase[queueName]++
		if mode == firstPass {
			if lastRaw, ok := qp.LastDBPaths[queueName]; ok && lastRaw != "" {
				qp.LastPathCursors[queueName] = lastRaw
			} else {
				// Fallback to task path if we don't have a recorded raw DB path yet
				qp.LastPathCursors[queueName] = tasks[len(tasks)-1].GetPath()
			}
		}
		qp.Mutex.Unlock()

		// Switch to retry mode if we got a short batch
		if len(tasks) < qp.BatchSize && mode == firstPass {
			qp.Mutex.Lock()
			qp.ScanModes[queueName] = retryPass
			qp.Mutex.Unlock()
		}
	} else {
		logging.GlobalLogger.LogQP("debug", queueName, "", "No tasks fetched from DB", map[string]any{
			"scanMode":     mode,
			"level":        currentLevel,
			"lastSeenPath": lastPath,
		})

		// Switch to retry mode if we found no tasks in first pass
		if mode == firstPass {
			qp.Mutex.Lock()
			qp.ScanModes[queueName] = retryPass
			qp.Mutex.Unlock()
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
	return queue.QueueSize() == 0 && queue.AreAllWorkersIdle() && queue.State == QueueRunning && !qp.hasPendingAtLevel(queue.QueueID, queue.Phase)
}

func (qp *QueuePublisher) advancePhase(queueName string) {
	qp.Mutex.Lock()
	queue := qp.Queues[queueName]

	// Advance the phase
	queue.Phase++
	qp.QueueLevels[queueName] = queue.Phase
	qp.LastPathCursors[queueName] = ""
	qp.ScanModes[queueName] = firstPass // Ensure new phase starts with firstPass
	qp.QueriesPerPhase[queueName] = 0
	syncedLevel := qp.QueueLevels[queueName]
	qp.Mutex.Unlock()

	logging.GlobalLogger.LogMessage("info", "Advancing to next phase", map[string]any{
		"queue": queueName,
		"phase": queue.Phase,
		"level": syncedLevel,
	})

	// Publish tasks for the new phase
	go qp.PublishTasks(queueName)
}

// checkTraversalComplete - mutex-acquiring version for external callers
func (qp *QueuePublisher) checkTraversalComplete(queueName string, queryResultSize int) bool {
	qp.Mutex.Lock()
	queue, exists := qp.Queues[queueName]
	if !exists {
		qp.Mutex.Unlock()
		return false
	}
	currentLevel := qp.QueueLevels[queueName]
	queriesPerPhase := qp.QueriesPerPhase[queueName]
	qp.Mutex.Unlock()

	return qp.checkTraversalCompleteWithData(queueName, queryResultSize, queue, currentLevel, queriesPerPhase)
}

// checkTraversalCompleteWithData - mutex-free version for internal use when data is already available
func (qp *QueuePublisher) checkTraversalCompleteWithData(queueName string, queryResultSize int, queue *TaskQueue, currentLevel int, queriesPerPhase int) bool {
	logging.GlobalLogger.LogMessage("info", "Checking traversal completion", map[string]any{
		"queue":           queueName,
		"queryResultSize": queryResultSize,
		"queriesPerPhase": queriesPerPhase,
		"currentLevel":    currentLevel,
	})

	// Only check for completion if we didn't get any tasks this round
	// Allow completion check regardless of scan mode when advancing phases
	if queryResultSize == 0 && queriesPerPhase <= 0 {
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
	scanMode := qp.ScanModes[queueName]
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

	if scanMode == firstPass {
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
		"scanMode": scanMode,
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

// Stop stops QP operations.
func (qp *QueuePublisher) Stop() {
	logging.GlobalLogger.LogMessage("info", "Stopping QueuePublisher main loop", nil)
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
