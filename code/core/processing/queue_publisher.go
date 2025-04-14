package processing

import (
	"context"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
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
	Queues             map[string]*TaskQueue // Dynamic queue storage
	QueueLevels        map[string]int        // Tracks the level for each queue
	QueueBoardChans    map[string]chan int   // Signals from multiple queue boards (now carries level)
	PublishSignals     map[string]chan bool  // Signals to workers for multiple queues
	PhaseUpdated       chan int              // Signals when phase has changed
	Mutex              sync.Mutex
	Running            bool
	LastPathCursors    map[string]string   // Tracks last-seen path per queue for pagination
	RetryThreshold     int                 // Max retries before marking failure
	BatchSize          int                 // How many tasks to fetch per DB query
	RunningLowChans    map[string]chan int // ✅ Separate signal channels per queue
	PollingControllers map[string]*PollingController
}

// NewQueuePublisher initializes a multi-queue publisher.
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
	}
}

func (qp *QueuePublisher) StartListening() {
	core.GlobalLogger.LogMessage("info", "Starting QueuePublisher listening loop", nil)
	qp.Running = true

	for qp.Running {
		select {
		case level := <-qp.PhaseUpdated:
			core.GlobalLogger.LogMessage("info", "Phase updated", map[string]any{
				"newLevel": level,
			})
			for name, signalChan := range qp.QueueBoardChans {
				select {
				case level := <-signalChan:
					if qp.Queues[name].State == QueueRunning {
						qp.QueueLevels[name] = level
						core.GlobalLogger.LogMessage("debug", "Signal received, publishing tasks", map[string]any{
							"queueName": name,
							"level":     level,
						})
						qp.PublishTasks(name)
					}

					go func(queueName string, signalChan chan int) {
						for {
							phase, ok := <-signalChan
							if !ok {
								return // Exit goroutine if channel closes
							}

							qp.Mutex.Lock()
							queue, exists := qp.Queues[queueName]
							qp.Mutex.Unlock()

							if !exists || queue.State != QueueRunning {
								continue // Skip if queue doesn't exist or is paused
							}

							qp.QueueLevels[queueName] = phase
							qp.PublishTasks(queueName)

							// ✅ Unlock the RunningLow trigger so it can fire again
							queue.ResetRunningLowTrigger()
						}
					}(name, qp.RunningLowChans[name])

				case <-time.After(10 * time.Second):
					if qp.Queues[name].QueueSize() == 0 && qp.Queues[name].State == QueueRunning {
						qp.PublishTasks(name)
					}
				}
			}
		}
	}
}

// PublishTasks fetches new tasks from DB and publishes them to the queue.
// PublishTasks fetches new tasks from DB and publishes them to the queue.
func (qp *QueuePublisher) PublishTasks(queueName string) {
	qp.Mutex.Lock()
	defer qp.Mutex.Unlock()

	queue, exists := qp.Queues[queueName]
	if !exists {
		core.GlobalLogger.LogMessage("error", "Queue not found", map[string]any{
			"queueName": queueName,
		})
		return
	}

	// Determine the correct table
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	}

	// Fetch only the tasks at the current level using path-based pagination
	currentLevel := qp.QueueLevels[queueName]
	lastPath := qp.LastPathCursors[queueName]
	tasks := qp.FetchTasksFromDB(table, queue.Type, currentLevel, lastPath)

	// If we're under batch size, trigger polling to keep checking
	core.GlobalLogger.LogMessage("info", "Tasks published successfully", map[string]any{
		"queueName": queueName,
		"taskCount": len(tasks),
	})

	if len(tasks) < qp.BatchSize {
		qp.startPolling(queueName)
	}

	// If no tasks and all workers are idle, advance to next phase
	if len(tasks) == 0 && queue.AreAllWorkersIdle() && queue.State == QueueRunning {
		queue.Phase++
		qp.QueueLevels[queueName] = queue.Phase
		qp.PhaseUpdated <- queue.Phase
		core.GlobalLogger.LogMessage("info", "Advancing to next phase", map[string]any{
			"queue": queueName,
			"phase": queue.Phase,
		})
		if queue.State == QueueRunning {
			qp.PublishTasks(queueName)
		}
		return
	}

	for _, task := range tasks {
		queue.AddTask(task)
	}

	if len(tasks) > 0 {
		lastTask := tasks[len(tasks)-1]
		qp.LastPathCursors[queueName] = lastTask.GetPath()
	}

	queue.NotifyWorkers()

	select {
	case qp.PublishSignals[queueName] <- true:
	default:
	}
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
				qp.PublishTasks(queueName)
				queue := qp.Queues[queueName]
				if queue.QueueSize() == 0 && queue.AreAllWorkersIdle() {
					queue.Phase++
					qp.QueueLevels[queueName] = queue.Phase
					qp.PhaseUpdated <- queue.Phase
					qp.PublishTasks(queueName)
					return
				}
			}
		}
	}()

	core.GlobalLogger.LogMessage("info", "Polling started", map[string]any{
		"queueName": queueName,
		"interval":  controller.Interval,
	})
}

func (qp *QueuePublisher) FetchTasksFromDB(table string, queueType QueueType, currentLevel int, lastSeenPath string) []*Task {
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

	// Start building the query
	query := `SELECT type, path, parent_id, last_modified, identifier FROM ` + table + `
		WHERE (` + statusColumn + ` = 'pending' OR (` + statusColumn + ` = 'failed' AND ` + retryColumn + ` < ?))
		AND level = ?`

	params := []any{qp.RetryThreshold, currentLevel}

	// Apply cursor pagination using path as surrogate key
	if lastSeenPath != "" {
		query += ` AND path > ?`
		params = append(params, lastSeenPath)
	}

	// Add type filtering
	if onlyFolders {
		query += ` AND type = 'folder'`
	} else if onlyFiles {
		query += ` AND type = 'file'`
	}

	query += ` ORDER BY path LIMIT ?`
	params = append(params, qp.BatchSize)

	// Run query
	rows, err := qp.DB.Query(table, query, params...)
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
				Name:         path, // You may want to trim this
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        currentLevel,
			}

			task, err := NewTraversalTask(path, folder, &parentID) // Using path as ID surrogate
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
				Size:         0, // Update later if needed
				Level:        currentLevel,
			}

			task, err := NewUploadTask(path, file, nil, &parentID)
			if err == nil {
				tasks = append(tasks, task)
			}
		}
	}

	core.GlobalLogger.LogMessage("info", "Tasks fetched from DB", map[string]any{
		"table":     table,
		"taskCount": len(tasks),
	})
	return tasks
}

// StopListening stops QP operations.
func (qp *QueuePublisher) StopListening() {
	core.GlobalLogger.LogMessage("info", "Stopping QueuePublisher listening loop", nil)
	qp.Running = false
}
