package processing

import (
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/core/filesystem"
	"github.com/Voltaic314/ByteWave/code/core/db"
)

// QueuePublisher manages multiple queues dynamically.
type QueuePublisher struct {
	DB              *db.DB
	Queues          map[string]*TaskQueue  // Dynamic queue storage
	QueueLevels     map[string]int         // Tracks the level for each queue
	QueueBoardChans map[string]chan int    // Signals from multiple queue boards (now carries level)
	PublishSignals  map[string]chan bool   // Signals to workers for multiple queues
	PhaseUpdated    chan int               // Signals when phase has changed
	Mutex           sync.Mutex
	Running         bool

	// Configurable settings (adjustable by DRA later)
	RetryThreshold  int // Max retries before marking failure
	BatchSize       int // How many tasks to fetch per DB query
}

// NewQueuePublisher initializes a multi-queue publisher.
func NewQueuePublisher(db *db.DB, retryThreshold, batchSize int) *QueuePublisher {
	return &QueuePublisher{
		DB:              db,
		Queues:          make(map[string]*TaskQueue),
		QueueLevels:     make(map[string]int),
		QueueBoardChans: make(map[string]chan int),
		PublishSignals:  make(map[string]chan bool),
		PhaseUpdated:    make(chan int, 1), // Global phase update signal
		Running:         false,

		// Set adjustable parameters
		RetryThreshold: retryThreshold,
		BatchSize:      batchSize,
	}
}

// RegisterQueue dynamically registers a queue and its channels.
func (qp *QueuePublisher) RegisterQueue(name string, queue *TaskQueue, initialLevel int) {
	qp.Queues[name] = queue
	qp.QueueLevels[name] = initialLevel
	qp.QueueBoardChans[name] = make(chan int, 1) // Now expects level info
	qp.PublishSignals[name] = make(chan bool, 1)
}

// StartListening listens for signals from multiple queue boards.
func (qp *QueuePublisher) StartListening() {
	qp.Running = true
	go func() {
		for qp.Running {
			for name, signalChan := range qp.QueueBoardChans {
				select {
				case level := <-signalChan: // Signal from specific queue board with level
					qp.QueueLevels[name] = level
					qp.PublishTasks(name)

				case <-time.After(10 * time.Second): // Fallback check (for now)
					if qp.Queues[name].QueueSize() == 0 {
						qp.PublishTasks(name)
					}
				}
			}
		}
	}()
}

// PublishTasks fetches new tasks from DB and publishes them to the queue.
func (qp *QueuePublisher) PublishTasks(queueName string) {
	qp.Mutex.Lock()
	defer qp.Mutex.Unlock()

	queue, exists := qp.Queues[queueName]
	if !exists || queue.Locked {
		return
	}

	// Determine the correct table
	table := "source_nodes"
	if queue.Type == UploadQueueType {
		table = "destination_nodes"
	}

	// Fetch only the tasks at the current level
	currentLevel := qp.QueueLevels[queueName]
	tasks := qp.FetchTasksFromDB(table, queue.Type, currentLevel)
	if len(tasks) == 0 && queue.AreAllWorkersIdle() {
		// 🎯 Round is over! Increment level & notify queue board
		queue.Phase++
		qp.QueueLevels[queueName] = queue.Phase

		// Signal Queue Board that the phase has changed
		qp.PhaseUpdated <- queue.Phase

		// Immediately fetch new tasks for the next level
		qp.PublishTasks(queueName)
		return
	}

	// Add tasks to queue
	for _, task := range tasks {
		queue.AddTask(task)
	}

	// Notify workers that tasks are ready
	select {
	case qp.PublishSignals[queueName] <- true:
	default:
	}
}

// FetchTasksFromDB intelligently retrieves and constructs tasks from the DB.
func (qp *QueuePublisher) FetchTasksFromDB(table string, queueType QueueType, currentLevel int) []*Task {
	// Determine columns to fetch based on queue type & phase
	statusColumn := "traversal_status"
	retryColumn := "traversal_attempts"

	if queueType == UploadQueueType {
		statusColumn = "upload_status"
		retryColumn = "upload_attempts"
	}

	// Determine the correct filtering logic
	onlyFolders := queueType == TraversalQueueType // Traversal: Fetch only folders
	onlyFiles := queueType == UploadQueueType      // Upload: Fetch only files

	// Construct query dynamically
	query := `SELECT id, type, path, parent_id, last_modified, identifier FROM ` + table + ` 
	WHERE (` + statusColumn + ` = 'pending' OR (` + statusColumn + ` = 'failed' AND ` + retryColumn + ` < ?)) 
	AND level = ?`

	if onlyFolders {
		query += ` AND type = 'folder'`
	} else if onlyFiles {
		query += ` AND type = 'file'`
	} // If `includeBoth`, don't filter

	query += ` LIMIT ?` // Add pagination

	// Execute query
	rows, err := qp.DB.Query(query, qp.RetryThreshold, currentLevel, qp.BatchSize)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		var id, nodeType, path, parentID, lastModified, identifier string
		if err := rows.Scan(&id, &nodeType, &path, &parentID, &lastModified, &identifier); err != nil {
			continue
		}

		// Dynamically construct the correct task
		if nodeType == "folder" {
			folder := &filesystem.Folder{
				Name:         path,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Level:        currentLevel,
			}

			task, err := NewTraversalTask(id, folder, &parentID)
			if err == nil { // Only append if valid
				tasks = append(tasks, task)
			}
		} else if nodeType == "file" {
			file := &filesystem.File{
				Name:         path,
				Path:         path,
				ParentID:     parentID,
				Identifier:   identifier,
				LastModified: lastModified,
				Size:         0, // Might need adjustment based on worker logic
				Level:        currentLevel,
			}

			task, err := NewUploadTask(id, file, nil, &parentID)
			if err == nil { // Only append if valid
				tasks = append(tasks, task)
			}
		}
	}
	return tasks
}

// StopListening stops QP operations.
func (qp *QueuePublisher) StopListening() {
	qp.Running = false
}
