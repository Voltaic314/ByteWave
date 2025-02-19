package processing

import (
	"fmt"
	"sync"
	"time"
	"log"

	"github.com/Voltaic314/Data_Migration_Tool/code/responsehandler"
	"github.com/Voltaic314/Data_Migration_Tool/code/filesystem"
)

// Worker represents a worker that processes tasks from a queue.
type Worker struct {
	ID         int          // Unique worker ID
	Queue      *TaskQueue   // Associated queue (traversal or upload)
	WorkerPool *sync.WaitGroup // Sync group to manage multiple workers
	Logger     *log.Logger  // Logger instance for worker messages
}

// NewWorker initializes a new worker.
func NewWorker(id int, queue *TaskQueue, pool *sync.WaitGroup, logger *log.Logger) *Worker {
	return &Worker{
		ID:         id,
		Queue:      queue,
		WorkerPool: pool,
		Logger:     logger,
	}
}

// Start begins processing tasks from the queue.
func (w *Worker) Start() {
	defer w.WorkerPool.Done()

	for {
		task := w.Queue.GetTask()
		if task == nil {
			time.Sleep(500 * time.Millisecond) // No task available, wait briefly
			continue
		}

		w.Logger.Printf("Worker %d picked up task %s\n", w.ID, task.ID)

		// Process the task
		success := w.ProcessTask(task)

		// Log result
		if success {
			w.Logger.Printf("Worker %d completed task %s\n", w.ID, task.ID)
		} else if task.CanRetry() {
			w.Queue.AddTask(task) // Requeue task for retry
			w.Logger.Printf("Worker %d requeued task %s (retry: %d/%d)\n", w.ID, task.ID, task.RetryAttempts, task.MaxRetries)
		} else {
			w.Logger.Printf("Worker %d marked task %s as failed\n", w.ID, task.ID)
		}
	}
}

// ProcessTask executes the actual task operation.
func (w *Worker) ProcessTask(task *Task) bool {
	task.MarkInProgress()

	switch task.Type {
	case TaskTraversal:
		return w.HandleTraversal(task)
	case TaskUpload:
		return w.HandleUpload(task)
	default:
		w.Logger.Printf("Unknown task type for task %s", task.ID)
		return false
	}
}

// HandleTraversal handles folder traversal tasks.
func (w *Worker) HandleTraversal(task *Task) bool {
	if task.Folder == nil {
		w.Logger.Printf("Traversal task %s missing folder reference", task.ID)
		task.MarkFailed("missing_folder")
		return false
	}

	// Simulate listing contents (replace with actual OS call)
	traversalResult := responsehandler.NewResponse(true, map[string]interface{}{
		"folders": []filesystem.Folder{}, // Retrieved folders
		"files":   []filesystem.File{},   // Retrieved files
	}, nil, nil)

	if !traversalResult.Ok {
		task.MarkFailed("traversal_failed")
		return false
	}

	// Extract retrieved folders/files and push them to the queue
	data := traversalResult.Data.(map[string]interface{})
	folders := data["folders"].([]filesystem.Folder)
	files := data["files"].([]filesystem.File)

	for _, folder := range folders {
		newTask := NewTraversalTask(fmt.Sprintf("%s-%s", task.ID, folder.Name), folder, task.MaxRetries)
		w.Queue.AddTask(newTask)
	}

	for _, file := range files {
		newTask, err := NewUploadTask(fmt.Sprintf("%s-%s", task.ID, file.Name), &file, nil, task.MaxRetries)
		if err == nil {
			w.Queue.AddTask(newTask)
		}
	}

	task.MarkSuccess()
	return true
}

// HandleUpload handles file/folder upload tasks.
func (w *Worker) HandleUpload(task *Task) bool {
	if task.File == nil && task.Folder == nil {
		w.Logger.Printf("Upload task %s missing file/folder reference", task.ID)
		task.MarkFailed("missing_file_or_folder")
		return false
	}

	// Simulate upload (replace with actual API call)
	uploadResult := responsehandler.NewResponse(true, nil, nil, nil)

	if !uploadResult.Ok {
		task.MarkFailed("upload_failed")
		return false
	}

	task.MarkSuccess()
	return true
}
