package workers

import (
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
	"github.com/Voltaic314/ByteWave/code/core/processing"
	"github.com/Voltaic314/ByteWave/code/core/pv"
	"github.com/Voltaic314/ByteWave/code/core/services"
)

// TraverserWorker processes folder traversal tasks.
type TraverserWorker struct {
	DB        *db.DB
	Service   services.BaseService // Interface for interacting with FS/API
	QueueType string               // "src" or "dst"
	pv        pv.PathValidator
	Logger    *core.Logger
	Queue     *processing.TaskQueue // NEW: Needed to get the Pagination Channel
	TaskReady bool
}

// FetchAndProcessTask pulls a task from the queue and executes it.
func (tw *TraverserWorker) FetchAndProcessTask() {
	for {
		// Check if queue is paused — if so, wait on the queue cond variable
		tw.Queue.WaitIfPaused(tw.Logger)

		// Try to get a task
		task := tw.Queue.PopTask()
		if task != nil {
			tw.Logger.LogMessage("info", "Task acquired, processing", map[string]any{
				"task_id":    task.ID,
				"queue_type": tw.QueueType,
			})

			// Reset task-ready flag just in case
			tw.TaskReady = false

			// Process the task
			err := tw.ProcessTraversalTask(task)
			if err != nil {
				tw.Logger.LogMessage("error", "Error during traversal task", map[string]any{
					"task_id": task.ID,
					"error":   err.Error(),
				})
			}

			continue // Go back and try to get the next task
		}

		// If worker was marked ready, log and reset it
		if tw.TaskReady {
			tw.Logger.LogMessage("info", "Worker still ready but no task available", map[string]any{
				"queue_type": tw.QueueType,
			})
			tw.TaskReady = false
		}

		// Sleep briefly before polling again
		time.Sleep(2 * time.Second)
	}
}

// ProcessTraversalTask executes a traversal task.
func (tw *TraverserWorker) ProcessTraversalTask(task *processing.Task) error {
	tw.Logger.LogMessage("info", "Processing traversal", map[string]any{
		"path":       task.Folder.Path,
		"queue_type": tw.QueueType,
	})

	// Fetch pagination stream from TaskQueue
	paginationStream := tw.Queue.GetPaginationChan()

	// Pass pagination stream to GetAllItems()
	allFolders, allFiles, err := tw.Service.GetAllItems(*task.Folder, paginationStream)

	if err != nil {
		tw.Logger.LogMessage("error", "Error listing contents", map[string]any{
			"path":  task.Folder.Path,
			"error": err.Error(),
		})
		tw.logTraversalFailure(task, err.Error())
		return err
	}

	// Filter paths using PV logic
	var validFolders []filesystem.Folder
	var validFiles []filesystem.File

	for _, folder := range allFolders {
		if tw.pv.IsValidPath(folder.Path) {
			validFolders = append(validFolders, folder)
		}
	}

	for _, file := range allFiles {
		if tw.pv.IsValidPath(file.Path) {
			validFiles = append(validFiles, file)
		}
	}

	// Log results
	return tw.logTraversalSuccess(task, validFiles, validFolders)
}

// logTraversalSuccess writes traversal results to the database.
func (tw *TraverserWorker) logTraversalSuccess(task *processing.Task, files []filesystem.File, folders []filesystem.Folder) error {
	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	// Insert files into DB
	for _, file := range files {
		tw.DB.QueueWrite(table, `INSERT INTO `+table+` (path, identifier, type, level, size, last_modified, traversal_status) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			file.Path, file.Identifier, "file", task.Folder.Level+1, file.Size, file.LastModified, "successful")
	}

	// Insert folders into DB
	for _, folder := range folders {
		tw.DB.QueueWrite(table, `INSERT INTO `+table+` (path, identifier, type, level, traversal_status) VALUES (?, ?, ?, ?, ?)`,

			folder.Path, folder.Identifier, "folder", task.Folder.Level+1, "successful")
	}

	tw.Logger.LogMessage("info", "Traversal success recorded", map[string]any{
		"path":       task.Folder.Path,
		"files":      len(files),
		"folders":    len(folders),
		"queue_type": tw.QueueType,
	})

	return nil
}

// logTraversalFailure logs traversal failure attempts in the database.
func (tw *TraverserWorker) logTraversalFailure(task *processing.Task, errorMsg string) {
	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	tw.DB.QueueWrite(table, `UPDATE `+table+` SET traversal_status = 'failed', traversal_attempts = traversal_attempts + 1 WHERE path = ?`, task.Folder.Path)

	tw.Logger.LogMessage("error", "Traversal failed", map[string]any{
		"path":       task.Folder.Path,
		"error":      errorMsg,
		"queue_type": tw.QueueType,
	})
}
