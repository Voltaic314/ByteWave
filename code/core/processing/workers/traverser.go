package workers

import (
	"github.com/Voltaic314/Data_Migration_Tool/code/core/filesystem"
	"github.com/Voltaic314/Data_Migration_Tool/code/core/db"
	"github.com/Voltaic314/Data_Migration_Tool/code/core/services"
	"github.com/Voltaic314/Data_Migration_Tool/code/core/pv"
	"github.com/Voltaic314/Data_Migration_Tool/code/core/processing"
	"github.com/Voltaic314/Data_Migration_Tool/code/core"
)

// TraverserWorker processes folder traversal tasks.
type TraverserWorker struct {
	DB        *db.DB
	Service   services.BaseService // Interface for interacting with FS/API
	QueueType string               // "src" or "dst"
	pv        pv.PathValidator
	Logger    *core.Logger
	Queue     *processing.TaskQueue // NEW: Needed to get the Pagination Channel
}

// FetchAndProcessTask pulls a task from the queue and executes it.
func (tw *TraverserWorker) FetchAndProcessTask() error {
	task := tw.Queue.PopTask() // Fetch and lock a task
	if task == nil {
		tw.Logger.LogMessage("info", "No tasks available in queue", map[string]any{
			"queue_type": tw.QueueType,
		})
		return nil
	}

	// Ensure task is a traversal task
	if task.Type != processing.TaskTraversal {
		tw.Logger.LogMessage("warning", "Non-traversal task found in traversal queue", map[string]any{
			"task_id": task.ID,
		})
		return nil
	}

	return tw.ProcessTraversalTask(task)
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
