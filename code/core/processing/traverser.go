// traverser.go (specific logic for traversal)
package processing

import (
	"time"

	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
	"github.com/Voltaic314/ByteWave/code/core/pv"
	"github.com/Voltaic314/ByteWave/code/core/services"
)

// TraverserWorker processes folder traversal tasks.
type TraverserWorker struct {
	WorkerBase
	DB      *db.DB
	Service services.BaseService // Interface for interacting with FS/API
	pv      pv.PathValidator
}

// FetchAndProcessTask pulls a task from the queue and executes it.
func (tw *TraverserWorker) FetchAndProcessTask() {
	for {
		tw.Queue.WaitIfPaused(tw.Logger)

		task := tw.Queue.PopTask()
		if task != nil {
			tw.Logger.LogMessage("info", "Task acquired, processing", map[string]any{
				"task_id":    task.ID,
				"queue_type": tw.QueueType,
			})

			tw.TaskReady = false

			err := tw.ProcessTraversalTask(task)
			if err != nil {
				tw.Logger.LogMessage("error", "Error during traversal task", map[string]any{
					"task_id": task.ID,
					"error":   err.Error(),
				})
			}

			continue
		}

		if tw.TaskReady {
			tw.Logger.LogMessage("info", "Worker still ready but no task available", map[string]any{
				"queue_type": tw.QueueType,
			})
			tw.TaskReady = false
		}

		time.Sleep(2 * time.Second)
	}
}

// ProcessTraversalTask executes a traversal task.
func (tw *TraverserWorker) ProcessTraversalTask(task *Task) error {
	tw.Logger.LogMessage("info", "Processing traversal", map[string]any{
		"path":       task.Folder.Path,
		"queue_type": tw.QueueType,
	})

	paginationStream := tw.Queue.GetPaginationChan()
	allFolders, allFiles, err := tw.Service.GetAllItems(*task.Folder, paginationStream)

	if err != nil {
		tw.Logger.LogMessage("error", "Error listing contents", map[string]any{
			"path":  task.Folder.Path,
			"error": err.Error(),
		})
		tw.logTraversalFailure(task, err.Error())
		return err
	}

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

	return tw.logTraversalSuccess(task, validFiles, validFolders)
}

func (tw *TraverserWorker) logTraversalSuccess(task *Task, files []filesystem.File, folders []filesystem.Folder) error {
	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	for _, file := range files {
		tw.DB.QueueWrite(table, `INSERT INTO `+table+` (path, identifier, type, level, size, last_modified, traversal_status) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			file.Path, file.Identifier, "file", task.Folder.Level+1, file.Size, file.LastModified, "successful")
	}

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

func (tw *TraverserWorker) logTraversalFailure(task *Task, errorMsg string) {
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
