// traverser.go (specific logic for traversal)
package processing

import (
	"fmt"
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
	"github.com/Voltaic314/ByteWave/code/core/pv"
	"github.com/Voltaic314/ByteWave/code/core/services"
)

// TraverserWorker processes folder traversal tasks.
type TraverserWorker struct {
	*WorkerBase
	DB      *db.DB
	Service services.BaseServiceInterface // Interface for interacting with FS/API
	pv      *pv.PathValidator
}

// FetchAndProcessTask pulls a task from the queue and executes it.
func (tw *TraverserWorker) FetchAndProcessTask() {
	core.GlobalLogger.LogMessage("info", "Starting task fetch and process loop", map[string]any{
		"workerID":  tw.ID,
		"queueType": tw.QueueType,
	})

	for {
		tw.Queue.WaitIfPaused()

		task := tw.Queue.PopTask()
		if task != nil {
			core.GlobalLogger.LogMessage("info", "Task acquired, processing", map[string]any{
				"workerID":  tw.ID,
				"taskID":    task.ID,
				"queueType": tw.QueueType,
				"path":      task.GetPath(),
			})

			tw.TaskReady = false
			tw.State = WorkerActive

			err := tw.ProcessTraversalTask(task)
			if err != nil {
				core.GlobalLogger.LogMessage("error", "Error during traversal task", map[string]any{
					"workerID": tw.ID,
					"taskID":   task.ID,
					"error":    err.Error(),
				})
			}

			continue
		}

		if tw.TaskReady {
			core.GlobalLogger.LogMessage("info", "Worker still ready but no task available", map[string]any{
				"workerID":  tw.ID,
				"queueType": tw.QueueType,
			})
			tw.TaskReady = false
		}

		tw.State = WorkerIdle
		time.Sleep(2 * time.Second)
	}
}

// ProcessTraversalTask executes a traversal task.
func (tw *TraverserWorker) ProcessTraversalTask(task *Task) error {
	core.GlobalLogger.LogMessage("info", "Processing traversal task", map[string]any{
		"workerID": tw.ID,
		"taskID":   task.ID,
		"path":     task.GetPath(),
	})

	// Validate path
	if !tw.pv.IsValidPath(task.Folder.Path) {
		core.GlobalLogger.LogMessage("error", "Invalid path for traversal", map[string]any{
			"workerID": tw.ID,
			"taskID":   task.ID,
			"path":     task.Folder.Path,
		})
		tw.LogTraversalFailure(task, "invalid path")
		return fmt.Errorf("invalid path: %s", task.Folder.Path)
	}

	// List folder contents
	paginationStream := tw.Queue.GetPaginationChan()
	foldersChan, filesChan, errChan := tw.Service.GetAllItems(*task.Folder, paginationStream)

	var allFolders []filesystem.Folder
	var allFiles []filesystem.File

	for folders := range foldersChan {
		allFolders = append(allFolders, folders...)
	}

	for files := range filesChan {
		allFiles = append(allFiles, files...)
	}

	err := <-errChan // Wait for error channel to close

	if err != nil {
		core.GlobalLogger.LogMessage("error", "Failed to list folder contents", map[string]any{
			"workerID": tw.ID,
			"taskID":   task.ID,
			"path":     task.Folder.Path,
			"error":    err.Error(),
		})
		tw.LogTraversalFailure(task, err.Error())
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

	return tw.LogTraversalSuccess(task, validFiles, validFolders)
}

func (tw *TraverserWorker) LogTraversalSuccess(task *Task, files []filesystem.File, folders []filesystem.Folder) error {
	core.GlobalLogger.LogMessage("info", "Logging traversal success", map[string]any{
		"workerID": tw.ID,
		"taskID":   task.ID,
		"path":     task.Folder.Path,
	})

	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	// Create inserts for all the files found in the traversal
	for _, file := range files {
		tw.DB.QueueWrite(table, `INSERT INTO `+table+` (
			path, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			file.Path,
			file.Identifier,
			task.Folder.Path,
			"file",
			task.Folder.Level+1,
			file.Size,
			file.LastModified,
			"successful",
			"pending",
			0, 0, nil,
		)
	}

	// Create inserts for all the folders found in the traversal
	for _, folder := range folders {
		tw.DB.QueueWrite(table, `INSERT INTO `+table+` (
			path, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			folder.Path,
			folder.Identifier,
			task.Folder.Path,
			"folder",
			task.Folder.Level+1,
			0, // Size = 0 for folders
			folder.LastModified,
			"successful",
			"pending",
			0, 0, nil,
		)
	}

	// Mark the parent folder (this task's folder) as successful
	tw.DB.QueueWrite(table, `UPDATE `+table+` SET traversal_status = 'successful' WHERE path = ?`, task.Folder.Path)

	core.GlobalLogger.LogMessage("info", "Traversal success logged to DB", map[string]any{
		"path":       task.Folder.Path,
		"files":      len(files),
		"folders":    len(folders),
		"queue_type": tw.QueueType,
	})
	return nil
}


func (tw *TraverserWorker) LogTraversalFailure(task *Task, errorMsg string) {
	core.GlobalLogger.LogMessage("error", "Logging traversal failure", map[string]any{
		"workerID": tw.ID,
		"taskID":   task.ID,
		"path":     task.Folder.Path,
		"error":    errorMsg,
	})

	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	tw.DB.QueueWrite(table, `UPDATE `+table+` SET traversal_status = 'failed', traversal_attempts = traversal_attempts + 1 WHERE path = ?`, task.Folder.Path)

	core.GlobalLogger.LogMessage("info", "Traversal failure logged to DB", map[string]any{
		"workerID": tw.ID,
		"taskID":   task.ID,
	})
}
