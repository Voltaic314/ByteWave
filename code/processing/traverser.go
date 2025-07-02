// traverser.go (specific logic for traversal)
package processing

import (
	"fmt"
	"strings"
	"time"

	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/pv"
	"github.com/Voltaic314/ByteWave/code/services"
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
	logging.GlobalLogger.LogWorker("info", tw.ID, "", "Starting task fetch and process loop", map[string]any{
		"queueType": tw.QueueType,
	})

	for {
		tw.Queue.WaitIfPaused()

		task := tw.Queue.PopTask()
		if task != nil {
			logging.GlobalLogger.LogWorker("info", tw.ID, task.GetPath(), "Task acquired, processing", map[string]any{
				"taskID":    task.ID,
				"queueType": tw.QueueType,
			})

			tw.TaskReady = false
			tw.State = WorkerActive

			err := tw.ProcessTraversalTask(task)
			if err != nil {
				logging.GlobalLogger.LogWorker("error", tw.ID, task.GetPath(), "Error during traversal task", map[string]any{
					"taskID": task.ID,
					"error":  err.Error(),
				})
			}

			continue
		}

		if tw.TaskReady {
			logging.GlobalLogger.LogWorker("info", tw.ID, "", "Worker still ready but no task available", map[string]any{
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
	logging.GlobalLogger.LogWorker("info", tw.ID, task.GetPath(), "Processing traversal task", map[string]any{
		"taskID": task.ID,
	})

	// Validate path
	if !tw.pv.IsValidPath(task.Folder.Path) {
		logging.GlobalLogger.LogWorker("error", tw.ID, task.GetPath(), "Invalid path for traversal", map[string]any{
			"taskID": task.ID,
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
		logging.GlobalLogger.LogWorker("error", tw.ID, task.GetPath(), "Failed to list folder contents", map[string]any{
			"taskID": task.ID,
			"error":  err.Error(),
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
	logging.GlobalLogger.LogWorker("info", tw.ID, task.GetPath(), "Logging traversal success", map[string]any{
		"taskID": task.ID,
	})

	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	// Create inserts for all the files found in the traversal
	for _, file := range files {
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			file.Path,
			file.Name,
			file.Identifier,
			strings.ReplaceAll(task.Folder.Path, "/", "\\"),
			"file",
			task.Folder.Level+1,
			file.Size,
			file.LastModified,
			"successful", // we don't traverse files so this is always successful
			"pending",
			0, 0, nil,
		)
	}

	// Create inserts for all the folders found in the traversal
	for _, folder := range folders {
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			folder.Path,
			folder.Name,
			folder.Identifier,
			strings.ReplaceAll(task.Folder.Path, "/", "\\"),
			"folder",
			task.Folder.Level+1,
			0, // Size = 0 for folders
			folder.LastModified,
			"pending",
			"pending",
			0, 0, nil,
		)
	}

	// Mark the parent folder (this task's folder) as successful
	updateQuery := `UPDATE ` + table + ` SET traversal_status = 'successful', traversal_attempts = traversal_attempts + 1 WHERE path = ?`
	updatePath := task.Folder.Path

	logging.GlobalLogger.LogWorker("debug", tw.ID, task.GetPath(), "Executing success UPDATE", map[string]any{
		"query": updateQuery,
		"path":  updatePath,
		"table": table,
	})

	tw.DB.QueueWriteWithPath(table, updatePath, updateQuery, updatePath)

	logging.GlobalLogger.LogWorker("info", tw.ID, task.GetPath(), "Traversal success logged to DB", map[string]any{
		"files":      len(files),
		"folders":    len(folders),
		"queue_type": tw.QueueType,
	})

	// Add debug logging to track the success update
	logging.GlobalLogger.LogWorker("debug", tw.ID, task.GetPath(), "Marking folder as successful", map[string]any{
		"taskID":       task.ID,
		"table":        table,
		"filesFound":   len(files),
		"foldersFound": len(folders),
	})

	return nil
}

func (tw *TraverserWorker) LogTraversalFailure(task *Task, errorMsg string) {
	logging.GlobalLogger.LogWorker("error", tw.ID, task.GetPath(), "Logging traversal failure", map[string]any{
		"taskID": task.ID,
		"error":  errorMsg,
	})

	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	tw.DB.QueueWriteWithPath(table, task.Folder.Path, `UPDATE `+table+` SET traversal_status = 'failed', traversal_attempts = traversal_attempts + 1, last_error = ? WHERE path = ?`, errorMsg, task.Folder.Path)

	logging.GlobalLogger.LogWorker("info", tw.ID, task.GetPath(), "Traversal failure logged to DB", map[string]any{
		"taskID": task.ID,
	})
}
