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

// PathNormalizer normalizes paths for the database
func (tw *TraverserWorker) PathNormalizer(path string) string {
	// convert all back slashes to forward slashes
	path = strings.ReplaceAll(path, "\\", "/")
	// remove the leading slash if any
	path = strings.TrimLeft(path, "/")
	// add a leading slash (this is just to avoid double start slashes just in case)
	path = "/" + path

	return path
}

// FetchAndProcessTask pulls a task from the queue and executes it.
func (tw *TraverserWorker) FetchAndProcessTask() {
	logging.GlobalLogger.Log("info", "System", "Traverser", "Starting task fetch and process loop", map[string]any{
		"queueType": tw.QueueType,
	}, "STARTING_TASK_FETCH_AND_PROCESS_LOOP", tw.QueueType,
	)

	for {
		tw.Queue.WaitIfPaused()

		task := tw.Queue.PopTask()
		if task != nil {
			logging.GlobalLogger.Log("info", "System", "Traverser", "Task acquired, processing", map[string]any{
				"taskID":    task.GetID(),
				"queueType": tw.QueueType,
			}, "TASK_ACQUIRED_PROCESSING", tw.QueueType,
			)

			tw.TaskReady = false
			tw.State = WorkerActive

			err := tw.ProcessTraversalTask(task)
			if err != nil {
				logging.GlobalLogger.Log("error", "System", "Traverser", "Error during traversal task", map[string]any{
					"taskID": task.GetID(),
					"error":  err.Error(),
				}, "ERROR_DURING_TRAVERSAL_TASK", tw.QueueType,
			)
			}

			continue
		}

		if tw.TaskReady {
			logging.GlobalLogger.Log("info", "System", "Traverser", "Worker still ready but no task available", map[string]any{
				"queueType": tw.QueueType,
			}, "WORKER_STILL_READY_BUT_NO_TASK_AVAILABLE", tw.QueueType,
			)
			tw.TaskReady = false
		}

		tw.State = WorkerIdle
		time.Sleep(2 * time.Second)
	}
}

// ProcessTraversalTask executes a traversal task.
func (tw *TraverserWorker) ProcessTraversalTask(task Task) error {
	logging.GlobalLogger.Log("info", "System", "Traverser", "Processing traversal task", map[string]any{
		"taskID": task.GetID(),
		"type":   task.GetType(),
	}, "PROCESSING_TRAVERSAL_TASK", tw.QueueType,
	)

	// Validate path
	if !tw.pv.IsValidPath(task.GetFolder().Path) {
		logging.GlobalLogger.Log("error", "System", "Traverser", "Invalid path for traversal", map[string]any{
			"taskID": task.GetID(),
		}, "INVALID_PATH_FOR_TRAVERSAL", tw.QueueType,
		)
		tw.LogTraversalFailure(task, "invalid path")
		return fmt.Errorf("invalid path: %s", task.GetFolder().Path)
	}

	// Handle different task types using type switching
	switch concreteTask := task.(type) {
	case *TraversalSrcTask:
		return tw.ProcessSrcTraversal(concreteTask)
	case *TraversalDstTask:
		return tw.ProcessDstTraversal(concreteTask)
	default:
		return fmt.Errorf("unknown task type: %s", task.GetType())
	}
}

// ProcessSrcTraversal handles source traversal - discovers and logs all valid items
func (tw *TraverserWorker) ProcessSrcTraversal(task *TraversalSrcTask) error {
	// List folder contents
	paginationStream := tw.Queue.GetPaginationChan()
	foldersChan, filesChan, errChan := tw.Service.GetAllItems(*task.GetFolder(), paginationStream)

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
		logging.GlobalLogger.Log("error", "System", "Traverser", "Failed to list folder contents", map[string]any{
			"taskID": task.GetID(),
			"error":  err.Error(),
		}, "FAILED_TO_LIST_FOLDER_CONTENTS", tw.QueueType,
		)
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

	// Log all discovered items for source traversal
	return tw.LogSrcTraversalSuccess(task, validFiles, validFolders)
}

// ProcessDstTraversal handles destination traversal - compares discovered items against expected source children
func (tw *TraverserWorker) ProcessDstTraversal(dstTask *TraversalDstTask) error {

	// List folder contents
	paginationStream := tw.Queue.GetPaginationChan()
	foldersChan, filesChan, errChan := tw.Service.GetAllItems(*dstTask.GetFolder(), paginationStream)

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
		logging.GlobalLogger.Log("error", "System", "Traverser", "Failed to list folder contents", map[string]any{
			"taskID": dstTask.GetID(),
			"error":  err.Error(),
		}, "FAILED_TO_LIST_FOLDER_CONTENTS", tw.QueueType,
		)
		tw.LogTraversalFailure(dstTask, err.Error())
		return err
	}

	// Create maps for efficient lookup of expected source items
	expectedFolders := make(map[string]*filesystem.Folder)
	expectedFiles := make(map[string]*filesystem.File)

	// Populate maps with expected source children from the task
	for _, folder := range dstTask.ExpectedSrcChildren {
		expectedFolders[folder.Name] = folder
	}

	for _, file := range dstTask.ExpectedSrcFiles {
		expectedFiles[file.Name] = file
	}

	// Filter to only include items that are expected from source AND actually present
	var presentFiles []filesystem.File
	var presentFolders []filesystem.Folder

	// Check discovered files against expected source files
	for _, file := range allFiles {
		if _, expectedFromSource := expectedFiles[file.Name]; expectedFromSource {
			// This file is expected from source AND present in destination
			presentFiles = append(presentFiles, file)
		}
	}

	// Check discovered folders against expected source folders
	for _, folder := range allFolders {
		if _, expectedFromSource := expectedFolders[folder.Name]; expectedFromSource {
			// This folder is expected from source AND present in destination
			presentFolders = append(presentFolders, folder)
		}
	}

	logging.GlobalLogger.Log("info", "System", "Traverser", "Destination traversal comparison complete", map[string]any{
		"expected_files":     len(expectedFiles),
		"expected_folders":   len(expectedFolders),
		"discovered_files":   len(allFiles),
		"discovered_folders": len(allFolders),
		"present_files":      len(presentFiles),
		"present_folders":    len(presentFolders),
	}, "DESTINATION_TRAVERSAL_COMPARISON_COMPLETE", tw.QueueType,
	)

	// Log only the items that are present (expected from source AND found in destination)
	return tw.LogDstTraversalSuccess(dstTask, presentFiles, presentFolders)
}

func (tw *TraverserWorker) LogSrcTraversalSuccess(task Task, files []filesystem.File, folders []filesystem.Folder) error {
	logging.GlobalLogger.Log("info", "System", "Traverser", "Logging traversal success", map[string]any{
		"taskID": task.GetID(),
	}, "LOGGING_TRAVERSAL_SUCCESS", tw.QueueType,
	)

	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	// Create inserts for all the files found in the traversal
	for _, file := range files {
		path := tw.PathNormalizer(file.Path)
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			path,
			file.Name,
			file.Identifier,
			task.GetFolder().Identifier, // Use parent's identifier (absolute path for OS service)
			"file",
			task.GetFolder().Level+1,
			file.Size,
			file.LastModified,
			"successful", // we don't traverse files so this is always successful
			"pending",
			0, 0, nil,
		)
	}

	// Create inserts for all the folders found in the traversal
	for _, folder := range folders {
		path := tw.PathNormalizer(folder.Path)
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			path,
			folder.Name,
			folder.Identifier,
			task.GetFolder().Identifier, // Use parent's identifier (absolute path for OS service)
			"folder",
			task.GetFolder().Level+1,
			0, // Size = 0 for folders
			folder.LastModified,
			"pending",
			"pending",
			0, 0, nil,
		)
	}

	// Mark the parent folder (this task's folder) as successful
	updateQuery := `UPDATE ` + table + ` SET traversal_status = 'successful', traversal_attempts = traversal_attempts + 1 WHERE path = ?`
	updatePath := task.GetFolder().Path // Already relative, matches DB format

	logging.GlobalLogger.Log("debug", "System", "Traverser", "Executing success UPDATE", map[string]any{
		"query": updateQuery,
		"path":  updatePath,
		"table": table,
	}, "EXECUTING_SUCCESS_UPDATE", tw.QueueType,
	)

	tw.DB.QueueWriteWithPath(table, updatePath, updateQuery, updatePath)

	logging.GlobalLogger.Log("info", "System", "Traverser", "Traversal success logged to DB", map[string]any{
		"files":      len(files),
		"folders":    len(folders),
		"queue_type": tw.QueueType,
	}, "TRAVERSAL_SUCCESS_LOGGED_TO_DB", tw.QueueType,
	)

	// Add debug logging to track the success update
	logging.GlobalLogger.Log("debug", "System", "Traverser", "Marking folder as successful", map[string]any{
		"taskID":       task.GetID(),
		"table":        table,
		"filesFound":   len(files),
		"foldersFound": len(folders),
	}, "MARKING_FOLDER_AS_SUCCESSFUL", tw.QueueType,
	)

	return nil
}

// LogDstTraversalSuccess handles destination traversal success - logs items to destination_nodes table
func (tw *TraverserWorker) LogDstTraversalSuccess(task Task, files []filesystem.File, folders []filesystem.Folder) error {
	logging.GlobalLogger.Log("info", "System", "Traverser", "Logging destination traversal success", map[string]any{
		"taskID": task.GetID(),
	}, "LOGGING_DESTINATION_TRAVERSAL_SUCCESS", tw.QueueType,
	)

	table := "destination_nodes"

	// Log all discovered items to destination_nodes table
	for _, file := range files {
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tw.PathNormalizer(file.Path),
			file.Name,
			file.Identifier,
			task.GetFolder().Identifier, // Use parent's identifier (absolute path for OS service)
			"file",
			task.GetFolder().Level+1,
			file.Size,
			file.LastModified,
			"successful", // we don't traverse files so this is always successful
			"pending",
			0, 0, nil,
		)
	}

	for _, folder := range folders {
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, upload_status, traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tw.PathNormalizer(folder.Path),
			folder.Name,
			folder.Identifier,
			task.GetFolder().Identifier, // Use parent's identifier (absolute path for OS service)
			"folder",
			task.GetFolder().Level+1,
			0, // Size = 0 for folders
			folder.LastModified,
			"pending",
			"pending",
			0, 0, nil,
		)
	}

	// Mark the parent folder as successful
	updateQuery := `UPDATE ` + table + ` SET traversal_status = 'successful', traversal_attempts = traversal_attempts + 1 WHERE path = ?`
	updatePath := task.GetFolder().Path // Already relative, matches DB format

	logging.GlobalLogger.Log("debug", "System", "Traverser", "Executing destination success UPDATE", map[string]any{
		"query": updateQuery,
		"path":  updatePath,
		"table": table,
	}, "EXECUTING_DESTINATION_SUCCESS_UPDATE", tw.QueueType,
	)

	tw.DB.QueueWriteWithPath(table, updatePath, updateQuery, updatePath)

	logging.GlobalLogger.Log("info", "System", "Traverser", "Destination traversal success logged to DB", map[string]any{
		"files":      len(files),
		"folders":    len(folders),
		"queue_type": tw.QueueType,
	}, "DESTINATION_TRAVERSAL_SUCCESS_LOGGED_TO_DB", tw.QueueType,
	)

	return nil
}

func (tw *TraverserWorker) LogTraversalFailure(task Task, errorMsg string) {
	logging.GlobalLogger.Log("error", "System", "Traverser", "Logging traversal failure", map[string]any{
		"taskID": task.GetID(),
		"error":  errorMsg,
	}, "LOGGING_TRAVERSAL_FAILURE", tw.QueueType,
	)

	table := "source_nodes"
	if tw.QueueType == "dst" {
		table = "destination_nodes"
	}

	taskPath := task.GetFolder().Path // Already relative, matches DB format
	tw.DB.QueueWriteWithPath(table, taskPath, `UPDATE `+table+` SET traversal_status = 'failed', traversal_attempts = traversal_attempts + 1, last_error = ? WHERE path = ?`, errorMsg, taskPath)

	logging.GlobalLogger.Log("info", "System", "Traverser", "Traversal failure logged to DB", map[string]any{
		"taskID": task.GetID(),
	}, "TRAVERSAL_FAILURE_LOGGED_TO_DB", tw.QueueType,
	)
}
