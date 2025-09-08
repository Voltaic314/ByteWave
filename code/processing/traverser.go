// traverser.go (specific logic for traversal)
package processing

import (
	"fmt"
	"strings"

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

// ProcessTraversalTask executes a traversal task.
func (tw *TraverserWorker) ProcessTraversalTask(task Task) error {
	logging.GlobalLogger.Log("info", "System", "Traverser", "Processing traversal task", map[string]any{
		"taskID":   task.GetID(),
		"type":     task.GetType(),
		"attempts": task.GetAttempts(),
	}, "PROCESSING_TRAVERSAL_TASK", tw.QueueType,
	)

	var err error

	var files []filesystem.File
	var folders []filesystem.Folder

	// Validate path
	if !tw.pv.IsValidPath(task.GetFolder().Path) {
		err = fmt.Errorf("invalid path: %s", task.GetFolder().Path)
		logging.GlobalLogger.Log("error", "System", "Traverser", "Invalid path for traversal", map[string]any{
			"taskID": task.GetID(),
			"error":  err.Error(),
		}, "INVALID_PATH_FOR_TRAVERSAL", tw.QueueType,
		)
	} else {
		// Handle different task types using type switching - collect data without writing
		switch concreteTask := task.(type) {
		case *TraversalSrcTask:
			files, folders, err = tw.ProcessSrcTraversal(concreteTask)
		case *TraversalDstTask:
			files, folders, err = tw.ProcessDstTraversal(concreteTask)
		default:
			err = fmt.Errorf("unknown task type: %s", task.GetType())
		}
	}

	// Simple processing - worker handles retry logic
	if err != nil {
		// Task failed - worker will handle retry logic
		logging.GlobalLogger.Log("error", "System", "Traverser", "Task processing failed", map[string]any{
			"taskID":   task.GetID(),
			"error":    err.Error(),
			"attempts": task.GetAttempts(),
		}, "TRAVERSER_TASK_PROCESSING_FAILED", tw.QueueType)
		return err
	}

	// Task succeeded - write to DB
	var dbErr error
	switch concreteTask := task.(type) {
	case *TraversalSrcTask:
		dbErr = tw.LogSrcTraversalSuccess(concreteTask, files, folders)
	case *TraversalDstTask:
		dbErr = tw.LogDstTraversalSuccess(concreteTask, files, folders)
	}

	if dbErr != nil {
		logging.GlobalLogger.Log("error", "System", "Traverser", "Task succeeded but DB write failed", map[string]any{
			"taskID":   task.GetID(),
			"error":    dbErr.Error(),
			"attempts": task.GetAttempts(),
		}, "TRAVERSER_DB_WRITE_FAILED", tw.QueueType)
		return dbErr
	}

	logging.GlobalLogger.Log("info", "System", "Traverser", "Task succeeded and logged to DB", map[string]any{
		"taskID":   task.GetID(),
		"attempts": task.GetAttempts(),
	}, "TRAVERSER_TASK_SUCCEEDED", tw.QueueType)
	return nil
}

// ProcessSrcTraversal handles source traversal - discovers and validates all items, returns data without writing
func (tw *TraverserWorker) ProcessSrcTraversal(task *TraversalSrcTask) ([]filesystem.File, []filesystem.Folder, error) {
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
		return nil, nil, err
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

	// Return validated data without writing to DB
	return validFiles, validFolders, nil
}

// ProcessDstTraversal handles destination traversal - compares discovered items against expected source children, returns data without writing
func (tw *TraverserWorker) ProcessDstTraversal(dstTask *TraversalDstTask) ([]filesystem.File, []filesystem.Folder, error) {

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
		return nil, nil, err
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

	// Return filtered data without writing to DB
	return presentFiles, presentFolders, nil
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
	updatePath := tw.PathNormalizer(task.GetFolder().Path) // Normalize to match INSERT format

	logging.GlobalLogger.Log("debug", "System", "Traverser", "Queueing success UPDATE", map[string]any{
		"query": updateQuery,
		"path":  updatePath,
		"table": table,
	}, "QUEUEING_SUCCESS_UPDATE", tw.QueueType,
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

	// Log all discovered items to destination_nodes table - Match canonical destination_nodes schema
	for _, file := range files {
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, traversal_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tw.PathNormalizer(file.Path),
			file.Name,
			file.Identifier,
			task.GetFolder().Identifier, // Use parent's identifier (absolute path for OS service)
			"file",
			task.GetFolder().Level+1,
			file.Size,
			file.LastModified,
			"successful", // we don't traverse files so this is always successful
			0, nil,
		)
	}

	for _, folder := range folders {
		tw.DB.QueueWrite(table, `INSERT OR IGNORE INTO `+table+` (
			path, name, identifier, parent_id, type, level, size, last_modified,
			traversal_status, traversal_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tw.PathNormalizer(folder.Path),
			folder.Name,
			folder.Identifier,
			task.GetFolder().Identifier, // Use parent's identifier (absolute path for OS service)
			"folder",
			task.GetFolder().Level+1,
			0, // Size = 0 for folders
			folder.LastModified,
			"pending",
			0, nil,
		)
	}

	// Mark the parent folder as successful
	updateQuery := `UPDATE ` + table + ` SET traversal_status = 'successful', traversal_attempts = traversal_attempts + 1 WHERE path = ?`
	updatePath := tw.PathNormalizer(task.GetFolder().Path) // Normalize to match INSERT format

	logging.GlobalLogger.Log("debug", "System", "Traverser", "Queueing destination success UPDATE", map[string]any{
		"query": updateQuery,
		"path":  updatePath,
		"table": table,
	}, "QUEUEING_DESTINATION_SUCCESS_UPDATE", tw.QueueType,
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

	taskPath := tw.PathNormalizer(task.GetFolder().Path) // Normalize to match DB format
	tw.DB.QueueWriteWithPath(table, taskPath, `UPDATE `+table+` SET traversal_status = 'failed', traversal_attempts = traversal_attempts + 1, last_error = ? WHERE path = ?`, errorMsg, taskPath)

	logging.GlobalLogger.Log("info", "System", "Traverser", "Traversal failure logged to DB", map[string]any{
		"taskID": task.GetID(),
	}, "TRAVERSAL_FAILURE_LOGGED_TO_DB", tw.QueueType,
	)
}
