package processing

import (
	"fmt"

	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
)

// TaskType defines the type of work this task is performing.
type TaskType string

const (
	TaskTraversal TaskType = "traversal"
	TaskUpload    TaskType = "upload"
)

type Task struct {
	ID           string             // Unique task identifier
	Type         TaskType           // Task type (traversal, upload)
	Folder       *filesystem.Folder // Pointer to Folder (nullable for upload tasks)
	File         *filesystem.File   // Pointer to File (nullable for traversal tasks)
	ParentTaskID *string            // Parent task ID (nullable for root tasks)
	Locked       bool               // Lock state to prevent race conditions
}

func NewTraversalTask(id string, folder *filesystem.Folder, parentTaskID *string) (*Task, error) {

	// Perform basic validation
	if folder.Path == "" {
		logging.GlobalLogger.LogMessage("error", "Invalid folder path", map[string]any{
			"taskID": id,
		})
		return nil, fmt.Errorf("invalid folder: path is empty")
	}

	task := &Task{
		ID:           id,
		Type:         TaskTraversal,
		Folder:       folder,
		File:         nil,
		ParentTaskID: parentTaskID,
		Locked:       false,
	}

	return task, nil
}

// NewUploadTask initializes a new upload task (file or folder).
func NewUploadTask(id string, file *filesystem.File, folder *filesystem.Folder, parentTaskID *string) (*Task, error) {
	logging.GlobalLogger.LogMessage("info", "Creating new upload task", map[string]any{
		"taskID":    id,
		"hasFile":   file != nil,
		"hasFolder": folder != nil,
		"hasParent": parentTaskID != nil,
	})

	if file == nil && folder == nil {
		logging.GlobalLogger.LogMessage("error", "Cannot create upload task without file or folder", map[string]any{
			"taskID": id,
		})
		return nil, fmt.Errorf("cannot create upload task without file or folder")
	}

	task := &Task{
		ID:           id,
		Type:         TaskUpload,
		Folder:       folder,
		File:         file,
		ParentTaskID: parentTaskID,
		Locked:       false,
	}

	logging.GlobalLogger.LogMessage("info", "Upload task created successfully", map[string]any{
		"taskID": id,
		"path":   task.GetPath(),
	})
	return task, nil
}

func (t *Task) GetPath() string {

	if t.Folder != nil {
		return t.Folder.Path
	}
	if t.File != nil {
		return t.File.Path
	}
	return ""
}
