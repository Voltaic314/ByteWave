package processing

import (
	"fmt"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
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
	core.GlobalLogger.LogMessage("info", "Creating new traversal task", map[string]any{
		"taskID":     id,
		"folderPath": folder.Path,
		"hasParent":  parentTaskID != nil,
	})

	// Perform basic validation
	if folder.Path == "" {
		core.GlobalLogger.LogMessage("error", "Invalid folder path", map[string]any{
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

	core.GlobalLogger.LogMessage("info", "Traversal task created successfully", map[string]any{
		"taskID":     id,
		"folderPath": folder.Path,
	})
	return task, nil
}

// NewUploadTask initializes a new upload task (file or folder).
func NewUploadTask(id string, file *filesystem.File, folder *filesystem.Folder, parentTaskID *string) (*Task, error) {
	core.GlobalLogger.LogMessage("info", "Creating new upload task", map[string]any{
		"taskID":    id,
		"hasFile":   file != nil,
		"hasFolder": folder != nil,
		"hasParent": parentTaskID != nil,
	})

	if file == nil && folder == nil {
		core.GlobalLogger.LogMessage("error", "Cannot create upload task without file or folder", map[string]any{
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

	core.GlobalLogger.LogMessage("info", "Upload task created successfully", map[string]any{
		"taskID": id,
		"path":   task.GetPath(),
	})
	return task, nil
}

func (t *Task) GetPath() string {
	core.GlobalLogger.LogMessage("info", "Getting task path", map[string]any{
		"taskID": t.ID,
		"type":   t.Type,
	})

	if t.Folder != nil {
		return t.Folder.Path
	}
	if t.File != nil {
		return t.File.Path
	}
	return ""
}
