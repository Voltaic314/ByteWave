package processing

import (
	"fmt"

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
	// Perform basic validation
	if folder.Path == "" {
		return nil, fmt.Errorf("invalid folder: path is empty")
	}

	return &Task{
		ID:           id,
		Type:         TaskTraversal,
		Folder:       folder,
		File:         nil,
		ParentTaskID: parentTaskID,
		Locked:       false, // Initially unlocked
	}, nil
}

// NewUploadTask initializes a new upload task (file or folder).
func NewUploadTask(id string, file *filesystem.File, folder *filesystem.Folder, parentTaskID *string) (*Task, error) {
	if file == nil && folder == nil {
		return nil, fmt.Errorf("cannot create upload task without file or folder")
	}

	return &Task{
		ID:           id,
		Type:         TaskUpload,
		Folder:       folder,
		File:         file,
		ParentTaskID: parentTaskID,
		Locked:       false, // Initially unlocked
	}, nil
}
