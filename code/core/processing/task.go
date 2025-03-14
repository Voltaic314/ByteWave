package processing

import (
	"fmt"
	"github.com/Voltaic314/Data_Migration_Tool/code/core/filesystem"
)

// TaskType defines the type of work this task is performing.
type TaskType string

const (
	TaskTraversal TaskType = "traversal"
	TaskUpload    TaskType = "upload"
)

// Task represents a single unit of work in the processing system.
type Task struct {
	ID           string             // Unique task identifier
	Type         TaskType           // Task type (traversal, upload)
	Folder       *filesystem.Folder // Folder (for traversal tasks)
	File         *filesystem.File   // File (for upload tasks)
	ParentTaskID *string            // Tracks the parent task (nullable for root tasks)
	Instruction  string             // Task instruction type ("src-traverse", "dst-traverse", etc.)
	Locked       bool               // Prevents race conditions (true if a worker has taken the task)
}

// NewTraversalTask initializes a new traversal task (folder only).
func NewTraversalTask(id string, folder filesystem.Folder, parentTaskID *string, instruction string) *Task {
	return &Task{
		ID:           id,
		Type:         TaskTraversal,
		Folder:       &folder,
		File:         nil,
		ParentTaskID: parentTaskID,
		Instruction:  instruction,
		Locked:       false, // Initially unlocked
	}
}

// NewUploadTask initializes a new upload task (file or folder).
func NewUploadTask(id string, file *filesystem.File, folder *filesystem.Folder, parentTaskID *string, instruction string) (*Task, error) {
	if file == nil && folder == nil {
		return nil, fmt.Errorf("cannot create upload task without file or folder")
	}

	return &Task{
		ID:           id,
		Type:         TaskUpload,
		Folder:       folder,
		File:         file,
		ParentTaskID: parentTaskID,
		Instruction:  instruction,
		Locked:       false, // Initially unlocked
	}, nil
}
