package processing

import (
	"fmt"

	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
)

// TaskType defines the type of work this task is performing.
type TaskType string

const (
	TaskSrcTraversal TaskType = "src_traversal"
	TaskDstTraversal TaskType = "dst_traversal"
	TaskUpload       TaskType = "upload"
)

// Task interface that all task types must implement
type Task interface {
	GetID() string
	GetType() TaskType
	GetPath() string
	GetFolder() *filesystem.Folder
	GetFile() *filesystem.File
	GetParentTaskID() *string
	IsLocked() bool
	SetLocked(bool)
	// In-memory retry tracking methods
	GetAttempts() int
	IncrementAttempts()
	CanRetry(maxAttempts int) bool
}

// BaseTask contains common fields for all task types
type BaseTask struct {
	ID           string             // Unique task identifier
	Type         TaskType           // Task type (src_traversal, dst_traversal, upload)
	Folder       *filesystem.Folder // Pointer to Folder (nullable for upload tasks)
	File         *filesystem.File   // Pointer to File (nullable for traversal tasks)
	ParentTaskID *string            // Parent task ID (nullable for root tasks)
	Locked       bool               // Lock state to prevent race conditions
	AttemptCount int                // In-memory retry counter
}

// TraversalSrcTask represents a source traversal task
type TraversalSrcTask struct {
	BaseTask
}

// TraversalDstTask represents a destination traversal task with expected source children
type TraversalDstTask struct {
	BaseTask
	ExpectedSrcChildren []*filesystem.Folder // Expected source children for comparison
	ExpectedSrcFiles    []*filesystem.File   // Expected source files for comparison
}

// UploadTask represents an upload task
type UploadTask struct {
	BaseTask
}

// Interface implementation for BaseTask
func (bt *BaseTask) GetID() string                 { return bt.ID }
func (bt *BaseTask) GetType() TaskType             { return bt.Type }
func (bt *BaseTask) GetFolder() *filesystem.Folder { return bt.Folder }
func (bt *BaseTask) GetFile() *filesystem.File     { return bt.File }
func (bt *BaseTask) GetParentTaskID() *string      { return bt.ParentTaskID }
func (bt *BaseTask) IsLocked() bool                { return bt.Locked }
func (bt *BaseTask) SetLocked(locked bool)         { bt.Locked = locked }

// In-memory retry tracking methods
func (bt *BaseTask) GetAttempts() int {
	return bt.AttemptCount
}

func (bt *BaseTask) IncrementAttempts() {
	bt.AttemptCount++
}

func (bt *BaseTask) CanRetry(maxAttempts int) bool {
	return bt.AttemptCount < maxAttempts
}

func (bt *BaseTask) GetPath() string {
	if bt.Folder != nil {
		return bt.Folder.Path
	}
	if bt.File != nil {
		return bt.File.Path
	}
	return ""
}

func NewSrcTraversalTask(id string, folder *filesystem.Folder, parentTaskID *string) (*TraversalSrcTask, error) {

	return &TraversalSrcTask{
		BaseTask: BaseTask{
			ID:           id,
			Type:         TaskSrcTraversal,
			Folder:       folder,
			File:         nil,
			ParentTaskID: parentTaskID,
			Locked:       false,
			AttemptCount: 0,
		},
	}, nil
}

// NewUploadTask initializes a new upload task (file or folder).
func NewUploadTask(id string, file *filesystem.File, folder *filesystem.Folder, parentTaskID *string) (*UploadTask, error) {
	logging.GlobalLogger.Log("info", "System", "Task", "Creating new upload task", map[string]any{
		"taskID":    id,
		"hasFile":   file != nil,
		"hasFolder": folder != nil,
		"hasParent": parentTaskID != nil,
	}, "CREATING_NEW_UPLOAD_TASK", "upload",
	)

	if file == nil && folder == nil {
		logging.GlobalLogger.Log("error", "System", "Task", "Cannot create upload task without file or folder", map[string]any{
			"taskID": id,
		}, "CREATING_NEW_UPLOAD_TASK", "upload",
		)
		return nil, fmt.Errorf("cannot create upload task without file or folder")
	}

	task := &UploadTask{
		BaseTask: BaseTask{
			ID:           id,
			Type:         TaskUpload,
			Folder:       folder,
			File:         file,
			ParentTaskID: parentTaskID,
			Locked:       false,
			AttemptCount: 0,
		},
	}

	logging.GlobalLogger.Log("info", "System", "Task", "Upload task created successfully", map[string]any{
		"taskID": id,
		"path":   task.GetPath(),
	}, "UPLOAD_TASK_CREATED_SUCCESSFULLY", "upload",
	)
	return task, nil
}

// NewDstTraversalTask creates a task specifically for destination traversal
func NewDstTraversalTask(id string, folder *filesystem.Folder, parentTaskID *string,
	expectedSrcChildren []*filesystem.Folder, expectedSrcFiles []*filesystem.File) (*TraversalDstTask, error) {

	return &TraversalDstTask{
		BaseTask: BaseTask{
			ID:           id,
			Type:         TaskDstTraversal,
			Folder:       folder,
			File:         nil,
			ParentTaskID: parentTaskID,
			Locked:       false,
			AttemptCount: 0,
		},
		ExpectedSrcChildren: expectedSrcChildren,
		ExpectedSrcFiles:    expectedSrcFiles,
	}, nil
}
