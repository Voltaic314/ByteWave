package processing

import (
	"time"
	"fmt"
	"github.com/Voltaic314/Data_Migration_Tool/responsehandler"
	"github.com/Voltaic314/Data_Migration_Tool/filesystem"
)

// TaskType defines the type of work this task is performing.
type TaskType string

const (
	TaskTraversal TaskType = "traversal"
	TaskUpload    TaskType = "upload"
)

// TaskStatus defines the current status of a task.
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusInProgress TaskStatus = "in_progress"
	StatusFailed     TaskStatus = "failed"
	StatusSuccess    TaskStatus = "success"
)

// Task represents a single unit of work in the processing system.
type Task struct {
	ID            string                 // Unique task identifier
	Type          TaskType               // Task type (traversal, upload)
	Folder        *filesystem.Folder     // Folder (for traversal tasks)
	File          *filesystem.File       // File (for upload tasks)
	Instructions  string                 // Human-readable instructions
	Status        TaskStatus             // Task execution status
	RetryAttempts int                    // Number of times retried
	MaxRetries    int                    // Maximum retries before failure
	LastAttempt   time.Time              // Last attempt timestamp
	ErrorIDs      []string               // Associated error IDs
	Metadata      map[string]interface{} // Additional metadata (pagination, etc.)
}

// NewTraversalTask initializes a new traversal task (folder only).
func NewTraversalTask(id string, folder filesystem.Folder, maxRetries int) *Task {
	return &Task{
		ID:            id,
		Type:          TaskTraversal,
		Folder:        &folder,
		File:          nil,
		Instructions:  "ListContents", // Traversal always lists contents :) 
		Status:        StatusPending,
		RetryAttempts: 0,
		MaxRetries:    maxRetries,
		LastAttempt:   time.Time{},
		ErrorIDs:      []string{},
		Metadata:      make(map[string]interface{}),
	}
}

// NewUploadTask initializes a new upload task (file or folder).
func NewUploadTask(id string, file *filesystem.File, folder *filesystem.Folder, maxRetries int) (*Task, error) {
	if file == nil && folder == nil {
		return nil, fmt.Errorf("cannot create upload task without file or folder")
	}

	var instruction string
	switch {
	case file != nil:
		instruction = "CreateFile"
	case folder != nil:
		instruction = "CreateFolder"
	default:
		instruction = "Unknown"
	}

	return &Task{
		ID:            id,
		Type:          TaskUpload,
		Folder:        folder,
		File:          file,
		Instructions:  instruction,
		Status:        StatusPending,
		RetryAttempts: 0,
		MaxRetries:    maxRetries,
		LastAttempt:   time.Time{},
		ErrorIDs:      []string{},
		Metadata:      make(map[string]interface{}),
	}, nil
}

// MarkInProgress updates the task status to "in_progress".
func (t *Task) MarkInProgress() {
	t.Status = StatusInProgress
	t.LastAttempt = time.Now()
}

// MarkSuccess updates the task status to "success".
func (t *Task) MarkSuccess() {
	t.Status = StatusSuccess
}

// MarkFailed logs an error and increments retry count.
func (t *Task) MarkFailed(errorID string) {
	t.Status = StatusFailed
	t.RetryAttempts++
	t.LastAttempt = time.Now()
	t.ErrorIDs = append(t.ErrorIDs, errorID)
}

// CanRetry checks if the task can be retried.
func (t *Task) CanRetry() bool {
	return t.RetryAttempts < t.MaxRetries
}

// AddMetadata attaches additional info to the task.
func (t *Task) AddMetadata(key string, value interface{}) {
	t.Metadata[key] = value
}

// ToResponse converts a task to a Response object.
func (t *Task) ToResponse() responsehandler.Response {
	return responsehandler.NewResponse(true, t, nil, nil)
}
