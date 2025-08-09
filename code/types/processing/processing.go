// Package processing provides types for task processing and queue management.
package processing

import (
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/types/filesystem"
)

// QueueType defines whether a queue is for traversal or uploading
type QueueType string

const (
	TraversalQueueType QueueType = "traversal"
	UploadQueueType    QueueType = "upload"
)

// QueueState defines the possible states of a queue
type QueueState string

const (
	QueueRunning  QueueState = "running"
	QueuePaused   QueueState = "paused"
	QueueComplete QueueState = "complete"
)

// TaskType defines the type of work a task is performing
type TaskType string

const (
	TaskTraversal TaskType = "traversal"
	TaskUpload    TaskType = "upload"
)

// WorkerState defines the possible states of a worker
type WorkerState string

const (
	WorkerIdle   WorkerState = "idle"
	WorkerActive WorkerState = "active"
)

// Task represents a unit of work to be processed
type Task struct {
	ID           string
	Type         TaskType
	Folder       *filesystem.Folder
	File         *filesystem.File
	ParentTaskID *string
	Locked       bool
}

// TaskQueue manages tasks, workers, and execution settings
type TaskQueue struct {
	QueueID             string
	Type                QueueType
	Phase               int
	SrcOrDst            string
	Tasks               []*Task
	Workers             []*WorkerBase
	Mu                  sync.Mutex
	PaginationSize      int
	PaginationChan      chan int
	GlobalSettings      map[string]any
	State               QueueState
	Cond                *sync.Cond
	SignalTopicBase     string
	RunningLowTriggered bool
	RunningLowThreshold int
}

// WorkerBase provides shared fields and logic for all worker types
type WorkerBase struct {
	ID        string
	Queue     *TaskQueue
	QueueType string
	State     WorkerState
	TaskReady bool
}

// PollingController manages soft polling state per queue
type PollingController struct {
	IsPolling  bool
	CancelFunc interface{} // context.CancelFunc
	Interval   time.Duration
	Mutex      sync.Mutex
}

// QueuePublisher manages multiple queues dynamically
type QueuePublisher struct {
	DB                 interface{} // *db.DB
	Queues             map[string]*TaskQueue
	QueueLevels        map[string]int
	Mutex              sync.Mutex
	Running            bool
	LastPathCursors    map[string]string
	RetryThreshold     int
	BatchSize          int
	PollingControllers map[string]*PollingController
	QueriesPerPhase    map[string]int
	ScanModes          map[string]int // scanMode
}

// Conductor manages the overall processing system
type Conductor struct {
	DB             interface{} // *db.DB
	QP             *QueuePublisher
	RetryThreshold int
	BatchSize      int
}
