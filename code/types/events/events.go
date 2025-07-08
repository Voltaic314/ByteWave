// Package events provides shared types for event-driven communication across components.
package events

type BusTarget struct {
	Group        string
	ID           *string // if targeting a specific instance
	BroadcastAll bool    // if true, ignore ID and send to entire group
}

// EventMessage represents a generic message that can be passed between components via the event bus.
type EventMessage struct {
	From      BusTarget
	To        BusTarget
	EventType string
	Payload   map[string]any
}

// Event types (constants)
const (
	EventSync          = "sync"
	EventRetry         = "retry"
	EventPhaseComplete = "phase_complete"
	EventTraversalDone = "traversal_done"
	EventFlush         = "flush"
	EventPauseWorker   = "pause_worker"
	EventResumeWorker  = "resume_worker"
)
