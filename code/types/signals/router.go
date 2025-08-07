// Package signals provides types for the SignalRouter event system.
package signals

import (
	"sync"
	"time"
)

// AckMode defines the acknowledgment mode for signals
type AckMode int

const (
	AckNone AckMode = iota // No acknowledgment required
	AckAny                 // Any subscriber can acknowledge
	AckAll                 // All subscribers must acknowledge
)

// Signal represents a message sent through the SignalRouter
type Signal struct {
	Topic     string        // The topic/channel for this signal
	Payload   any           // The data payload
	Ack       chan struct{} // Channel for acknowledgments
	AckMode   AckMode       // How acknowledgments should be handled
	Timestamp time.Time     // When the signal was created
	ID        string        // Unique identifier for the signal
}

// SignalRouter manages signal routing and delivery
type SignalRouter struct {
	Mu     sync.RWMutex         // Read-write mutex for thread safety
	Topics map[string]*TopicHub // Map of topic names to their hubs
}

// TopicHub manages subscribers for a specific topic
type TopicHub struct {
	Input       chan Signal   // Channel for incoming signals
	Subscribers []chan Signal // List of subscriber channels
	Mu          sync.RWMutex  // Mutex for subscriber list access
}

// SignalHandler is a function type for processing signals
type SignalHandler func(Signal)
