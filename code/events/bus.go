package events

import (
	"sync"

	"github.com/Voltaic314/ByteWave/code/types/events"
)

type EventMessage struct {
	From      events.BusTarget
	To        events.BusTarget
	EventType string
	Payload   map[string]any
}

type subscriberChan chan EventMessage

type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]subscriberChan // group → list of channels
}

// The wheels on the bus go round and round...
// round and round...
// round and round...
// The wheels on the bus go round and round...
// All through the town! 
// Dear god someone help me I need sleep...
// NewEventBus creates the shared pub-sub bus
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]subscriberChan),
	}
}

// Subscribe returns a channel that receives all events for a group
func (bus *EventBus) Subscribe(group string) <-chan EventMessage {
	ch := make(subscriberChan, 100) // buffered for safety
	bus.mu.Lock()
	defer bus.mu.Unlock()
	bus.subscribers[group] = append(bus.subscribers[group], ch)
	return ch
}

// Emit sends an event to all subscribers of the ToGroup
func (bus *EventBus) Emit(msg EventMessage) {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	for _, ch := range bus.subscribers[msg.To.Group] {
		select {
		case ch <- msg:
		default:
			// If the buffer is full, drop the message silently
			// Could log or metric here if needed
		}
	}
}
