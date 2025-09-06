package signals

import (
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/google/uuid"
)

type Signal struct {
	Topic     string
	Payload   any
	Timestamp time.Time
	ID        string
}

type SignalRouter struct {
	mu     sync.RWMutex
	topics map[string]*topicHub
}

type topicHub struct {
	input       chan Signal
	subscribers map[string]subscriber // keyed by actorID for easy Unsubscribe
	mu          sync.RWMutex
	closed      bool
}

type subscriber struct {
	mailbox chan Signal
	actorID string
}

// UnsubscribeResult provides details about an unsubscribe attempt
type UnsubscribeResult struct {
	Found   bool
	Removed bool
}

var GlobalSR *SignalRouter

func InitSignalRouter() {
	GlobalSR = &SignalRouter{
		topics: make(map[string]*topicHub),
	}
	logging.GlobalLogger.Log("info", "System", "SignalRouter", "✅ SignalRouter initialized", nil, "SIGNAL_ROUTER_INITIALIZED", "None")
}

// Subscribe assigns a mailbox (PO box) to an entity for a topic
// Returns actorID for the mailbox
// Optional mailboxSize parameter (default: 1)
func (sr *SignalRouter) Subscribe(topic string, mailboxSize ...int) string {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	hub, exists := sr.topics[topic]
	if !exists {
		hub = &topicHub{
			input:       make(chan Signal, 64),
			subscribers: make(map[string]subscriber),
			closed:      false,
		}
		sr.topics[topic] = hub
		go hub.runFanOut(topic)
		logging.GlobalLogger.Log("debug", "System", "SignalRouter", "Initialized topic hub", map[string]any{
			"topic": topic,
		}, "INITIALIZED_TOPIC_HUB", "None")
	}

	hub.mu.Lock()
	defer hub.mu.Unlock()
	actorID := uuid.New().String()

	// Use provided mailbox size or default to 1
	size := 1
	if len(mailboxSize) > 0 && mailboxSize[0] > 0 {
		size = mailboxSize[0]
	}

	mailbox := make(chan Signal, size) // SR manages the channel internally
	hub.subscribers[actorID] = subscriber{mailbox: mailbox, actorID: actorID}

	logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Mailbox assigned", map[string]any{
		"topic":       topic,
		"actorID":     actorID,
		"mailboxSize": size,
	}, "MAILBOX_ASSIGNED", "None")

	return actorID
}

// CheckMail checks mailbox for new mail (non-blocking)
// Returns signal if available, nil if no mail
func (sr *SignalRouter) CheckMail(topic string, actorID string) *Signal {
	sr.mu.RLock()
	hub, exists := sr.topics[topic]
	sr.mu.RUnlock()
	if !exists {
		return nil
	}

	hub.mu.RLock()
	sub, exists := hub.subscribers[actorID]
	hub.mu.RUnlock()
	if !exists {
		return nil
	}

	select {
	case sig := <-sub.mailbox:
		return &sig
	default:
		return nil // No mail
	}
}

// WaitForMail waits for mail in mailbox (blocking)
// Returns signal when available, or nil if topic/mailbox was closed
func (sr *SignalRouter) WaitForMail(topic string, actorID string) *Signal {
	sr.mu.RLock()
	hub, exists := sr.topics[topic]
	sr.mu.RUnlock()
	if !exists {
		return nil
	}

	hub.mu.RLock()
	sub, exists := hub.subscribers[actorID]
	hub.mu.RUnlock()
	if !exists {
		return nil
	}

	sig, ok := <-sub.mailbox
	if !ok {
		return nil // Channel closed
	}
	return &sig
}

// Unsubscribe removes a subscriber by actorID from a topic.
// Returns a result struct indicating whether it was found/removed and an error if something went wrong.
func (sr *SignalRouter) Unsubscribe(topic string, actorID string) (UnsubscribeResult, error) {
	sr.mu.RLock()
	hub, exists := sr.topics[topic]
	sr.mu.RUnlock()
	if !exists {
		return UnsubscribeResult{Found: false, Removed: false}, nil
	}

	hub.mu.Lock()
	defer hub.mu.Unlock()

	sub, ok := hub.subscribers[actorID]
	if !ok {
		return UnsubscribeResult{Found: false, Removed: false}, nil
	}

	// Close the mailbox channel and remove from subscribers
	close(sub.mailbox)
	delete(hub.subscribers, actorID)

	logging.GlobalLogger.Log("debug", "System", "SignalRouter", "Unsubscribed actor from topic", map[string]any{
		"topic":   topic,
		"actorID": actorID,
	}, "UNSUBSCRIBED_ACTOR_FROM_TOPIC", "None")

	return UnsubscribeResult{Found: true, Removed: true}, nil
}

func (sr *SignalRouter) Publish(sig Signal) {
	if sig.Timestamp.IsZero() {
		sig.Timestamp = time.Now()
	}
	if sig.ID == "" {
		sig.ID = uuid.New().String()
	}

	// log that we are sending a signal
	logging.GlobalLogger.Log("debug", "System", "SignalRouter", "Sending signal", map[string]any{
		"topic":   sig.Topic,
		"id":      sig.ID,
		"payload": sig.Payload,
	}, "SENDING_SIGNAL", "None")

	sr.mu.RLock()
	hub, exists := sr.topics[sig.Topic]
	sr.mu.RUnlock()
	if !exists {
		logging.GlobalLogger.Log("warning", "System", "SignalRouter", "Signal dropped (no hub exists)", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		}, "SIGNAL_DROPPED_NO_HUB_EXISTS", "None")
		return
	}

	select {
	case hub.input <- sig:
		logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Signal enqueued to topic", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		}, "SIGNAL_ENQUEUED_TO_TOPIC", "None")
	default:
		logging.GlobalLogger.Log("warning", "System", "SignalRouter", "Signal dropped (hub input full)", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		}, "SIGNAL_DROPPED_HUB_INPUT_FULL", "None")
	}
}

func (hub *topicHub) runFanOut(topic string) {
	for sig := range hub.input {
		hub.mu.RLock()
		if hub.closed {
			hub.mu.RUnlock()
			return
		}
		subs := make([]subscriber, 0, len(hub.subscribers))
		for _, s := range hub.subscribers {
			subs = append(subs, s)
		}
		hub.mu.RUnlock()

		for i, s := range subs {
			sigToSend := sig
			select {
			case s.mailbox <- sigToSend:
				logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Signal delivered to subscriber", map[string]any{
					"topic":    topic,
					"id":       sig.ID,
					"subIndex": i,
					"actorID":  s.actorID,
				}, "SIGNAL_DELIVERED_TO_SUBSCRIBER", "None")
			default:
				logging.GlobalLogger.Log("warning", "System", "SignalRouter", "Mailbox full, dropping signal", map[string]any{
					"topic":    topic,
					"id":       sig.ID,
					"subIndex": i,
					"actorID":  s.actorID,
				}, "MAILBOX_FULL_DROPPING_SIGNAL", "None")
			}
		}
	}
}

func (sr *SignalRouter) On(topic string, handler func(Signal), async ...bool) string {
	actorID := sr.Subscribe(topic)
	runAsync := len(async) > 0 && async[0]

	go func() {
		for {
			// Wait for mail (blocking)
			sig := sr.WaitForMail(topic, actorID)
			if sig == nil {
				// Topic closed or mailbox removed
				return
			}

			logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Handler triggered", map[string]any{
				"topic":   topic,
				"id":      sig.ID,
				"async":   runAsync,
				"actorID": actorID,
			}, "HANDLER_TRIGGERED", "None")

			if runAsync {
				go handler(*sig)
			} else {
				handler(*sig)
			}
		}
	}()

	return actorID
}

// CloseTopic closes a single topic and all its subscribers.
func (sr *SignalRouter) CloseTopic(topic string) {
	sr.mu.Lock()
	hub, exists := sr.topics[topic]
	if exists {
		delete(sr.topics, topic)
	}
	sr.mu.Unlock()

	if !exists {
		return
	}

	hub.mu.Lock()
	if !hub.closed {
		hub.closed = true
		close(hub.input)
		// Close all subscriber mailboxes
		for _, sub := range hub.subscribers {
			close(sub.mailbox)
		}
		hub.subscribers = make(map[string]subscriber)
		logging.GlobalLogger.Log("info", "System", "SignalRouter", "Topic closed", map[string]any{
			"topic": topic,
		}, "TOPIC_CLOSED", "None")
	}
	hub.mu.Unlock()
}

// Close shuts down the entire router and all topics.
func (sr *SignalRouter) Close() {
	sr.mu.Lock()
	topics := sr.topics
	sr.topics = make(map[string]*topicHub)
	sr.mu.Unlock()

	for name, hub := range topics {
		hub.mu.Lock()
		if !hub.closed {
			hub.closed = true
			close(hub.input)
			// Close all subscriber mailboxes
			for _, sub := range hub.subscribers {
				close(sub.mailbox)
			}
			hub.subscribers = make(map[string]subscriber)
			logging.GlobalLogger.Log("info", "System", "SignalRouter", "Topic closed during global shutdown", map[string]any{
				"topic": name,
			}, "TOPIC_CLOSED_GLOBAL", "None")
		}
		hub.mu.Unlock()
	}

	logging.GlobalLogger.Log("info", "System", "SignalRouter", "✅ Global SignalRouter closed", nil, "SIGNAL_ROUTER_CLOSED", "None")
}
