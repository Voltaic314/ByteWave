package signals

import (
	"fmt"
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

func (sr *SignalRouter) Subscribe(topic string, mailbox chan Signal) string {
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
		logging.GlobalLogger.Log("debug", "System", "SignalRouter", fmt.Sprintf("Initialized topic hub: %s", topic), nil, "INITIALIZED_TOPIC_HUB", "None")
	}

	hub.mu.Lock()
	defer hub.mu.Unlock()
	actorID := uuid.New().String()
	hub.subscribers[actorID] = subscriber{mailbox: mailbox, actorID: actorID}

	logging.GlobalLogger.Log("trace", "System", "SignalRouter", fmt.Sprintf("Subscribed mailbox to topic: %s", topic), map[string]any{
		"mailbox_size": cap(mailbox),
		"actorID":      actorID,
	}, "SUBSCRIBED_MAILBOX_TO_TOPIC", "None")

	return actorID
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

	_, ok := hub.subscribers[actorID]
	if !ok {
		return UnsubscribeResult{Found: false, Removed: false}, nil
	}

	delete(hub.subscribers, actorID)
	if _, stillExists := hub.subscribers[actorID]; stillExists {
		// Extremely unlikely, but let's be explicit
		return UnsubscribeResult{Found: true, Removed: false}, fmt.Errorf("failed to remove actorID %s from topic %s", actorID, topic)
	}

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
	ch := make(chan Signal, 8)
	actorID := sr.Subscribe(topic, ch)
	runAsync := len(async) > 0 && async[0]

	go func() {
		for sig := range ch {
			logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Handler triggered", map[string]any{
				"topic": topic,
				"id":    sig.ID,
				"async": runAsync,
			}, "HANDLER_TRIGGERED", "None")

			if runAsync {
				go handler(sig)
			} else {
				handler(sig)
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
		hub.subscribers = make(map[string]subscriber) // drop subscribers
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
			hub.subscribers = make(map[string]subscriber)
			logging.GlobalLogger.Log("info", "System", "SignalRouter", "Topic closed during global shutdown", map[string]any{
				"topic": name,
			}, "TOPIC_CLOSED_GLOBAL", "None")
		}
		hub.mu.Unlock()
	}

	logging.GlobalLogger.Log("info", "System", "SignalRouter", "✅ Global SignalRouter closed", nil, "SIGNAL_ROUTER_CLOSED", "None")
}
