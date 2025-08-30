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
	subscribers []subscriber
	mu          sync.RWMutex
}

type subscriber struct {
	mailbox chan Signal
	actorID string
}

var GlobalSR *SignalRouter

func InitSignalRouter() {
	GlobalSR = &SignalRouter{
		topics: make(map[string]*topicHub),
	}
	logging.GlobalLogger.Log("info", "System", "SignalRouter", "✅ SignalRouter initialized", nil, "SIGNAL_ROUTER_INITIALIZED", "None",
	)
}

func (sr *SignalRouter) Subscribe(topic string, mailbox chan Signal) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	hub, exists := sr.topics[topic]
	if !exists {
		hub = &topicHub{
			input:       make(chan Signal, 64),
			subscribers: make([]subscriber, 0),
		}
		sr.topics[topic] = hub
		go hub.runFanOut(topic)
		logging.GlobalLogger.Log("debug", "System", "SignalRouter", fmt.Sprintf("Initialized topic hub: %s", topic), nil, "INITIALIZED_TOPIC_HUB", "None",
		)
	}

	hub.mu.Lock()
	defer hub.mu.Unlock()
	hub.subscribers = append(hub.subscribers, subscriber{mailbox: mailbox, actorID: uuid.New().String()})
	logging.GlobalLogger.Log("trace", "System", "SignalRouter", fmt.Sprintf("Subscribed mailbox to topic: %s", topic), map[string]any{
		"mailbox_size": cap(mailbox),
	}, "SUBSCRIBED_MAILBOX_TO_TOPIC", "None",
	)
}

func (sr *SignalRouter) Publish(sig Signal) {
	// Set timestamp if not already set
	if sig.Timestamp.IsZero() {
		sig.Timestamp = time.Now()
	}

	// Generate ID if not provided
	if sig.ID == "" {
		sig.ID = uuid.New().String()
	}

	sr.mu.RLock()
	defer sr.mu.RUnlock()

	hub, exists := sr.topics[sig.Topic]
	if !exists {
		logging.GlobalLogger.Log("warning", "System", "SignalRouter", "Signal dropped (no hub exists)", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		}, "SIGNAL_DROPPED_NO_HUB_EXISTS", "None",
		)
		return
	}

	select {
	case hub.input <- sig:
		logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Signal enqueued to topic", map[string]any{
			"topic":   sig.Topic,
			"id":      sig.ID,
		}, "SIGNAL_ENQUEUED_TO_TOPIC", "None",
		)
	default:
		logging.GlobalLogger.Log("warning", "System", "SignalRouter", "Signal dropped (hub input full)", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		}, "SIGNAL_DROPPED_HUB_INPUT_FULL", "None",
		)
	}
}

func (hub *topicHub) runFanOut(topic string) {
	for sig := range hub.input {
		hub.mu.RLock()
		subs := make([]subscriber, len(hub.subscribers))
		copy(subs, hub.subscribers)
		hub.mu.RUnlock()


		for i, s := range subs {
			sigToSend := sig

			select {
			case s.mailbox <- sigToSend:
				logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Signal delivered to subscriber", map[string]any{
					"topic":    topic,
					"id":       sig.ID,
					"subIndex": i,
				}, "SIGNAL_DELIVERED_TO_SUBSCRIBER", "None",
				)
			default:
				logging.GlobalLogger.Log("warning", "System", "SignalRouter", "Mailbox full, dropping signal", map[string]any{
					"topic":    topic,
					"id":       sig.ID,
					"subIndex": i,
				}, "MAILBOX_FULL_DROPPING_SIGNAL", "None",
				)
			}
		}
	}
}

// On registers a handler for a topic. If async is true, handler runs in a separate goroutine.
func (sr *SignalRouter) On(topic string, handler func(Signal), async ...bool) {
	ch := make(chan Signal, 8)
	sr.Subscribe(topic, ch)

	runAsync := len(async) > 0 && async[0]

	go func() {
		for sig := range ch {
			logging.GlobalLogger.Log("trace", "System", "SignalRouter", "Handler triggered", map[string]any{
				"topic": topic,
				"id":    sig.ID,
				"async": runAsync,
			}, "HANDLER_TRIGGERED", "None",
			)

			if runAsync {
				go handler(sig)
			} else {
				handler(sig)
			}
		}
	}()
}