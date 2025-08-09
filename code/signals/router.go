package signals

import (
	"fmt"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/google/uuid"
)

type AckMode int

const (
	AckNone AckMode = iota
	AckAny
	AckAll
)

type Signal struct {
	Topic     string
	Payload   any
	Ack       chan struct{}
	AckMode   AckMode
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
	logging.GlobalLogger.LogSystem("info", "SignalRouter", "✅ SignalRouter initialized", nil)
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
		logging.GlobalLogger.LogSystem("debug", "SignalRouter", fmt.Sprintf("Initialized topic hub: %s", topic), nil)
	}

	hub.mu.Lock()
	defer hub.mu.Unlock()
	hub.subscribers = append(hub.subscribers, subscriber{mailbox: mailbox, actorID: uuid.New().String()})
	logging.GlobalLogger.LogSystem("trace", "SignalRouter", fmt.Sprintf("Subscribed mailbox to topic: %s", topic), map[string]any{
		"mailbox_size": cap(mailbox),
	})
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
		logging.GlobalLogger.LogSystem("warning", "SignalRouter", "Signal dropped (no hub exists)", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		})
		return
	}

	select {
	case hub.input <- sig:
		logging.GlobalLogger.LogSystem("trace", "SignalRouter", "Signal enqueued to topic", map[string]any{
			"topic":   sig.Topic,
			"id":      sig.ID,
			"ackMode": sig.AckMode,
		})
	default:
		logging.GlobalLogger.LogSystem("warning", "SignalRouter", "Signal dropped (hub input full)", map[string]any{
			"topic": sig.Topic,
			"id":    sig.ID,
		})
	}
}

func (sr *SignalRouter) PublishWithAck(sig Signal, timeout time.Duration) error {
	if sig.AckMode == AckNone {
		sr.Publish(sig)
		return nil
	}

	// Set timestamp if not already set
	if sig.Timestamp.IsZero() {
		sig.Timestamp = time.Now()
	}

	// Generate ID if not provided
	if sig.ID == "" {
		sig.ID = uuid.New().String()
	}

	sig.Ack = make(chan struct{})
	sr.Publish(sig)

	select {
	case <-sig.Ack:
		logging.GlobalLogger.LogSystem("debug", "SignalRouter", "Signal acknowledged", map[string]any{
			"topic":   sig.Topic,
			"id":      sig.ID,
			"ackMode": sig.AckMode,
		})
		return nil
	case <-time.After(timeout):
		logging.GlobalLogger.LogSystem("warning", "SignalRouter", "Signal ack timed out", map[string]any{
			"topic":   sig.Topic,
			"id":      sig.ID,
			"ackMode": sig.AckMode,
		})
		return fmt.Errorf("signal ack timed out: topic=%s ID=%s", sig.Topic, sig.ID)
	}
}

func (hub *topicHub) runFanOut(topic string) {
	for sig := range hub.input {
		hub.mu.RLock()
		subs := make([]subscriber, len(hub.subscribers))
		copy(subs, hub.subscribers)
		hub.mu.RUnlock()

		// Build and deliver per-subscriber signals
		var acks []ackEntry

		for i, s := range subs {
			sigToSend := sig
			if sig.AckMode != AckNone {
				sigToSend.Ack = make(chan struct{}, 1) // one-shot
				acks = append(acks, ackEntry{id: s.actorID, ch: sigToSend.Ack})
			}

			select {
			case s.mailbox <- sigToSend:
				logging.GlobalLogger.LogSystem("trace", "SignalRouter", "Signal delivered to subscriber", map[string]any{
					"topic":    topic,
					"id":       sig.ID,
					"subIndex": i,
				})
			default:
				logging.GlobalLogger.LogSystem("warning", "SignalRouter", "Mailbox full, dropping signal", map[string]any{
					"topic":    topic,
					"id":       sig.ID,
					"subIndex": i,
				})
			}
		}

		// Aggregate acknowledgments when required
		if sig.AckMode == AckAny || sig.AckMode == AckAll {
			go aggregateAcks(sig, acks)
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
			logging.GlobalLogger.LogSystem("trace", "SignalRouter", "Handler triggered", map[string]any{
				"topic": topic,
				"id":    sig.ID,
				"async": runAsync,
			})

			if runAsync {
				go handler(sig)
			} else {
				handler(sig)
			}
		}
	}()
}

type ackEntry struct {
	id string
	ch chan struct{}
}

func aggregateAcks(root Signal, acks []ackEntry) {
	if root.AckMode == AckNone || len(acks) == 0 {
		return
	}

	target := 1
	if root.AckMode == AckAll {
		target = len(acks)
	}

	seen := make(map[string]struct{}, len(acks))
	done := make(chan string, len(acks))

	for _, a := range acks {
		a := a
		go func() {
			<-a.ch
			done <- a.id
		}()
	}

	remaining := target
	for remaining > 0 {
		id := <-done
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		remaining--
	}

	if root.Ack != nil {
		select {
		case root.Ack <- struct{}{}:
		default:
		}
	}

	logging.GlobalLogger.LogSystem("debug", "SignalRouter", "Ack aggregation completed", map[string]any{
		"topic":  root.Topic,
		"id":     root.ID,
		"mode":   root.AckMode,
		"needed": target,
	})
}
