# Signal Router (SR) System

## Overview

The **Signal Router (SR)** in ByteWave is a centralized event signaling system designed to decouple communication between key components such as the Conductor, QueuePublisher, TaskQueues, and Workers.

It replaces hardcoded channel maps and point-to-point communication with a flexible, topic-based fan-out architecture that includes sophisticated acknowledgment handling.

Inspired by event buses and pub/sub patterns, the Signal Router allows for easier scaling, better maintainability, and cleaner control flow across ByteWave's coordination layers.

## Core Responsibilities

* **Topic-based routing**: Components can publish to and subscribe from named topics (e.g., `running_low:src`, `idle:dst`, `traversal:complete:phase2`).
* **Fan-out**: When a signal is published to a topic, all subscribers receive it.
* **Centralized control**: All cross-component signals are funneled through a single, importable global router (`signals.GlobalSR`).
* **Acknowledgment management**: Supports configurable acknowledgment modes for reliable signal delivery.

## Example Use Cases

* `QueuePublisher` listens for:

  * `running_low:{src/dst}`: to replenish tasks when queues dip below threshold.
  * `idle:{src/dst}`: to detect when all workers are idle and queues are dry.
  * `traversal:complete`: to know when all traversal queues are finished.

* `TaskQueues` emit signals such as:

  * `running_low:{src/dst}`
  * `idle:{src/dst}`

* `Conductor` may subscribe to global state signals to coordinate phase transitions.

---

## Benefits

### ✅ **Decouples Logic**

No more tightly coupled channel maps or direct channel ownership. Components only need to know the topic name to communicate.

### ✅ **Easier to Extend**

New signals and components can be integrated without changing a dozen places.

### ✅ **Improved Readability**

Control flow and event-driven behaviors are now centralized and observable through consistent topic naming.

### ✅ **Reduces Boilerplate**

No more maintaining multiple `chan int` maps or listener-specific logic in every struct.

### ✅ **Reliable Acknowledgment**

Built-in acknowledgment system prevents signal loss and enables coordinated multi-party communication.

---

## Current Implementation Details

* The SR is a singleton: `signals.GlobalSR`
* Topics are strings (e.g., `"running_low:src"`, `"idle:dst"`)
* **UPDATED**: Subscribers register handlers using:

  ```go
  signals.GlobalSR.On(topic, handler)
  ```
* Publishers can use acknowledgment modes:

  ```go
  signals.GlobalSR.PublishWithAck(signal, timeout)
  ```

### Acknowledgment System

The Signal Router now supports three acknowledgment modes:

* **`AckNone`**: No acknowledgments required (fire-and-forget)
* **`AckAny`**: Any single subscriber acknowledgment completes the signal
* **`AckAll`**: All subscribers must acknowledge before completion

**Owl Pattern**: For `AckAny` and `AckAll` modes, the system creates a middleman acknowledgment layer that:
- Assigns unique `actorID` to each subscriber
- Prevents duplicate acknowledgments from the same actor
- Ensures non-blocking operation
- Maintains clean separation between signal delivery and acknowledgment handling

### Signal Structure

```go
type Signal struct {
  Topic     string
  Payload   any
  Ack       chan struct{}  // Acknowledgment channel
  AckMode   AckMode        // How acks should be handled
  Timestamp time.Time
  ID        string
}
```

### Internals

* **UPDATED**: SR now stores `map[string]*topicHub` with structured subscribers
* Each subscriber has a unique `actorID` for acknowledgment deduplication
* Acknowledgment aggregation happens in dedicated goroutines to prevent blocking
* Subscriber management uses `[]subscriber` instead of raw channel slices

---

## Usage Examples

### Basic Signal Publishing

```go
// Fire-and-forget signal
signals.GlobalSR.Publish(signals.Signal{
    Topic:   "worker:started",
    Payload: workerID,
})

// Signal requiring acknowledgment
signal := signals.Signal{
    Topic:   "phase:complete",
    Payload: phaseNumber,
    AckMode: signals.AckAll,
    Ack:     make(chan struct{}),
}

err := signals.GlobalSR.PublishWithAck(signal, 30*time.Second)
if err != nil {
    // Handle timeout or error
}
```

### Handler Registration

```go
// Register a handler for a topic
signals.GlobalSR.On("queue:running_low", func(sig signals.Signal) {
    queueName := sig.Payload.(string)
    // Handle running low condition
    
    // Acknowledge if required
    if sig.Ack != nil {
        close(sig.Ack)
    }
})
```

---

## Potential Future Improvements

### 🔁 Buffered Replay Layer

Maintain a fixed-size buffer of past N signals per topic (with timestamps), allowing:

* Late subscribers to replay recent events
* Optional backfilling for new workers spawned after a signal was already sent

### 🕒 Timestamp-Based Filtering

Let subscribers optionally fetch signals sent *after* a given timestamp (e.g., for phase recovery or missed events).

### 📦 Enhanced Signal Validation

Move from `any` payloads to strongly typed payloads with validation:

```go
type PhaseCompletePayload struct {
    Phase     int
    QueueName string
    Timestamp time.Time
}
```

### ⏱ Timeout Subscriptions

Allow one-time or time-bounded subscriptions (e.g., "Wait for signal X within 30s").

### 📋 Debug Mode

Emit logs or stats for:

* Total signals sent per topic
* Number of active subscribers
* Missed signals (if buffer tracking is implemented)
* Acknowledgment timing and success rates

### 🔄 Signal Routing Rules

Advanced routing based on payload content, subscriber metadata, or dynamic conditions.

---

## Summary

The Signal Router is a key architectural improvement to ByteWave's coordination model. It removes brittle channel wiring in favor of a clean, centralized abstraction that scales better and is easier to reason about.

The new acknowledgment system adds reliability and coordination capabilities, making it suitable for complex multi-party workflows where signal delivery confirmation is critical.

It plays a foundational role in enabling phase-based coordination, worker queue responsiveness, and dynamic task orchestration — and leaves room for future enhancements like signal replay, observability, and conditional routing.

---

## Migration Notes

If you're updating existing code from the old channel-based system:

1. Replace `signals.GlobalSR.Subscribe(topic, handlerChan)` with `signals.GlobalSR.On(topic, handler)`
2. Update signal handling to use the new `Signal` struct format
3. Add acknowledgment handling where required using `sig.Ack` channel
4. Use `PublishWithAck` for signals that require confirmation
