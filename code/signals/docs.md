# Signal Router (SR) System

## Overview

The **Signal Router (SR)** in ByteWave is a centralized event signaling system designed to decouple communication between key components such as the Conductor, QueuePublisher, TaskQueues, and Workers.

It replaces hardcoded channel maps and point-to-point communication with a flexible, topic-based fan-out architecture.

Inspired by event buses and pub/sub patterns, the Signal Router allows for easier scaling, better maintainability, and cleaner control flow across ByteWave's coordination layers.

## Core Responsibilities

* **Topic-based routing**: Components can publish to and subscribe from named topics (e.g., `running_low:src`, `idle:dst`, `traversal:complete:phase2`).
* **Fan-out**: When a signal is published to a topic, all subscribers receive it.
* **Centralized control**: All cross-component signals are funneled through a single, importable global router (`signals.GlobalSR`).

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

---

## Current Implementation Details

* The SR is a singleton: `signals.GlobalSR`

* Topics are strings (e.g., `"running_low:src"`, `"idle:dst"`)

* **UPDATED**: Subscribers register handlers using:

  ```go
  signals.GlobalSR.On(topic, handler)
  ```

* Subscribers can also be explicitly removed using:

  ```go
  signals.GlobalSR.Unsubscribe(topic, actorID)
  ```

* Entire topics can be closed (tearing down their input loop and dropping all subscribers) using:

  ```go
  signals.GlobalSR.CloseTopic(topic)
  ```

### Signal Structure

```go
type Signal struct {
  Topic     string
  Payload   any
  Timestamp time.Time
  ID        string
}
```

### Internals

* **UPDATED**: SR now stores `map[string]*topicHub` with structured subscribers.
* Each subscriber has:

  * A unique `actorID` (UUID) for identification and removal.
  * A mailbox channel for delivering signals.
* `Unsubscribe` removes a single subscriber by `actorID`.
* `CloseTopic` closes the hub for a topic and removes all subscribers at once.

---

## Usage Examples

### Basic Signal Publishing

```go
// Fire-and-forget signal
signals.GlobalSR.Publish(signals.Signal{
    Topic:   "worker:started",
    Payload: workerID,
})
```

### Handler Registration

```go
// Register a handler for a topic
signals.GlobalSR.On("queue:running_low", func(sig signals.Signal) {
    queueName := sig.Payload.(string)
    // Handle running low condition
})
```

### Explicit Unsubscribe

```go
// Subscribe manually, capture actorID
actorID := signals.GlobalSR.On("queue:idle", func(sig signals.Signal) {
    fmt.Println("Queue idle detected!")
})

// Later, unsubscribe when the worker shuts down
signals.GlobalSR.Unsubscribe("queue:idle", actorID)
```

### Close a Topic

```go
// Conductor can close an entire topic when a queue is torn down
signals.GlobalSR.CloseTopic("queue:running_low")
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

It plays a foundational role in enabling phase-based coordination, worker queue responsiveness, and dynamic task orchestration — and leaves room for future enhancements like signal replay, observability, and conditional routing.
