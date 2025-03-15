package cli

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
)

// Sender struct sends log messages via UDP.
type Sender struct {
	sessionID string
}

// NewSender initializes a log sender with a unique session ID.
func NewSender() *Sender {
	return &Sender{
		sessionID: generateSessionID(),
	}
}

// generateSessionID creates a unique identifier for the session.
func generateSessionID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%X", rand.Uint64())
}

// SendLog sends a log message via UDP asynchronously.
func (s *Sender) SendLog(level, message string, details map[string]any) {
	go func() {
		logEntry := core.LogEntry{
			Timestamp: time.Now().Format(time.RFC3339),
			Level:     level,
			Message:   message,
			Details:   details, // Include structured details
		}

		// Serialize log entry
		serialized, err := json.Marshal(logEntry)
		if err != nil {
			fmt.Println("❌ Error encoding log entry:", err)
			return
		}

		// Send log to UDP listener
		conn, err := net.Dial("udp", "127.0.0.1:9999")
		if err != nil {
			fmt.Println("❌ Error sending log via UDP:", err)
			return
		}
		defer conn.Close()

		_, writeErr := conn.Write(serialized)
		if writeErr != nil {
			fmt.Println("❌ Error writing to UDP connection:", writeErr)
			return
		}
	}()

	// Also push log to global logger (already async internally)
	core.GlobalLogger.LogMessage(level, message, details)
}
