package cli

import (
	"encoding/json"
	"fmt"
	"net"
)

// Receiver struct listens for log messages via UDP.
type Receiver struct {
	listenAddr string
	listenPort int
}

// NewReceiver initializes a log receiver.
func NewReceiver() *Receiver {
	return &Receiver{
		listenAddr: "127.0.0.1",
		listenPort: 9999,
	}
}

// StartListener begins listening for incoming logs via UDP.
func (r *Receiver) StartListener() {
	addr := fmt.Sprintf("%s:%d", r.listenAddr, r.listenPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Println("❌ Error resolving UDP address:", err)
		return
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println("❌ Error starting UDP listener:", err)
		return
	}
	defer conn.Close()

	fmt.Println("📡 Log listener started on", addr)

	buffer := make([]byte, 4096) // Increased buffer size for structured logs
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("❌ Error reading from UDP:", err)
			continue
		}

		var logEntry map[string]interface{}
		if err := json.Unmarshal(buffer[:n], &logEntry); err != nil {
			fmt.Println("❌ Error decoding log entry:", err)
			continue
		}

		// Print formatted log message with details
		fmt.Printf("[%s] %s: %s\n", logEntry["timestamp"], logEntry["level"], logEntry["message"])
		if details, exists := logEntry["details"]; exists {
			fmt.Printf("   ➡️ Details: %v\n", details)
		}
	}
}
