package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"runtime"
)

// Receiver struct listens for log messages via UDP.
type Receiver struct {
	listenAddr string
	listenPort int
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewReceiver initializes a log receiver.
func NewReceiver() *Receiver {
	ctx, cancel := context.WithCancel(context.Background())
	return &Receiver{
		listenAddr: "127.0.0.1",
		listenPort: 9999,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// SpawnReceiverTerminal opens a second terminal to run the log receiver.
func SpawnReceiverTerminal() error {
	if runtime.GOOS != "windows" {
		fmt.Println("⚠️ Terminal spawning only implemented for Windows")
		return nil
	}

	cmd := exec.Command("C:\\Windows\\System32\\cmd.exe", "/C", "start", "cmd", "/K", "go run ./code/cli/main/open_terminal.go")
	return cmd.Start()
}

// StartListener begins listening for incoming logs via UDP asynchronously.
func (r *Receiver) StartListener() {
	go func() {
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
			select {
			case <-r.ctx.Done():
				fmt.Println("🛑 Log listener shutting down...")
				return
			default:
				n, _, err := conn.ReadFromUDP(buffer)
				if err != nil {
					fmt.Println("❌ Error reading from UDP:", err)
					continue
				}

				var logEntry map[string]any
				if err := json.Unmarshal(buffer[:n], &logEntry); err != nil {
					fmt.Println("❌ Error decoding log entry:", err)
					continue
				}

				// Print formatted log message with details
				fmt.Printf("[%s] %s: %s\n", logEntry["timestamp"], logEntry["level"], logEntry["message"])
				if details, exists := logEntry["details"]; exists {
					jsonDetails, err := json.MarshalIndent(details, "   ", "  ")
					if err != nil {
						fmt.Printf("   ⚠️  Details decode failed: %v\n", err)
					} else {
						fmt.Printf("   ➡️ Details:\n   %s\n", jsonDetails)
					}
				}
				
			}
		}
	}()
}

// StartReceiverLoop runs the listener in the foreground and blocks.
func StartReceiverLoop() {
	r := NewReceiver()
	r.StartListener()

	fmt.Println("📡 Now listening for ByteWave's log outputs...")

	select {} // block forever
}

// StopListener gracefully shuts down the log listener.
func (r *Receiver) StopListener() {
	r.cancel()
}
