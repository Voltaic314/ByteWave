package cli

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync"
)

// Receiver struct listens for log messages via UDP and prints them to the terminal.
type Receiver struct {
	listenAddr   string
	listenPort   int
	sessionID    string
	configPath   string
	mu           sync.Mutex
}

// NewReceiver initializes a new log receiver instance.
func NewReceiver(configPath string) *Receiver {
	r := &Receiver{
		configPath: configPath,
	}
	r.loadSettings()
	return r
}

// loadSettings reads session ID and log listening settings from settings.json
func (r *Receiver) loadSettings() {
	r.mu.Lock()
	defer r.mu.Unlock()

	file, err := os.ReadFile(r.configPath)
	if err != nil {
		fmt.Println("⚠️  Failed to load settings.json for receiver.")
		r.listenAddr = "127.0.0.1"
		r.listenPort = 9999
		return
	}

	var config map[string]interface{}
	json.Unmarshal(file, &config)

	if val, ok := config["log_print_address"].(string); ok {
		r.listenAddr = val
	} else {
		r.listenAddr = "127.0.0.1"
	}

	if val, ok := config["log_print_port"].(float64); ok { // JSON treats numbers as float64
		r.listenPort = int(val)
	} else {
		r.listenPort = 9999
	}

	if val, ok := config["session_id"].(string); ok {
		r.sessionID = val
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

	buffer := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("❌ Error reading from UDP:", err)
			continue
		}

		message := string(buffer[:n])
		if r.isValidSessionLog(message) {
			fmt.Println(message) // Print to terminal
		}
	}
}

// isValidSessionLog checks if a log message belongs to the current session.
func (r *Receiver) isValidSessionLog(logMessage string) bool {
	return r.sessionID == "" || (r.sessionID != "" && contains(logMessage, r.sessionID))
}

// contains checks if a string contains a substring.
func contains(str, substr string) bool {
	return len(substr) == 0 || (len(str) >= len(substr) && str[:len(substr)] == substr)
}

// SpawnListenerTerminal starts a new terminal window to run the log listener.
func (r *Receiver) SpawnListenerTerminal() {
	file, err := os.ReadFile(r.configPath)
	if err != nil {
		fmt.Println("⚠️  Failed to load settings.json while checking log terminal spawn.")
		return
	}

	var config map[string]interface{}
	json.Unmarshal(file, &config)

	if val, ok := config["spawn_log_terminal"].(bool); ok && val {
		fmt.Println("🖥️ Spawning log listener terminal...")

		// OS-specific terminal launch
		switch current_os := detectOS(); current_os {
		case "windows":
			exec.Command("cmd", "/C", "start", "go", "run", "receiver.go").Start()
		case "linux":
			exec.Command("x-terminal-emulator", "-e", "go run receiver.go").Start()
		case "darwin": // macOS
			exec.Command("open", "-a", "Terminal", "go run receiver.go").Start()
		default:
			fmt.Println("⚠️  Unsupported OS for auto-spawning log terminal.")
		}
	}
}

// detectOS returns the current operating system
func detectOS() string {
	return runtime.GOOS
}
