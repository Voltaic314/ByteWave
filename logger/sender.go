package logger

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

// Logger struct handles UDP streaming and batch file logging.
type Logger struct {
	logLevel   string
	logFile    string
	sessionID  string // Unique session identifier
	logQueue   chan string // Buffered channel for batched log writes
	udpAddr    *net.UDPAddr
	udpConn    *net.UDPConn
	configPath string
	batch_sleep_time int // Sleep time between batch writes
	mu         sync.Mutex // Ensures thread safety when loading config
}

// NewLogger initializes the Logger instance.
func NewLogger(configPath string) *Logger {
	l := &Logger{
		logQueue:   make(chan string, 100), // Buffered channel for batching logs
		configPath: configPath,
		sessionID:  generateSessionID(), // Generate unique session ID
	}
	l.loadSettings()
	l.connectToUDP()
	l.storeSessionID() // Store session ID in settings.json
	go l.batchLogWriter() // Start background log writer
	return l
}

// generateSessionID creates a unique identifier for the current session.
func generateSessionID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%X", rand.Uint64()) // Generate a hex-based random session ID
}

// storeSessionID writes the session ID to settings.json
func (l *Logger) storeSessionID() {
	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.ReadFile(l.configPath)
	if err != nil {
		fmt.Println("⚠️  Failed to load settings.json while storing session ID.")
		return
	}

	var config map[string]interface{}
	json.Unmarshal(file, &config)
	config["session_id"] = l.sessionID // Update session ID field

	updatedConfig, _ := json.MarshalIndent(config, "", "  ")
	os.WriteFile(l.configPath, updatedConfig, 0644)
}

// loadSettings reads log level and address from settings.json
func (l *Logger) loadSettings() {
	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.ReadFile(l.configPath)
	if err != nil {
		fmt.Println("⚠️  Failed to load settings.json. Defaulting to 'warning' level.")
		l.logLevel = "warning"
		l.logFile = generateLogFilename()
		return
	}

	var config map[string]interface{}
	json.Unmarshal(file, &config)

	if val, ok := config["log_level"].(string); ok {
		l.logLevel = val
	} else {
		l.logLevel = "warning"
	}

	if val, ok := config["log_print_address"].(string); ok {
		l.udpAddr, _ = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%v", val, config["log_print_port"]))
	} else {
		l.udpAddr, _ = net.ResolveUDPAddr("udp", "127.0.0.1:9999")
	}
	
	if val, ok := config["log_batch_sleep_time"].(float64); ok {
		l.batch_sleep_time = int(val) // Convert float64 to int
	} else {
		l.batch_sleep_time = 300
	}

	l.logFile = generateLogFilename()
}

// generateLogFilename creates a unique filename for each session
func generateLogFilename() string {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	return fmt.Sprintf("DMT Session Logs %s.log", timestamp)
}

// connectToUDP initializes UDP connection for log streaming
func (l *Logger) connectToUDP() {
	if l.udpAddr == nil {
		fmt.Println("⚠️  Invalid log listener address. Check settings.json.")
		return
	}

	conn, err := net.DialUDP("udp", nil, l.udpAddr)
	if err != nil {
		fmt.Println("⚠️  Could not connect to log listener.")
		return
	}

	l.udpConn = conn
}

// LogMessage sends logs to UDP and queues them for batch file writing
func (l *Logger) LogMessage(level, msg string) {
	if !l.shouldLog(level) {
		return // Skip log if below threshold
	}

	logEntry := fmt.Sprintf("[%s] [%s] %s: %s", time.Now().Format(time.RFC3339), l.sessionID, level, msg)

	// Send to UDP log listener (fire-and-forget)
	if l.udpConn != nil {
		l.udpConn.Write([]byte(logEntry))
	}

	// Queue log for batch file writing
	l.logQueue <- logEntry
}

// batchLogWriter flushes logs to file in batches
func (l *Logger) batchLogWriter() {
    for {
        sleepDuration := time.Duration(l.batch_sleep_time) * time.Second
        time.Sleep(sleepDuration) // Adjust batch interval dynamically
        l.flushLogs()
    }
}


// flushLogs writes queued logs to the file
func (l *Logger) flushLogs() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.logQueue) == 0 {
		return // No logs to flush
	}

	file, err := os.OpenFile(l.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("⚠️  Could not open log file.")
		return
	}
	defer file.Close()

	for len(l.logQueue) > 0 {
		logEntry := <-l.logQueue
		file.WriteString(logEntry + "\n")
	}
}

// shouldLog determines if a message should be logged based on level
func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{"error": 0, "warn": 1, "info": 2, "debug": 3, "trace": 4}
	return levels[level] <= levels[l.logLevel]
}

// UpdateLogLevel updates the logger's log level dynamically (called by CLI)
func (l *Logger) UpdateLogLevel(newLevel string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logLevel = newLevel
}
