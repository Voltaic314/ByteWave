package core

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// Logger handles log streaming and batch writes to the audit_log DB.
type Logger struct {
	logLevel       string
	logQueue       chan LogEntry
	udpAddr        *net.UDPAddr
	udpConn        *net.UDPConn
	batchSize      int
	batchSleepTime time.Duration
	mu             sync.Mutex
}

// LogEntry represents a structured log.
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

// Global Logger instance
var GlobalLogger *Logger

// InitLogger initializes the global logger.
func InitLogger(configPath string) {
	GlobalLogger = &Logger{
		logQueue: make(chan LogEntry, 100),
	}
	GlobalLogger.loadSettings(configPath)
	GlobalLogger.connectToUDP()
	go GlobalLogger.batchLogWriter()
}

// loadSettings loads logger settings from JSON.
func (l *Logger) loadSettings(configPath string) {
	file, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Println("⚠️  Failed to load logger.json, using defaults.")
		l.logLevel = "warning"
		l.batchSize = 50
		l.batchSleepTime = 5 * time.Second
		return
	}

	var config map[string]interface{}
	json.Unmarshal(file, &config)

	if val, ok := config["log_level"].(string); ok {
		l.logLevel = val
	} else {
		l.logLevel = "warning"
	}

	if val, ok := config["log_batch_size"].(float64); ok {
		l.batchSize = int(val)
	} else {
		l.batchSize = 50
	}

	if val, ok := config["log_batch_sleep_time"].(float64); ok {
		l.batchSleepTime = time.Duration(int(val)) * time.Second
	} else {
		l.batchSleepTime = 5 * time.Second
	}

	fmt.Println("✅ Logger settings loaded:", l.logLevel, l.batchSize, l.batchSleepTime)
}

// connectToUDP initializes UDP connection for log streaming.
func (l *Logger) connectToUDP() {
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:9999")
	if err != nil {
		fmt.Println("⚠️  Invalid log listener address.")
		return
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		fmt.Println("⚠️  Could not connect to log listener.")
		return
	}
	l.udpConn = conn
}

// LogMessage sends logs to UDP and queues them for batch DB writing.
func (l *Logger) LogMessage(level, msg string) {
	if !l.shouldLog(level) {
		return
	}

	logEntry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339),
		Level:     level,
		Message:   msg,
	}

	if l.udpConn != nil {
		serialized, _ := json.Marshal(logEntry)
		l.udpConn.Write(serialized)
	}

	l.logQueue <- logEntry
}

// batchLogWriter periodically writes logs in batches.
func (l *Logger) batchLogWriter() {
	for {
		time.Sleep(l.batchSleepTime)
		l.flushLogs()
	}
}

// flushLogs writes queued logs to the DB.
func (l *Logger) flushLogs() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.logQueue) == 0 {
		return
	}

	// Simulate DB write (replace with actual DB logic)
	for len(l.logQueue) > 0 {
		entry := <-l.logQueue
		log.Printf("[%s] %s: %s\n", entry.Timestamp, entry.Level, entry.Message)
	}
}

// shouldLog checks if a message should be logged based on log level.
func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{"error": 0, "warn": 1, "info": 2, "debug": 3, "trace": 4}
	return levels[level] <= levels[l.logLevel]
}
