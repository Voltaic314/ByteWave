package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Voltaic314/ByteWave/code/core/db"
)

// Logger handles log streaming and batch writes to the audit_log DB.
type Logger struct {
	logLevel       string
	logQueue       chan LogEntry
	udpConn        *net.UDPConn
	batchSize      int
	batchSleepTime time.Duration
	dbInstance     *db.DB
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
}

// LogEntry represents a structured log.
type LogEntry struct {
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"` // Optional details field
}

// Global Logger instance
var GlobalLogger *Logger

// InitLogger initializes the global logger with a DB connection.
func InitLogger(configPath string, dbInstance *db.DB) {
	ctx, cancel := context.WithCancel(context.Background())
	GlobalLogger = &Logger{
		logQueue:   make(chan LogEntry, 100),
		dbInstance: dbInstance,
		ctx:        ctx,
		cancel:     cancel,
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

	var config map[string]any
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

// LogMessage sends logs asynchronously to UDP and queues for batch DB writing.
func (l *Logger) LogMessage(level, message string, details map[string]any) {
	if !l.shouldLog(level) {
		return
	}

	logEntry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339),
		Level:     level,
		Message:   message,
		Details:   details,
	}

	// Send log to UDP listener asynchronously
	if l.udpConn != nil {
		go func(entry LogEntry) {
			serialized, _ := json.Marshal(entry)
			l.udpConn.Write(serialized)
		}(logEntry)
	}

	// Queue log for batch DB writing asynchronously
	select {
	case l.logQueue <- logEntry:
	default:
		fmt.Println("⚠️  Log queue is full, dropping log entry.")
	}
}

// batchLogWriter handles logs asynchronously in batches with graceful shutdown.
func (l *Logger) batchLogWriter() {
	ticker := time.NewTicker(l.batchSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			fmt.Println("🛑 Logger shutting down.")
			return
		case <-ticker.C:
			l.flushLogs()
		case entry := <-l.logQueue:
			l.WriteLogToDB(entry)
		}
	}
}

// flushLogs writes queued logs to the DB.
func (l *Logger) flushLogs() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for len(l.logQueue) > 0 {
		entry := <-l.logQueue
		l.WriteLogToDB(entry)
	}
}

// WriteLogToDB writes logs to the DB.
func (l *Logger) WriteLogToDB(entry LogEntry) {
	if l.dbInstance == nil {
		fmt.Println("❌ Logger Error: No DB instance available")
		return
	}

	// Convert details to JSON string
	detailsJSON, err := json.Marshal(entry.Details)
	if err != nil {
		detailsJSON = []byte("{}") // Default to empty object if error
	}
	detailsStr := string(detailsJSON)

	// Insert into DB via queue
	l.dbInstance.WriteLog(db.LogEntry{
		Category:  entry.Level,
		ErrorType: nil,
		Details:   &detailsStr,
		Message:   entry.Message,
	})
}

// shouldLog checks if a message should be logged based on log level.
func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{"error": 0, "warn": 1, "info": 2, "debug": 3, "trace": 4}
	return levels[level] <= levels[l.logLevel]
}
