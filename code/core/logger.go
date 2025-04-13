package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Voltaic314/ByteWave/code/core/db"
	"github.com/Voltaic314/ByteWave/code/core/db/writequeue"
)

// Logger handles log streaming and batch writes to the audit_log DB.
type Logger struct {
	logLevel    string
	udpConn     *net.UDPConn
	logWQ       *writequeue.WriteQueue
	dbInstance  *db.DB
	ctx         context.Context
	cancel      context.CancelFunc
	batchSize   int
	batchDelay  time.Duration
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

func InitLogger(configPath string) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := &Logger{
		ctx:    ctx,
		cancel: cancel,
	}

	logger.loadSettings(configPath)
	logger.connectToUDP()

	GlobalLogger = logger
	fmt.Println("✅ Logger initialized in UDP-only mode.")
}

func (l *Logger) RegisterDB(dbInstance *db.DB) {
	l.dbInstance = dbInstance
	l.logWQ = writequeue.NewQueue(
		l.batchSize,
		l.batchDelay,
		func(tableQueries map[string][]string, tableParams map[string][][]any) error {
			return dbInstance.WriteBatch(tableQueries, tableParams)
		},
	)
	fmt.Println("✅ Logger connected to DB and write queue activated.")
}


// loadSettings loads logger settings from JSON config.
func (l *Logger) loadSettings(configPath string) {
	file, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Println("⚠️  Failed to load logger.json, using defaults.")
		l.logLevel = "warning"
		l.batchSize = 50
		l.batchDelay = 5 * time.Second
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
		l.batchDelay = time.Duration(int(val)) * time.Second
	} else {
		l.batchDelay = 5 * time.Second
	}

	fmt.Println("✅ Logger settings loaded:", l.logLevel, l.batchSize, l.batchDelay)
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

// LogMessage sends logs to UDP and queues for DB via write queue.
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

	// Send to terminal
	if l.udpConn != nil {
		go func(entry LogEntry) {
			serialized, _ := json.Marshal(entry)
			l.udpConn.Write(serialized)
		}(logEntry)
	}

	// Prepare DB write
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		detailsJSON = []byte("{}")
	}
	detailsStr := string(detailsJSON)

	query := `INSERT INTO audit_log (category, error_type, details, message) VALUES (?, ?, ?, ?)`
	params := []any{level, nil, &detailsStr, message}

	if l.logWQ != nil {
		l.logWQ.AddWriteOperation("audit_log", query, params)
	}

	l.logWQ.AddWriteOperation("audit_log", query, params)
}

// shouldLog checks if a message should be logged based on log level.
func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{"error": 0, "warn": 1, "info": 2, "debug": 3, "trace": 4}
	return levels[level] <= levels[l.logLevel]
}

// Stop flushes and closes everything cleanly.
func (l *Logger) Stop() {
	l.logWQ.FlushAll()
	l.logWQ.Stop()
	l.cancel()
	if l.udpConn != nil {
		l.udpConn.Close()
	}
	fmt.Println("🛑 Logger shut down.")
}
