package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Voltaic314/ByteWave/code/db/writequeue"
	"github.com/Voltaic314/ByteWave/code/db"
	"github.com/Voltaic314/ByteWave/code/logging/types"
)

type Logger struct {
	logLevel   string
	udpConn    *net.UDPConn
	logWQ      *writequeue.WriteQueue
	batchSize  int
	batchDelay time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
}

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
	l.logWQ = writequeue.NewQueue(
		l.batchSize,
		l.batchDelay,
		func(tableQueries map[string][]string, tableParams map[string][][]any) error {
			return dbInstance.WriteBatch(tableQueries, tableParams)
		},
	)
	fmt.Println("✅ Logger connected to DB and write queue activated.")
}

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
	}
	if val, ok := config["log_batch_size"].(float64); ok {
		l.batchSize = int(val)
	}
	if val, ok := config["log_batch_sleep_time"].(float64); ok {
		l.batchDelay = time.Duration(int(val)) * time.Second
	}
}

func (l *Logger) connectToUDP() {
	udpAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:9999")
	conn, _ := net.DialUDP("udp", nil, udpAddr)
	l.udpConn = conn
}

func (l *Logger) LogMessage(level, message string, details map[string]any) {
	if !l.shouldLog(level) {
		return
	}
	entry := types.LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Details:   details,
	}

	// Send to UI listener (UDP)
	if l.udpConn != nil {
		go func() {
			payload, _ := json.Marshal(entry)
			l.udpConn.Write(payload)
		}()
	}

	// Queue to DB
	if l.logWQ != nil {
		detailsJSON, _ := json.Marshal(details)
		params := []any{entry.Timestamp, level, string(detailsJSON), message}
		query := `INSERT OR IGNORE INTO audit_log (timestamp, level, details, message) VALUES (?, ?, ?, ?)`
		l.enqueueLog(query, params)
	}
}

func (l *Logger) enqueueLog(query string, params []any) {
	if l.logWQ != nil {
		l.logWQ.AddWriteOperation("audit_log", "", query, params)
	}
}

func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{"error": 0, "warning": 1, "info": 2, "debug": 3, "trace": 4}
	return levels[level] <= levels[l.logLevel]
}

func (l *Logger) Stop() {
	if l.logWQ != nil {
		l.logWQ.FlushAll()
		l.logWQ.Stop()
	}
	if l.udpConn != nil {
		l.udpConn.Close()
	}
	l.cancel()
	fmt.Println("🛑 Logger shut down.")
}
