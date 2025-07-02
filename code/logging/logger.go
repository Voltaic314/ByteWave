package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/Voltaic314/ByteWave/code/types/db"
	"github.com/Voltaic314/ByteWave/code/types/logging"
)

type Logger struct {
	logLevel   string
	udpConn    *net.UDPConn
	logWQ      db.WriteQueueInterface
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

func (l *Logger) RegisterDB(dbInstance db.DBInterface) {
	l.logWQ = dbInstance.GetWriteQueue("audit_log")
	if l.logWQ == nil {
		dbInstance.InitWriteQueue("audit_log", db.LogWriteQueue, l.batchSize, l.batchDelay)
		l.logWQ = dbInstance.GetWriteQueue("audit_log")
	}
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
	l.logWithEntity(level, "", "", "", message, details)
}

// LogMessageWithEntity logs a message with entity information
func (l *Logger) LogMessageWithEntity(level, entity, entityID, path, message string, details map[string]any) {
	l.logWithEntity(level, entity, entityID, path, message, details)
}

// LogWorker logs a message from a worker
func (l *Logger) LogWorker(level, workerID, path, message string, details map[string]any) {
	l.logWithEntity(level, "worker", workerID, path, message, details)
}

// LogQP logs a message from QueuePublisher
func (l *Logger) LogQP(level, queueName, path, message string, details map[string]any) {
	l.logWithEntity(level, "QP", queueName, path, message, details)
}

// LogSystem logs a system-level message
func (l *Logger) LogSystem(level, component, message string, details map[string]any) {
	l.logWithEntity(level, "system", component, "", message, details)
}

// LogAPI logs a message from API interactions
func (l *Logger) LogAPI(level, serviceID, path, message string, details map[string]any) {
	l.logWithEntity(level, "API", serviceID, path, message, details)
}

// logWithEntity is the internal method that handles the actual logging
func (l *Logger) logWithEntity(level, entity, entityID, path, message string, details map[string]any) {
	if !l.shouldLog(level) {
		return
	}
	entry := logging.LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Entity:    entity,
		EntityID:  entityID,
		Path:      path,
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
		params := []any{uuid.New().String(), entry.Timestamp, level, entry.Entity, entry.EntityID, entry.Path, string(detailsJSON), message}
		query := `INSERT INTO audit_log (id, timestamp, level, entity, entity_id, path, details, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
		l.enqueueLog(query, params)
	}
}

func (l *Logger) enqueueLog(query string, params []any) {
	if l.logWQ != nil {
		l.logWQ.Add("", db.WriteOp{
			Path:   "",
			Query:  query,
			Params: params,
			OpType: "insert",
		})
	}
}

func (l *Logger) shouldLog(level string) bool {
	levels := map[string]int{"error": 0, "warning": 1, "info": 2, "debug": 3, "trace": 4}
	return levels[level] <= levels[l.logLevel]
}

func (l *Logger) Stop() {
	if l.logWQ != nil {
		l.logWQ.Flush(true)
	}
	if l.udpConn != nil {
		l.udpConn.Close()
	}
	l.cancel()
	fmt.Println("🛑 Logger shut down.")
}
