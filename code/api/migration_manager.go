package api

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MigrationStatus represents the current state of a migration
type MigrationStatus string

const (
	StatusPending  MigrationStatus = "pending"
	StatusRunning  MigrationStatus = "running"
	StatusPaused   MigrationStatus = "paused"
	StatusComplete MigrationStatus = "complete"
	StatusFailed   MigrationStatus = "failed"
	StatusStopped  MigrationStatus = "stopped"
)

// MigrationConfig contains the configuration for a migration
type MigrationConfig struct {
	SourcePath      string   `json:"sourcePath" binding:"required"`
	DestinationPath string   `json:"destinationPath" binding:"required"`
	IncludePatterns []string `json:"includePatterns,omitempty"`
	ExcludePatterns []string `json:"excludePatterns,omitempty"`
	ChunkSize       int      `json:"chunkSize,omitempty"`
}

// MigrationInfo contains information about a migration
type MigrationInfo struct {
	ID           string          `json:"id"`
	Status       MigrationStatus `json:"status"`
	Config       MigrationConfig `json:"config"`
	StartTime    time.Time       `json:"startTime,omitempty"`
	EndTime      time.Time       `json:"endTime,omitempty"`
	TotalFiles   int             `json:"totalFiles"`
	ProcessedFiles int           `json:"processedFiles"`
	ErrorCount   int             `json:"errorCount"`
}

// MigrationManager defines the interface for managing migrations
type MigrationManager interface {
	StartMigration(config MigrationConfig) (string, error)
	StopMigration(id string) error
	PauseMigration(id string) error
	ResumeMigration(id string) error
	GetMigrationStatus(id string) (*MigrationInfo, error)
	
	// New methods that don't require an ID
	StopCurrentMigration() error
	PauseCurrentMigration() error
	ResumeCurrentMigration() error
	GetCurrentMigrationStatus() (*MigrationInfo, error)
}

// DefaultMigrationManager is the default implementation of MigrationManager
type DefaultMigrationManager struct {
	migrations map[string]*MigrationInfo
	currentMigrationID string
	mu         sync.RWMutex
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager() *DefaultMigrationManager {
	return &DefaultMigrationManager{
		migrations: make(map[string]*MigrationInfo),
		currentMigrationID: "",
	}
}

// StartMigration starts a new migration
func (m *DefaultMigrationManager) StartMigration(config MigrationConfig) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Generate a unique ID for the migration
	id := uuid.New().String()
	
	// Create a new migration info
	migration := &MigrationInfo{
		ID:           id,
		Status:       StatusPending,
		Config:       config,
		StartTime:    time.Now(),
		TotalFiles:   0,
		ProcessedFiles: 0,
		ErrorCount:   0,
	}
	
	// Store the migration
	m.migrations[id] = migration
	
	// Set as current migration
	m.currentMigrationID = id
	
	// TODO: Start the actual migration process
	// This would involve calling into the core ByteWave functionality
	// For now, we'll just update the status
	migration.Status = StatusRunning
	
	return id, nil
}

// StopMigration stops a migration
func (m *DefaultMigrationManager) StopMigration(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	migration, exists := m.migrations[id]
	if !exists {
		return errors.New("migration not found")
	}
	
	// Can only stop running or paused migrations
	if migration.Status != StatusRunning && migration.Status != StatusPaused {
		return errors.New("migration is not running or paused")
	}
	
	// TODO: Stop the actual migration process
	// This would involve calling into the core ByteWave functionality
	
	migration.Status = StatusStopped
	migration.EndTime = time.Now()
	
	return nil
}

// PauseMigration pauses a migration
func (m *DefaultMigrationManager) PauseMigration(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	migration, exists := m.migrations[id]
	if !exists {
		return errors.New("migration not found")
	}
	
	// Can only pause running migrations
	if migration.Status != StatusRunning {
		return errors.New("migration is not running")
	}
	
	// TODO: Pause the actual migration process
	// This would involve calling into the core ByteWave functionality
	
	migration.Status = StatusPaused
	
	return nil
}

// ResumeMigration resumes a paused migration
func (m *DefaultMigrationManager) ResumeMigration(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	migration, exists := m.migrations[id]
	if !exists {
		return errors.New("migration not found")
	}
	
	// Can only resume paused migrations
	if migration.Status != StatusPaused {
		return errors.New("migration is not paused")
	}
	
	// TODO: Resume the actual migration process
	// This would involve calling into the core ByteWave functionality
	
	migration.Status = StatusRunning
	
	return nil
}

// GetMigrationStatus gets the status of a migration
func (m *DefaultMigrationManager) GetMigrationStatus(id string) (*MigrationInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	migration, exists := m.migrations[id]
	if !exists {
		return nil, errors.New("migration not found")
	}
	
	// Return a copy to avoid race conditions
	migrationCopy := *migration
	
	return &migrationCopy, nil
}

// StopCurrentMigration stops the current migration
func (m *DefaultMigrationManager) StopCurrentMigration() error {
	m.mu.RLock()
	currentID := m.currentMigrationID
	m.mu.RUnlock()
	
	if currentID == "" {
		return errors.New("no active migration found")
	}
	
	return m.StopMigration(currentID)
}

// PauseCurrentMigration pauses the current migration
func (m *DefaultMigrationManager) PauseCurrentMigration() error {
	m.mu.RLock()
	currentID := m.currentMigrationID
	m.mu.RUnlock()
	
	if currentID == "" {
		return errors.New("no active migration found")
	}
	
	return m.PauseMigration(currentID)
}

// ResumeCurrentMigration resumes the current migration
func (m *DefaultMigrationManager) ResumeCurrentMigration() error {
	m.mu.RLock()
	currentID := m.currentMigrationID
	m.mu.RUnlock()
	
	if currentID == "" {
		return errors.New("no active migration found")
	}
	
	return m.ResumeMigration(currentID)
}

// GetCurrentMigrationStatus gets the status of the current migration
func (m *DefaultMigrationManager) GetCurrentMigrationStatus() (*MigrationInfo, error) {
	m.mu.RLock()
	currentID := m.currentMigrationID
	m.mu.RUnlock()
	
	if currentID == "" {
		return nil, errors.New("no active migration found")
	}
	
	return m.GetMigrationStatus(currentID)
}