package services

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
)

// MigrationRules defines configurable migration settings.
type MigrationRules struct {
	Mode            string `json:"mode"`             // "copy" or "move"
	OverwritePolicy string `json:"overwrite_policy"` // "newest" or "force"
	CleanSlate      bool   `json:"clean_slate"`
}

// BaseService holds shared attributes/methods and migration rules.
type BaseService struct {
	TotalDiskReads  int
	TotalDiskWrites int
	PaginationSize  int

	Rules     MigrationRules // Holds migration rules in memory
	RulesPath string         // Path to migration_rules.json
	mu        sync.Mutex     // Prevents race conditions when writing rules
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewBaseService initializes a BaseService, loading migration rules from JSON.
func NewBaseService(rulesPath string) (*BaseService, error) {
	ctx, cancel := context.WithCancel(context.Background())
	base := &BaseService{
		RulesPath: rulesPath,
		ctx:       ctx,
		cancel:    cancel,
	}

	err := base.LoadMigrationRules()
	if err != nil {
		core.GlobalLogger.LogMessage("error", "Failed to load migration rules", map[string]any{
			"file": rulesPath,
			"err":  err.Error(),
		})
		return nil, err
	}

	return base, nil
}

// LoadMigrationRules loads migration settings asynchronously.
func (b *BaseService) LoadMigrationRules() error {
	done := make(chan error, 1)

	go func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		file, err := os.Open(b.RulesPath)
		if err != nil {
			done <- err
			return
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			done <- err
			return
		}

		if err := json.Unmarshal(data, &b.Rules); err != nil {
			done <- err
			return
		}

		// Set defaults if fields are missing
		if b.Rules.Mode == "" {
			b.Rules.Mode = "copy"
		}
		if b.Rules.OverwritePolicy == "" {
			b.Rules.OverwritePolicy = "newest"
		}

		done <- nil
	}()

	select {
	case <-b.ctx.Done():
		return errors.New("load migration rules canceled")
	case err := <-done:
		return err
	}
}

// SaveMigrationRules writes the in-memory rules back to the JSON file asynchronously.
func (b *BaseService) SaveMigrationRules() error {
	done := make(chan error, 1)

	go func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		data, err := json.MarshalIndent(b.Rules, "", "  ")
		if err != nil {
			done <- err
			return
		}

		err = os.WriteFile(b.RulesPath, data, 0644)
		done <- err
	}()

	select {
	case <-b.ctx.Done():
		return errors.New("save migration rules canceled")
	case err := <-done:
		return err
	}
}

// Example of modifying a rule dynamically and persisting it.
func (b *BaseService) SetMode(mode string) error {
	if mode != "copy" && mode != "move" {
		return errors.New("invalid mode: must be 'copy' or 'move'")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.Rules.Mode = mode
	return b.SaveMigrationRules()
}

// Stub methods (must be overridden in actual services).
func (b *BaseService) IsDirectory(path string) (bool, error) {
	return false, errors.New("IsDirectory not implemented in BaseService")
}

// GetAllItems is a stub function that must be overridden in actual services.
func (b *BaseService) GetAllItems(folder filesystem.Folder, paginationStream <-chan int) ([]filesystem.Folder, []filesystem.File, error) {
	return nil, nil, errors.New("GetAllItems must be implemented in a specific service (e.g., OS, Dropbox, etc.)")
}

func (b *BaseService) GetFileContents(filePath string) (io.ReadCloser, error) {
	return nil, errors.New("GetFileContents not implemented in BaseService")
}

func (b *BaseService) CreateFolder(folderPath string) error {
	return errors.New("CreateFolder not implemented in BaseService")
}

func (b *BaseService) UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) error {
	return errors.New("UploadFile not implemented in BaseService")
}

// GetFileIdentifier should be overridden in actual services.
// It takes a generic metadata map instead of a strict FileInfo struct.
func (b *BaseService) GetFileIdentifier(path string) string {
	return ""
}

func (b *BaseService) NormalizePath(path string) string {
	return path
}
