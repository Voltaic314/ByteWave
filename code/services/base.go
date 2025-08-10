package services

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
)

// MigrationRules defines configurable migration settings.
type MigrationRules struct {
	Mode            string `json:"mode"`             // "copy" or "move"
	OverwritePolicy string `json:"overwrite_policy"` // "newest" or "force"
	CleanSlate      bool   `json:"clean_slate"`
}

type BaseServiceInterface interface {
	IsDirectory(path string) <-chan bool
	GetAllItems(folder filesystem.Folder, paginationStream <-chan int) (<-chan []filesystem.Folder, <-chan []filesystem.File, <-chan error)
	GetFileContents(filePath string) (<-chan io.ReadCloser, <-chan error)
	CreateFolder(folderPath string) <-chan error
	UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) <-chan error
	NormalizePath(path string) string
	GetFileIdentifier(path string) string
	SetRootPath(path string)
	GetRootPath() string
	Relativize(path string) string
}

// BaseService holds shared attributes/methods and migration rules.
type BaseService struct {
	TotalDiskReads  int
	TotalDiskWrites int
	PaginationSize  int

	Rules     MigrationRules
	RulesPath string
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	rootPath  string
}

// NewBaseService initializes a BaseService, loading migration rules from JSON.
func NewBaseService(rulesPath string) (*BaseService, error) {
	ctx, cancel := context.WithCancel(context.Background())
	base := &BaseService{
		RulesPath: rulesPath,
		ctx:       ctx,
		cancel:    cancel,
		rootPath:  "",
	}

	err := base.LoadMigrationRules()
	if err != nil {
		logging.GlobalLogger.LogMessage("error", "Failed to load migration rules", map[string]any{
			"file": rulesPath,
			"err":  err.Error(),
		})
		return nil, err
	}

	return base, nil
}

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

func (b *BaseService) SetMode(mode string) error {
	if mode != "copy" && mode != "move" {
		return errors.New("invalid mode: must be 'copy' or 'move'")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.Rules.Mode = mode
	return b.SaveMigrationRules()
}

// ---- Stub methods (all async stubs to comply with BaseServiceInterface) ----

func (b *BaseService) IsDirectory(path string) <-chan bool {
	result := make(chan bool, 1)
	go func() {
		defer close(result)
		result <- false
	}()
	return result
}

func (b *BaseService) GetAllItems(folder filesystem.Folder, paginationStream <-chan int) (<-chan []filesystem.Folder, <-chan []filesystem.File, <-chan error) {
	foldersChan := make(chan []filesystem.Folder, 1)
	filesChan := make(chan []filesystem.File, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(foldersChan)
		defer close(filesChan)
		defer close(errChan)
		errChan <- errors.New("GetAllItems must be implemented in a specific service")
	}()

	return foldersChan, filesChan, errChan
}

func (b *BaseService) GetFileContents(filePath string) (<-chan io.ReadCloser, <-chan error) {
	result := make(chan io.ReadCloser, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(result)
		defer close(errChan)
		errChan <- errors.New("GetFileContents not implemented")
	}()

	return result, errChan
}

func (b *BaseService) CreateFolder(folderPath string) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		result <- errors.New("CreateFolder not implemented")
	}()
	return result
}

func (b *BaseService) UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		result <- errors.New("UploadFile not implemented")
	}()
	return result
}

func (b *BaseService) NormalizePath(path string) string {
	return path
}

func (b *BaseService) GetFileIdentifier(path string) string {
	return ""
}

func (b *BaseService) SetRootPath(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rootPath = path
}

func (b *BaseService) GetRootPath() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rootPath
}

func (b *BaseService) Relativize(path string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	normalized := b.NormalizePath(path)
	root := b.rootPath
	if strings.HasPrefix(normalized, root) {
		rel := strings.TrimPrefix(normalized, root)
		return strings.TrimPrefix(rel, "/")
	}
	return normalized // fallback
}
