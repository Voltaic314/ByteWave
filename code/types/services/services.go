// Package services provides types for service interfaces and configurations.
package services

import (
	"context"
	"io"
	"sync"

	"github.com/Voltaic314/ByteWave/code/types/filesystem"
)

// MigrationRules defines configurable migration settings
type MigrationRules struct {
	Mode            string `json:"mode"`             // "copy" or "move"
	OverwritePolicy string `json:"overwrite_policy"` // "newest" or "force"
	CleanSlate      bool   `json:"clean_slate"`
}

// BaseServiceInterface defines methods for service operations
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
}

// BaseService holds shared attributes/methods and migration rules
type BaseService struct {
	TotalDiskReads  int
	TotalDiskWrites int
	PaginationSize  int
	Rules           MigrationRules
	RulesPath       string
	Mu              sync.Mutex
	Ctx             context.Context
	Cancel          context.CancelFunc
	RootPath        string
}
