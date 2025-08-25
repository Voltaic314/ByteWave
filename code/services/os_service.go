// OSService handles file/folder operations on the OS.
package services

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Voltaic314/ByteWave/code/filesystem"
	"github.com/Voltaic314/ByteWave/code/logging"
)

var _ BaseServiceInterface = (*OSService)(nil)

// OSService handles file/folder operations on the OS.
type OSService struct {
	TotalDiskReads  int
	TotalDiskWrites int
	PaginationSize  int
	ctx             context.Context
	cancel          context.CancelFunc
	rootPath        string
}

// NewOSService initializes the OS service with a default pagination size.
func NewOSService() *OSService {
	ctx, cancel := context.WithCancel(context.Background())
	return &OSService{
		PaginationSize: 100,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (osSvc *OSService) SetRootPath(path string) {
	osSvc.rootPath = osSvc.NormalizePath(path)
}

func (osSvc *OSService) GetRootPath() string {
	return osSvc.rootPath
}

// IsDirectory returns a channel that asynchronously sends whether the given path is a directory.
func (osSvc *OSService) IsDirectory(path string) <-chan bool {
	result := make(chan bool, 1)
	go func() {
		defer close(result)
		osSvc.TotalDiskReads++
		info, err := os.Stat(path)
		if err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to check directory status", map[string]any{
				"path": path,
				"err":  err.Error(),
			})
			result <- false
			return
		}
		result <- info.IsDir()
	}()
	return result
}

func (osSvc *OSService) ConvertFileInfoToMap(info os.FileInfo, path string) map[string]any {
	return map[string]any{
		"name":          info.Name(),
		"path":          path,
		"size":          info.Size(),
		"is_dir":        info.IsDir(),
		"last_modified": info.ModTime().Format(time.RFC3339),
		"identifier":    path,
	}
}

// GetAllItems returns channels that asynchronously emit folders, files, and errors found at a path.
func (osSvc *OSService) GetAllItems(folder filesystem.Folder, _ <-chan int) (<-chan []filesystem.Folder, <-chan []filesystem.File, <-chan error) {
	foldersChan := make(chan []filesystem.Folder, 1)
	filesChan := make(chan []filesystem.File, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(foldersChan)
		defer close(filesChan)
		defer close(errChan)

		osSvc.TotalDiskReads++
		normalizedPath := osSvc.NormalizePath(folder.Identifier)
		entries, err := os.ReadDir(normalizedPath)
		if err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to read directory", map[string]any{
				"folderPath": folder.Path,
				"err":        err.Error(),
			})
			errChan <- err
			return
		}

		var foldersList []filesystem.Folder
		var filesList []filesystem.File

		for _, item := range entries {
			itemPath := filepath.Join(normalizedPath, item.Name())
			info, statErr := os.Stat(itemPath)
			if statErr != nil {
				logging.GlobalLogger.LogMessage("error", "Failed to stat file/folder", map[string]any{
					"itemPath": itemPath,
					"err":      statErr.Error(),
				})
				continue
			}

			metadata := osSvc.ConvertFileInfoToMap(info, itemPath)

			// Normalize identifier: all slashes to OS-native
			identifier := filepath.Clean(itemPath)

			// Fix path construction to handle root case properly
			var relPath string
			if folder.Path == "/" {
				// Root case: path should be "/item_name" (not "//item_name")
				relPath = folder.Path + item.Name() // end result is "/{item_name}"
			} else {
				// Non-root case: path should be "/parent_path/item_name"
				parentPath := strings.TrimLeft(folder.Path, "/")
				relPath = fmt.Sprintf("/%s/%s", parentPath, item.Name())
			}

			logging.GlobalLogger.LogMessage("info", "Item Found", map[string]any{
				"name":          item.Name(),
				"path":          relPath,
				"identifier":    identifier,
				"parent_id":     folder.Identifier,
				"last_modified": metadata["last_modified"].(string),
			})

			if info.IsDir() {
				foldersList = append(foldersList, filesystem.Folder{
					Name:         item.Name(),
					Path:         relPath,           // Relative path with forward slashes
					Identifier:   identifier,        // OS-native slashes
					ParentID:     folder.Identifier, // Use input folder.Identifier
					LastModified: metadata["last_modified"].(string),
				})
			} else {
				filesList = append(filesList, filesystem.File{
					Name:         item.Name(),
					Path:         relPath,           // Relative path with forward slashes
					Identifier:   identifier,        // OS-native slashes
					ParentID:     folder.Identifier, // Use input folder.Identifier
					Size:         metadata["size"].(int64),
					LastModified: metadata["last_modified"].(string),
				})
			}
		}

		foldersChan <- foldersList
		filesChan <- filesList
		errChan <- nil
	}()

	return foldersChan, filesChan, errChan
}

// GetFileContents opens a file asynchronously and returns its reader.
func (osSvc *OSService) GetFileContents(filePath string) (<-chan io.ReadCloser, <-chan error) {
	result := make(chan io.ReadCloser, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(result)
		defer close(errChan)

		osSvc.TotalDiskReads++
		file, err := os.Open(filePath)
		if err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to open file", map[string]any{
				"filePath": filePath,
				"err":      err.Error(),
			})
			errChan <- err
			return
		}
		result <- file
	}()

	return result, errChan
}

// CreateFolder creates the folder asynchronously if it doesn't exist.
func (osSvc *OSService) CreateFolder(folderPath string) <-chan error {
	result := make(chan error, 1)

	go func() {
		defer close(result)
		normalizedPath := osSvc.NormalizePath(folderPath)

		if _, err := os.Stat(normalizedPath); os.IsNotExist(err) {
			if mkErr := os.MkdirAll(normalizedPath, os.ModePerm); mkErr != nil {
				logging.GlobalLogger.LogMessage("error", "Failed to create folder", map[string]any{
					"folderPath": folderPath,
					"err":        mkErr.Error(),
				})
				result <- mkErr
				return
			}
			osSvc.TotalDiskWrites++
		}
		result <- nil
	}()

	return result
}

// UploadFile writes a file asynchronously, honoring overwrite policy.
func (osSvc *OSService) UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) <-chan error {
	result := make(chan error, 1)

	go func() {
		defer close(result)

		if _, err := os.Stat(filePath); err == nil {
			overwrite, policyErr := shouldOverWrite()
			if policyErr != nil {
				logging.GlobalLogger.LogMessage("error", "Failed to determine overwrite policy", map[string]any{
					"filePath": filePath,
					"err":      policyErr.Error(),
				})
				result <- policyErr
				return
			}
			osSvc.TotalDiskReads++
			if !overwrite {
				logging.GlobalLogger.LogMessage("info", "File already exists; overwrite disabled", map[string]any{
					"filePath": filePath,
				})
				result <- nil
				return
			}
		}

		file, err := os.Create(filePath)
		if err != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to create file", map[string]any{
				"filePath": filePath,
				"err":      err.Error(),
			})
			result <- err
			return
		}
		defer file.Close()

		if _, copyErr := io.Copy(file, reader); copyErr != nil {
			logging.GlobalLogger.LogMessage("error", "Failed to write file contents", map[string]any{
				"filePath": filePath,
				"err":      copyErr.Error(),
			})
			result <- copyErr
			return
		}

		osSvc.TotalDiskWrites++
		result <- nil
	}()

	return result
}

// NormalizePath ensures consistent formatting for a path.
func (osSvc *OSService) NormalizePath(path string) string {
	return filepath.ToSlash(filepath.Clean(path))
}

// GetFileIdentifier returns a unique identifier for a file (path for local).
func (osSvc *OSService) GetFileIdentifier(path string) string {
	return filepath.Clean(strings.ReplaceAll(path, "/", "\\"))
}
