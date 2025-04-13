// OSService handles file/folder operations on the OS.
package services

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/filesystem"
)

// OSService handles file/folder operations on the OS.
type OSService struct {
	Logger          *core.Logger
	TotalDiskReads  int
	TotalDiskWrites int
	PaginationSize  int
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewOSService initializes the OS service with a default pagination size,
// and injects the logger dependency.
func NewOSService(logger *core.Logger) *OSService {
	ctx, cancel := context.WithCancel(context.Background())
	return &OSService{
		Logger:         logger,
		PaginationSize: 100,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// IsDirectory checks asynchronously if a given path is a directory.
func (osSvc *OSService) IsDirectory(path string) <-chan bool {
	result := make(chan bool, 1)

	go func() {
		defer close(result)
		osSvc.TotalDiskReads++
		info, err := os.Stat(path)
		if err != nil {
			osSvc.Logger.LogMessage("error", "Failed to check directory status", map[string]any{
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

// ConvertFileInfoToMap converts os.FileInfo into a generic map[string]any.
func (osSvc *OSService) ConvertFileInfoToMap(info os.FileInfo, path string) map[string]any {
	metadata := map[string]any{
		"name":          info.Name(),
		"path":          path,
		"size":          info.Size(),
		"is_dir":        info.IsDir(),
		"last_modified": info.ModTime().Format(time.RFC3339),
	}

	// Use path as identifier for local/Windows services, but set to nil to avoid DB duplication
	metadata["identifier"] = nil
	return metadata
}

// GetAllItems retrieves all items in a folder asynchronously with dynamic pagination.
func (osSvc *OSService) GetAllItems(folderPath string) (<-chan []filesystem.Folder, <-chan []filesystem.File, <-chan error) {
	foldersChan := make(chan []filesystem.Folder, 1)
	filesChan := make(chan []filesystem.File, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(foldersChan)
		defer close(filesChan)
		defer close(errChan)

		osSvc.TotalDiskReads++
		normalizedPath := osSvc.NormalizePath(folderPath)
		entries, err := os.ReadDir(normalizedPath)
		if err != nil {
			osSvc.Logger.LogMessage("error", "Failed to read directory", map[string]any{
				"folderPath": folderPath,
				"err":        err.Error(),
			})
			errChan <- err
			return
		}

		var foldersList []filesystem.Folder
		var filesList []filesystem.File
		totalEntries := len(entries)
		currentIndex := 0

		// Paginate dynamically based on current settings
		for currentIndex < totalEntries {
			paginationSize := osSvc.PaginationSize // Dynamically fetch updated pagination size

			endIdx := currentIndex + paginationSize
			if endIdx > totalEntries {
				endIdx = totalEntries
			}

			for _, item := range entries[currentIndex:endIdx] {
				itemPath := filepath.Join(normalizedPath, item.Name())
				info, statErr := os.Stat(itemPath)
				if statErr != nil {
					osSvc.Logger.LogMessage("error", "Failed to retrieve item details", map[string]any{
						"itemPath": itemPath,
						"err":      statErr.Error(),
					})
					continue
				}

				metadata := osSvc.ConvertFileInfoToMap(info, itemPath)
				identifier, _ := metadata["identifier"].(string)

				if info.IsDir() {
					foldersList = append(foldersList, filesystem.Folder{
						Name:         item.Name(),
						Path:         itemPath,
						Identifier:   identifier,
						ParentID:     normalizedPath,
						LastModified: metadata["last_modified"].(string),
					})
				} else {
					filesList = append(filesList, filesystem.File{
						Name:         item.Name(),
						Path:         itemPath,
						Identifier:   identifier,
						ParentID:     normalizedPath,
						Size:         metadata["size"].(int64),
						LastModified: metadata["last_modified"].(string),
					})
				}
			}

			// Send results per batch
			foldersChan <- foldersList
			filesChan <- filesList
			errChan <- nil

			// Clear lists for the next batch
			foldersList = nil
			filesList = nil

			// Move to the next batch
			currentIndex += paginationSize
		}
	}()

	return foldersChan, filesChan, errChan
}

// GetFileContents opens a file for reading asynchronously.
func (osSvc *OSService) GetFileContents(filePath string) (<-chan io.ReadCloser, <-chan error) {
	result := make(chan io.ReadCloser, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(result)
		defer close(errChan)

		osSvc.TotalDiskReads++
		file, err := os.Open(filePath)
		if err != nil {
			osSvc.Logger.LogMessage("error", "Failed to open file", map[string]any{
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

// CreateFolderAsync ensures a folder exists (creates it if needed) asynchronously.
func (osSvc *OSService) CreateFolder(folderPath string) <-chan error {
	result := make(chan error, 1)

	go func() {
		defer close(result)
		normalizedPath := osSvc.NormalizePath(folderPath)

		if _, err := os.Stat(normalizedPath); os.IsNotExist(err) {
			if mkErr := os.MkdirAll(normalizedPath, os.ModePerm); mkErr != nil {
				osSvc.Logger.LogMessage("error", "Failed to create folder", map[string]any{
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

// UploadFile streams content asynchronously from `reader` to `filePath`, respecting the overwrite policy.
func (osSvc *OSService) UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) <-chan error {
	result := make(chan error, 1)

	go func() {
		defer close(result)

		if _, err := os.Stat(filePath); err == nil {
			overwrite, policyErr := shouldOverWrite()
			if policyErr != nil {
				osSvc.Logger.LogMessage("error", "Failed to determine overwrite policy", map[string]any{
					"filePath": filePath,
					"err":      policyErr.Error(),
				})
				result <- policyErr
				return
			}
			osSvc.TotalDiskReads++
			if !overwrite {
				osSvc.Logger.LogMessage("info", "File already exists; overwrite disabled", map[string]any{
					"filePath": filePath,
				})
				result <- nil
				return
			}
		}

		file, err := os.Create(filePath)
		if err != nil {
			osSvc.Logger.LogMessage("error", "Failed to create file", map[string]any{
				"filePath": filePath,
				"err":      err.Error(),
			})
			result <- err
			return
		}
		defer file.Close()

		if _, copyErr := io.Copy(file, reader); copyErr != nil {
			osSvc.Logger.LogMessage("error", "Failed to write file contents", map[string]any{
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

// NormalizePath ensures consistent path formatting.
func (osSvc *OSService) NormalizePath(path string) string {
	return filepath.Clean(path)
}
