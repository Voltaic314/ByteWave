package services

import (
	"os"
	"path/filepath"

	"github.com/Voltaic314/Data_Migration_Tool/responsehandler"
)

// OSService handles file and folder operations for the OS.
type OSService struct {
	TotalDiskReads  int // TODO: Replace with resource tracking system
	TotalDiskWrites int // TODO: Replace with resource tracking system
}

// NewOSService initializes the OS service.
func NewOSService() *OSService {
	return &OSService{}
}

// IsDirectory checks if a given path is a directory.
func (osSvc *OSService) IsDirectory(identifier string) responsehandler.Response {
	// TODO: Send disk read count to resource tracker channel
	osSvc.TotalDiskReads++

	info, err := os.Stat(identifier)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("os_stat", "Failed to check directory", &errMsg, map[string]interface{}{
				"path": identifier,
			}),
		}, nil)
	}
	return responsehandler.NewResponse(info.IsDir(), info.IsDir(), nil, nil)
}

// GetAllItems retrieves all items in a folder.
func (osSvc *OSService) GetAllItems(folderPath string) responsehandler.Response {
	// TODO: Send disk read count to resource tracker channel
	osSvc.TotalDiskReads++

	normalizedPath := osSvc.NormalizePath(folderPath)
	items := []string{}

	files, err := os.ReadDir(normalizedPath)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("read_dir", "Failed to read directory", &errMsg, map[string]interface{}{
				"folder": folderPath,
			}),
		}, nil)
	}

	for _, file := range files {
		items = append(items, filepath.Join(normalizedPath, file.Name()))
	}

	return responsehandler.NewResponse(true, items, nil, nil)
}

// GetFileContents retrieves file contents as bytes.
// TODO: Update this to use a file path stream instead of loading contents into memory.
func (osSvc *OSService) GetFileContents(filePath string) responsehandler.Response {
	// TODO: Send disk read count to resource tracker channel
	osSvc.TotalDiskReads++

	contents, err := os.ReadFile(filePath)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("read_file", "Failed to read file", &errMsg, map[string]interface{}{
				"file": filePath,
			}),
		}, nil)
	}
	return responsehandler.NewResponse(true, contents, nil, nil)
}

// CreateFolder creates a folder if it doesn’t exist.
func (osSvc *OSService) CreateFolder(folderPath string) responsehandler.Response {
	normalizedPath := osSvc.NormalizePath(folderPath)

	if _, err := os.Stat(normalizedPath); os.IsNotExist(err) {
		err = os.MkdirAll(normalizedPath, os.ModePerm)
		if err != nil {
			errMsg := err.Error()
			return responsehandler.NewResponse(false, nil, []responsehandler.Error{
				responsehandler.NewError("mkdir", "Failed to create folder", &errMsg, map[string]interface{}{
					"folder": folderPath,
				}),
			}, nil)
		}
		// TODO: Send disk write count to resource tracker channel
		osSvc.TotalDiskWrites++
	}

	return responsehandler.NewResponse(true, nil, nil, nil)
}

// UploadFile writes contents to a file.
// TODO: Update this to use a file path stream instead of loading contents into memory.
func (osSvc *OSService) UploadFile(filePath string, contents []byte) responsehandler.Response {
	err := os.WriteFile(filePath, contents, 0644)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("write_file", "Failed to write file", &errMsg, map[string]interface{}{
				"file": filePath,
			}),
		}, nil)
	}
	// TODO: Send disk write count to resource tracker channel
	osSvc.TotalDiskWrites++

	return responsehandler.NewResponse(true, nil, nil, nil)
}

// NormalizePath ensures consistent path formatting.
func (osSvc *OSService) NormalizePath(path string) string {
	return filepath.Clean(path)
}
