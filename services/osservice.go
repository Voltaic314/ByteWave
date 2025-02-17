package services

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"runtime"

    // "golang.org/x/sys/unix"
    "golang.org/x/sys/windows"

	"github.com/Voltaic314/Data_Migration_Tool/responsehandler"
	"github.com/Voltaic314/Data_Migration_Tool/filesystem"
)

// OSService handles file and folder operations for the OS.
type OSService struct {
	TotalDiskReads  int // TODO: Replace with resource tracking system
	TotalDiskWrites int // TODO: Replace with resource tracking system
	PaginationSize  int
}

// NewOSService initializes the OS service with a default pagination size.
func NewOSService() *OSService {
	return &OSService{PaginationSize: 100}
}

// IsDirectory checks if a given path is a directory.
func (osSvc *OSService) IsDirectory(path string) responsehandler.Response {
	// TODO: Send disk read count to resource tracker channel
	osSvc.TotalDiskReads++

	info, err := os.Stat(path)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("os_stat", "Failed to check directory", &errMsg, map[string]interface{}{
				"path": path,
			}),
		}, nil)
	}
	return responsehandler.NewResponse(true, info.IsDir(), nil, nil)
}

// GetAllItems retrieves all items in a folder with pagination support.
func (osSvc *OSService) GetAllItems(folderPath string, offset int) responsehandler.Response {
	// TODO: Send disk read count to resource tracker channel
	osSvc.TotalDiskReads++

	normalizedPath := osSvc.NormalizePath(folderPath)
	var (
		filesList   []filesystem.File
		foldersList []filesystem.Folder
		errorsList  []responsehandler.Error
	)

	files, err := os.ReadDir(normalizedPath)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("read_dir", "Failed to read directory", &errMsg, map[string]interface{}{
				"folder": folderPath,
			}),
		}, nil)
	}

	// Apply pagination logic
	startIdx := offset * osSvc.PaginationSize
	endIdx := startIdx + osSvc.PaginationSize
	if startIdx >= len(files) {
		return responsehandler.NewResponse(true, map[string]interface{}{
			"folders": foldersList,
			"files":   filesList,
		}, nil, nil)
	}
	if endIdx > len(files) {
		endIdx = len(files)
	}

	for _, item := range files[startIdx:endIdx] {
		itemPath := filepath.Join(normalizedPath, item.Name())
		info, err := os.Stat(itemPath)
		if err != nil {
			errMsg := err.Error()
			errorsList = append(errorsList, responsehandler.NewError(
				"stat_failed",
				"Failed to retrieve item details",
				&errMsg,
				map[string]interface{}{"item": itemPath},
			))
			continue // Log the error but still process what we can
		}

		if info.IsDir() {
			foldersList = append(foldersList, filesystem.Folder{
				Name:         item.Name(),
				Path:         itemPath,
				Identifier:   osSvc.GetFileIdentifier(info),
				ParentID:     normalizedPath,
				LastModified: info.ModTime().Format(time.RFC3339),
			})
		} else {
			filesList = append(filesList, filesystem.File{
				Name:         item.Name(),
				Path:         itemPath,
				Identifier:   osSvc.GetFileIdentifier(info),
				ParentID:     normalizedPath,
				Size:         info.Size(),
				LastModified: info.ModTime().Format(time.RFC3339),
			})
		}
	}

	return responsehandler.NewResponse(true, map[string]interface{}{
		"folders": foldersList,
		"files":   filesList,
	}, errorsList, nil) // Errors included but don’t halt execution
}

// GetFileContents retrieves file contents as a stream instead of loading into memory.
func (osSvc *OSService) GetFileContents(filePath string) responsehandler.Response {
	// TODO: Send disk read count to resource tracker channel
	osSvc.TotalDiskReads++

	file, err := os.Open(filePath)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("read_file", "Failed to open file", &errMsg, map[string]interface{}{
				"file": filePath,
			}),
		}, nil)
	}

	return responsehandler.NewResponse(true, file, nil, nil)
}

// DefaultOverwritePolicy: Only overwrite if the source file is newer than the destination.
func DefaultOverwritePolicy(srcFile, dstFile filesystem.File) (bool, error) {
	srcTime, err := time.Parse(time.RFC3339, srcFile.LastModified)
	if err != nil {
		return false, err
	}
	dstTime, err := time.Parse(time.RFC3339, dstFile.LastModified)
	if err != nil {
		return false, err
	}

	// Overwrite if src is newer than dst
	return srcTime.After(dstTime), nil
}

// ForceOverwritePolicy: Always return true, forcing overwrite.
func ForceOverwritePolicy(srcFile, dstFile filesystem.File) (bool, error) {
	return true, nil
}

// GetFileIdentifier retrieves a unique identifier for a file (Inode on Unix, MFT ID on Windows).
func (osSvc *OSService) GetFileIdentifier(info os.FileInfo) string {
    switch runtime.GOOS {
    case "windows":
        return osSvc.getFileIdentifierWindows(info)
    // case "linux", "darwin":
    //     return osSvc.getFileIdentifierUnix(info)
    default:
        return ""
    }
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

// UploadFile streams file contents to disk instead of loading into memory.
func (osSvc *OSService) UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) responsehandler.Response {
	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		// Overwrite policy
		overwrite, err := shouldOverWrite()
		if err != nil {
			errMsg := err.Error()
			return responsehandler.NewResponse(false, nil, []responsehandler.Error{
				responsehandler.NewError("overwrite_policy", "Failed to determine overwrite policy", &errMsg, map[string]interface{}{
					"file": filePath,
				}),
			}, nil)
		}
		osSvc.TotalDiskReads++
		if !overwrite {
			return responsehandler.NewResponse(false, nil, []responsehandler.Error{
				responsehandler.NewError("overwrite", "File already exists and overwrite is disabled", nil, map[string]interface{}{
					"file": filePath,
				}),
			}, nil)
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		errMsg := err.Error()
		return responsehandler.NewResponse(false, nil, []responsehandler.Error{
			responsehandler.NewError("write_file", "Failed to create file", &errMsg, map[string]interface{}{
				"file": filePath,
			}),
		}, nil)
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
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

// getFileIdentifierUnix retrieves the inode number on Unix systems.
// If we ever decide to make this linux compatible we could use this function
// func (osSvc *OSService) getFileIdentifierUnix(info os.FileInfo) string {
//     stat, ok := info.Sys().(*unix.Stat_t)
//     if !ok {
//         return ""
//     }
//     return fmt.Sprintf("%d", stat.Ino)
// }

// getFileIdentifierWindows retrieves the NTFS MFT file index on Windows.
func (osSvc *OSService) getFileIdentifierWindows(info os.FileInfo) string {
    handle, err := syscall.CreateFile(
        syscall.StringToUTF16Ptr(info.Name()),
        syscall.GENERIC_READ,
        syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
        nil,
        syscall.OPEN_EXISTING,
        syscall.FILE_FLAG_BACKUP_SEMANTICS,
        0,
    )
    if err != nil {
        return ""
    }
    defer syscall.CloseHandle(handle)

    var fileInfo windows.ByHandleFileInformation
    err = windows.GetFileInformationByHandle(windows.Handle(handle), &fileInfo)
    if err != nil {
        return ""
    }
    return fmt.Sprintf("%d-%d", fileInfo.FileIndexHigh, fileInfo.FileIndexLow)
}

// NormalizePath ensures consistent path formatting.
func (osSvc *OSService) NormalizePath(path string) string {
	return filepath.Clean(path)
}
