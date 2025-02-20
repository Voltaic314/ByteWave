package services

import (
    "fmt"
    "io"
    "os"
    "path/filepath"
    "runtime"
    "syscall"
    "time"

    "golang.org/x/sys/windows"

    "github.com/Voltaic314/Data_Migration_Tool/code/core"
    "github.com/Voltaic314/Data_Migration_Tool/code/core/filesystem"
)

// OSService handles file/folder operations on the OS.
type OSService struct {
    Logger          *core.Logger
    TotalDiskReads  int
    TotalDiskWrites int
    PaginationSize  int
}

// NewOSService initializes the OS service with a default pagination size,
// and injects the logger dependency.
func NewOSService(logger *core.Logger) *OSService {
    return &OSService{
        Logger:         logger,
        PaginationSize: 100,
    }
}

// IsDirectory checks if a given path is a directory.
// Returns (true, nil) if path is a directory, (false, nil) if it's a file,
// or (false, err) if there's an error.
func (osSvc *OSService) IsDirectory(path string) (bool, error) {
    osSvc.TotalDiskReads++

    info, err := os.Stat(path)
    if err != nil {
        osSvc.Logger.LogMessage("error", "Failed to check directory status", map[string]interface{}{
            "path": path,
            "err":  err.Error(),
        })
        return false, err
    }
    return info.IsDir(), nil
}


// ConvertFileInfoToMap converts os.FileInfo into a generic map[string]interface{}
// and retrieves a proper unique identifier (MFT ID on Windows, Inode on Linux/macOS).
func (osSvc *OSService) ConvertFileInfoToMap(info os.FileInfo, path string) map[string]interface{} {
    metadata := map[string]interface{}{
        "name":          info.Name(),
        "path":          path,
        "size":          info.Size(),
        "is_dir":        info.IsDir(),
        "last_modified": info.ModTime().Format(time.RFC3339),
    }

    // Retrieve and store the unique file identifier (MFT or Inode)
    metadata["identifier"] = osSvc.GetFileIdentifier(path)

    return metadata
}

// GetAllItems retrieves all items in a folder with pagination support.
// Returns slices of folders, files, and possibly an error.
// Logs partial errors (e.g., stat failures) but continues listing other items.
func (osSvc *OSService) GetAllItems(folderPath string, offset int) ([]filesystem.Folder, []filesystem.File, error) {
    osSvc.TotalDiskReads++

    normalizedPath := osSvc.NormalizePath(folderPath)
    entries, err := os.ReadDir(normalizedPath)
    if err != nil {
        osSvc.Logger.LogMessage("error", "Failed to read directory", map[string]interface{}{
            "folderPath": folderPath,
            "err":        err.Error(),
        })
        return nil, nil, err
    }

    var (
        foldersList []filesystem.Folder
        filesList   []filesystem.File
    )

    // Apply pagination logic
    startIdx := offset * osSvc.PaginationSize
    endIdx := startIdx + osSvc.PaginationSize
    if startIdx >= len(entries) {
        // No items in this “page”
        return foldersList, filesList, nil
    }
    if endIdx > len(entries) {
        endIdx = len(entries)
    }

    // Collect partial errors in a slice so we can return at least one if needed.
    var partialErrs []error

    for _, item := range entries[startIdx:endIdx] {
        itemPath := filepath.Join(normalizedPath, item.Name())
        info, statErr := os.Stat(itemPath)
        if statErr != nil {
            osSvc.Logger.LogMessage("error", "Failed to retrieve item details", map[string]interface{}{
                "itemPath": itemPath,
                "err":      statErr.Error(),
            })
            partialErrs = append(partialErrs, statErr)
            continue
        }
    
        metadata := osSvc.ConvertFileInfoToMap(info, itemPath)
    
        if info.IsDir() {
            foldersList = append(foldersList, filesystem.Folder{
                Name:         item.Name(),
                Path:         itemPath,
                Identifier:   metadata["identifier"].(string),
                ParentID:     normalizedPath,
                LastModified: metadata["last_modified"].(string),
            })
        } else {
            filesList = append(filesList, filesystem.File{
                Name:         item.Name(),
                Path:         itemPath,
                Identifier:   metadata["identifier"].(string),
                ParentID:     normalizedPath,
                Size:         metadata["size"].(int64),
                LastModified: metadata["last_modified"].(string),
            })
        }
    }
    
    // If partial errors occurred, you can return one of them here
    // or create an aggregate error. At least they've all been logged.
    if len(partialErrs) > 0 {
        return foldersList, filesList, partialErrs[0]
    }

    return foldersList, filesList, nil
}

// GetFileContents opens a file for reading.
// Returns an io.ReadCloser (so callers can stream the file) + error if any.
func (osSvc *OSService) GetFileContents(filePath string) (io.ReadCloser, error) {
    osSvc.TotalDiskReads++

    file, err := os.Open(filePath)
    if err != nil {
        osSvc.Logger.LogMessage("error", "Failed to open file", map[string]interface{}{
            "filePath": filePath,
            "err":      err.Error(),
        })
        return nil, err
    }
    return file, nil
}

// GetFileIdentifier retrieves the unique ID (MFT ID on Windows, Inode on Linux/macOS).
func (osSvc *OSService) GetFileIdentifier(path string) string {
    switch runtime.GOOS {
    case "windows":
        return osSvc.getFileIdentifierWindows(path)
    // case "linux", "darwin":
        // return osSvc.getFileIdentifierUnix(path)
    default:
        return ""
    }
}


// CreateFolder ensures a folder exists (creates it if needed).
func (osSvc *OSService) CreateFolder(folderPath string) error {
    normalizedPath := osSvc.NormalizePath(folderPath)

    if _, err := os.Stat(normalizedPath); os.IsNotExist(err) {
        if mkErr := os.MkdirAll(normalizedPath, os.ModePerm); mkErr != nil {
            osSvc.Logger.LogMessage("error", "Failed to create folder", map[string]interface{}{
                "folderPath": folderPath,
                "err":        mkErr.Error(),
            })
            return mkErr
        }
        osSvc.TotalDiskWrites++
    }

    return nil
}

// UploadFile streams the content from `reader` to `filePath` on disk, respecting the overwrite policy.
func (osSvc *OSService) UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) error {
    // Check if file already exists
    if _, err := os.Stat(filePath); err == nil {
        overwrite, policyErr := shouldOverWrite()
        if policyErr != nil {
            osSvc.Logger.LogMessage("error", "Failed to determine overwrite policy", map[string]interface{}{
                "filePath": filePath,
                "err":      policyErr.Error(),
            })
            return policyErr
        }
        osSvc.TotalDiskReads++
        if !overwrite {
            osSvc.Logger.LogMessage("info", "File already exists; overwrite disabled", map[string]interface{}{
            "filePath": filePath,
            })
            return nil
        }
    }

    file, err := os.Create(filePath)
    if err != nil {
        osSvc.Logger.LogMessage("error", "Failed to create file", map[string]interface{}{
            "filePath": filePath,
            "err":      err.Error(),
        })
        return err
    }
    defer file.Close()

    if _, copyErr := io.Copy(file, reader); copyErr != nil {
        osSvc.Logger.LogMessage("error", "Failed to write file contents", map[string]interface{}{
            "filePath": filePath,
            "err":      copyErr.Error(),
        })
        return copyErr
    }

    osSvc.TotalDiskWrites++
    return nil
}

// getFileIdentifierWindows retrieves the NTFS MFT file index on Windows.
func (osSvc *OSService) getFileIdentifierWindows(path string) string {
    handle, err := syscall.CreateFile(
        syscall.StringToUTF16Ptr(path),
        syscall.GENERIC_READ,
        syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
        nil,
        syscall.OPEN_EXISTING,
        syscall.FILE_FLAG_BACKUP_SEMANTICS,
        0,
    )
    if err != nil {
        osSvc.Logger.LogMessage("error", "Failed to get file handle for MFT index", map[string]interface{}{
            "path": path,
            "err":  err.Error(),
        })
        return ""
    }
    defer syscall.CloseHandle(handle)

    var fileInfo windows.ByHandleFileInformation
    err = windows.GetFileInformationByHandle(windows.Handle(handle), &fileInfo)
    if err != nil {
        osSvc.Logger.LogMessage("error", "Failed to retrieve file info for MFT index", map[string]interface{}{
            "path": path,
            "err":  err.Error(),
        })
        return ""
    }

    return fmt.Sprintf("%d-%d", fileInfo.FileIndexHigh, fileInfo.FileIndexLow)
}

// uncomment this out for unix builds. Will only be building windows for now in V1.
// getFileIdentifierUnix retrieves the Inode number on Unix-based systems (Linux/macOS).
// func (osSvc *OSService) getFileIdentifierUnix(path string) string {
//     var stat syscall.Stat_t
//     err := syscall.Stat(path, &stat)
//     if err != nil {
//         osSvc.Logger.LogMessage("error", "Failed to retrieve file Inode number", map[string]interface{}{
//             "path": path,
//             "err":  err.Error(),
//         })
//         return ""
//     }
//     return fmt.Sprintf("%d", stat.Ino)
// }

// NormalizePath ensures consistent path formatting.
func (osSvc *OSService) NormalizePath(path string) string {
    return filepath.Clean(path)
}
