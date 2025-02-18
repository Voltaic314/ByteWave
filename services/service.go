package services

import (
	"io"

	"github.com/Voltaic314/Data_Migration_Tool/responsehandler"
)

// Service defines common operations for handling files & folders.
type Service interface {
	GetAllItems(folderPath string, offset int) responsehandler.Response
	CreateFolder(folderPath string) responsehandler.Response
	GetFileContents(filePath string) responsehandler.Response
	UploadFile(filePath string, reader io.Reader, shouldOverWrite func() (bool, error)) responsehandler.Response
}
