package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

// handleHealth handles health check requests
func (s *MockFSServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"service": "mockfs",
	})
}

// handleListChildren handles requests to list folder contents
func (s *MockFSServer) handleListChildren(w http.ResponseWriter, r *http.Request) {
	path := chi.URLParam(r, "path")
	if path == "" {
		path = "/"
	}

	// Ensure path starts with /
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Create request
	requestID := fmt.Sprintf("%d", time.Now().UnixNano())
	responseCh := make(chan ListChildrenResponse, 1)

	req := ListChildrenRequest{
		ParentPath: path,
		ResponseCh: responseCh,
		RequestID:  requestID,
	}

	// Send to batcher
	select {
	case s.batcher.requests <- req:
		// Request queued successfully
	case <-time.After(5 * time.Second):
		http.Error(w, "Request timeout", http.StatusRequestTimeout)
		return
	}

	// Wait for response
	select {
	case resp := <-responseCh:
		w.Header().Set("Content-Type", "application/json")
		if resp.Success {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	case <-time.After(10 * time.Second):
		http.Error(w, "Response timeout", http.StatusRequestTimeout)
		return
	}
}

// handleIsDirectory handles requests to check if a path is a directory
func (s *MockFSServer) handleIsDirectory(w http.ResponseWriter, r *http.Request) {
	path := chi.URLParam(r, "path")
	if path == "" {
		path = "/"
	}

	// Ensure path starts with /
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Query the database to check if path exists and is a folder
	query := `
		SELECT type FROM source_nodes WHERE path = ? AND type = 'folder'
		UNION ALL
		SELECT type FROM destination_nodes WHERE path = ? AND type = 'folder'
		LIMIT 1
	`

	rows, err := s.oracleDB.Query("source_nodes", query, path, path)
	if err != nil {
		http.Error(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	isDirectory := false
	if rows.Next() {
		var nodeType string
		if err := rows.Scan(&nodeType); err == nil && nodeType == "folder" {
			isDirectory = true
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{
		"is_directory": isDirectory,
	})
}

// handleCreateFolder handles requests to create a new folder
func (s *MockFSServer) handleCreateFolder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ParentPath string `json:"parent_path"`
		Name       string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Generate new folder path
	folderPath := filepath.Join(req.ParentPath, req.Name)
	if !strings.HasPrefix(folderPath, "/") {
		folderPath = "/" + folderPath
	}

	// For now, just return success (in a real implementation, you'd insert into DB)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    folderPath,
		"message": "Folder creation not implemented yet",
	})
}

// handleCreateFile handles requests to create a new file
func (s *MockFSServer) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ParentPath string `json:"parent_path"`
		Name       string `json:"name"`
		Size       int64  `json:"size"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Generate new file path
	filePath := filepath.Join(req.ParentPath, req.Name)
	if !strings.HasPrefix(filePath, "/") {
		filePath = "/" + filePath
	}

	// For now, just return success (in a real implementation, you'd insert into DB)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    filePath,
		"size":    req.Size,
		"message": "File creation not implemented yet",
	})
}

// handleGetFileContents handles requests to get file contents (returns download URL)
func (s *MockFSServer) handleGetFileContents(w http.ResponseWriter, r *http.Request) {
	fileID := chi.URLParam(r, "fileID")
	filename := chi.URLParam(r, "filename")

	if fileID == "" || filename == "" {
		http.Error(w, "Missing file ID or filename", http.StatusBadRequest)
		return
	}

	// Generate download URL
	downloadURL := fmt.Sprintf("http://%s:%d/download/%s/%s",
		s.config.Network.Address,
		s.config.Network.Port,
		fileID,
		filename,
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":      true,
		"download_url": downloadURL,
	})
}

// handleDownloadFile handles actual file downloads (returns random binary data)
func (s *MockFSServer) handleDownloadFile(w http.ResponseWriter, r *http.Request) {
	fileID := chi.URLParam(r, "fileID")
	filename := chi.URLParam(r, "filename")

	if fileID == "" || filename == "" {
		http.Error(w, "Missing file ID or filename", http.StatusBadRequest)
		return
	}

	// Query database for file size
	query := `
		SELECT size FROM source_nodes WHERE id = ? AND type = 'file'
		UNION ALL
		SELECT size FROM destination_nodes WHERE id = ? AND type = 'file'
		LIMIT 1
	`

	rows, err := s.oracleDB.Query("source_nodes", query, fileID, fileID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var fileSize int64 = 1024 // Default size
	if rows.Next() {
		if err := rows.Scan(&fileSize); err != nil {
			fileSize = 1024 // Fallback to default
		}
	}

	// Set headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))

	// Generate random binary data
	randomData := make([]byte, fileSize)
	for i := range randomData {
		randomData[i] = byte(i % 256)
	}

	w.Write(randomData)
}
