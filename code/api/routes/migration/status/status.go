package status

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// RegisterRoute registers the migration status route
func RegisterRoute(router *gin.RouterGroup) {
	router.GET("/status", handleGetMigrationStatus)
}

// handleGetMigrationStatus handles the request to get migration status
func handleGetMigrationStatus(c *gin.Context) {
	// TODO: Implement the actual migration status logic
	// This is where Logan will implement the code to get the migration status

	// For now, just return a mock status response
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"migration_status": "running",
			"progress": 45,
			"files_processed": 123,
			"total_files": 275,
			"current_file": "documents/reports/annual_2024.pdf",
			"errors": []string{},
		},
	})
}