package status

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes registers the status route with the router
func RegisterRoutes(router *gin.RouterGroup) {
	router.GET("/status", handleStatus)
}

// handleStatus handles the request to get migration status
func handleStatus(c *gin.Context) {
	// TODO: Implement the actual migration status logic
	// This is where Logan will implement the code to get the migration status
	
	// For now, just return a mock status response
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"migration_status": "idle", // Could be "idle", "running", "paused", "completed", "failed"
			"progress": 0,
			"files_processed": 0,
			"total_files": 0,
			"errors": []string{},
		},
	})
}