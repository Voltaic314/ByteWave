package stop

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes registers the stop migration route
func RegisterRoutes(router *gin.RouterGroup) {
	router.POST("/stop", handleStopMigration)
}

// handleStopMigration handles the request to stop a migration
func handleStopMigration(c *gin.Context) {
	// TODO: Implement the actual migration stop logic
	// This is where you'll add your code to stop the migration process
	
	// For now, just return a success response
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"message": "Migration stopped successfully",
	})
}