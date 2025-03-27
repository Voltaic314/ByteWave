package stop

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// RegisterRoute registers the stop migration route
func RegisterRoute(router *gin.RouterGroup) {
	router.POST("/stop", handleStopMigration)
}

// handleStopMigration handles the request to stop a migration
func handleStopMigration(c *gin.Context) {
	// TODO: Implement the actual migration stop logic
	// This is where Logan will implement the code to stop the migration

	// For now, just return a success response
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"message": "Migration stopped successfully",
	})
}