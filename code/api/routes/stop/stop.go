package stop

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes registers the stop route with the router
func RegisterRoutes(router *gin.RouterGroup) {
	router.POST("/stop", handleStop)
}

// handleStop handles the request to stop a migration
func handleStop(c *gin.Context) {
	// TODO: Implement the actual migration stop logic
	// This is where Logan will implement the code to stop the migration
	
	// For now, just return a success response
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"message": "Migration stopped successfully",
	})
}