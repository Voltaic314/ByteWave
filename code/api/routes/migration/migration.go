package migration

import (
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/start"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/stop"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/pause"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/resume"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/status"
	"github.com/gin-gonic/gin"
)

// RegisterRoutes registers all migration-related routes
func RegisterRoutes(router *gin.RouterGroup) {
	// Register individual route handlers
	start.RegisterRoute(router)
	stop.RegisterRoute(router)
	pause.RegisterRoute(router)
	resume.RegisterRoute(router)
	status.RegisterRoute(router)
}