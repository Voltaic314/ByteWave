package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/Voltaic314/ByteWave/code/api/handlers/migration"
)

// RegisterMigrationRoutes registers all migration-related routes
func RegisterMigrationRoutes(router *gin.RouterGroup) {
	// Create a migrations group
	migrationGroup := router.Group("/migrations")
	
	// Register the migration routes
	migrationGroup.POST("/start", migration.StartMigration)
	migrationGroup.POST("/stop", migration.StopMigration)
	migrationGroup.POST("/pause", migration.PauseMigration)
	migrationGroup.POST("/resume", migration.ResumeMigration)
	migrationGroup.GET("/status", migration.GetMigrationStatus)
}