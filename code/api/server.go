package api

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Server represents the API server
type Server struct {
	router      *gin.Engine
	migrationMgr MigrationManager
}

// NewServer creates a new API server instance
func NewServer(migrationMgr MigrationManager) *Server {
	// Set Gin to release mode in production
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()
	
	server := &Server{
		router:      router,
		migrationMgr: migrationMgr,
	}
	
	// Setup routes
	server.setupRoutes()
	
	return server
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// API v1 group
	v1 := s.router.Group("/api/v1")
	{
		// Migration control endpoints
		migrations := v1.Group("/migrations")
		{
			migrations.POST("/start", s.startMigration)
			migrations.POST("/stop", s.stopMigration)
			migrations.POST("/pause", s.pauseMigration)
			migrations.POST("/resume", s.resumeMigration)
			
			// Status endpoint
			migrations.GET("/status", s.getMigrationStatus)
		}
	}
}

// Start begins the API server on localhost only
func (s *Server) Start(port string) error {
	// Only bind to localhost (127.0.0.1) for security
	return s.router.Run("127.0.0.1:" + port)
}

// startMigration handles the request to start a new migration
func (s *Server) startMigration(c *gin.Context) {
	// Parse migration configuration from request
	var config MigrationConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format", "details": err.Error()})
		return
	}
	
	// Start the migration
	id, err := s.migrationMgr.StartMigration(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start migration", "details": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Migration started successfully",
		"id": id,
	})
}

// stopMigration handles the request to stop a migration
func (s *Server) stopMigration(c *gin.Context) {
	// Check if ID is provided in the request body
	var req struct {
		ID string `json:"id"`
	}
	
	if err := c.ShouldBindJSON(&req); err == nil && req.ID != "" {
		// If ID is provided, use it to stop the specific migration
		err := s.migrationMgr.StopMigration(req.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to stop migration", "details": err.Error()})
			return
		}
	} else {
		// Otherwise, stop the current migration
		err := s.migrationMgr.StopCurrentMigration()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to stop current migration", "details": err.Error()})
			return
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Migration stopped successfully",
	})
}

// pauseMigration handles the request to pause a migration
func (s *Server) pauseMigration(c *gin.Context) {
	// Check if ID is provided in the request body
	var req struct {
		ID string `json:"id"`
	}
	
	if err := c.ShouldBindJSON(&req); err == nil && req.ID != "" {
		// If ID is provided, use it to pause the specific migration
		err := s.migrationMgr.PauseMigration(req.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to pause migration", "details": err.Error()})
			return
		}
	} else {
		// Otherwise, pause the current migration
		err := s.migrationMgr.PauseCurrentMigration()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to pause current migration", "details": err.Error()})
			return
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Migration paused successfully",
	})
}

// resumeMigration handles the request to resume a paused migration
func (s *Server) resumeMigration(c *gin.Context) {
	// Check if ID is provided in the request body
	var req struct {
		ID string `json:"id"`
	}
	
	if err := c.ShouldBindJSON(&req); err == nil && req.ID != "" {
		// If ID is provided, use it to resume the specific migration
		err := s.migrationMgr.ResumeMigration(req.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resume migration", "details": err.Error()})
			return
		}
	} else {
		// Otherwise, resume the current migration
		err := s.migrationMgr.ResumeCurrentMigration()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resume current migration", "details": err.Error()})
			return
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Migration resumed successfully",
	})
}

// getMigrationStatus handles the request to get the status of a migration
func (s *Server) getMigrationStatus(c *gin.Context) {
	// Get migration ID from query parameter
	id := c.Query("id")
	
	// If ID is provided, get status for that migration
	if id != "" {
		status, err := s.migrationMgr.GetMigrationStatus(id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get migration status", "details": err.Error()})
			return
		}
		
		c.JSON(http.StatusOK, status)
		return
	}
	
	// Otherwise, get status for the current migration
	status, err := s.migrationMgr.GetCurrentMigrationStatus()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get current migration status", "details": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, status)
}