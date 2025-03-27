package server

import (
	"github.com/gin-gonic/gin"
	
	// Import all route modules
	"github.com/Voltaic314/ByteWave/code/api/routes/start"
	"github.com/Voltaic314/ByteWave/code/api/routes/stop"
	"github.com/Voltaic314/ByteWave/code/api/routes/pause"
	"github.com/Voltaic314/ByteWave/code/api/routes/resume"
	"github.com/Voltaic314/ByteWave/code/api/routes/status"
)

// Server represents the API server
type Server struct {
	router *gin.Engine
}

// NewServer creates a new API server instance
func NewServer() *Server {
	// Create a new Gin router
	router := gin.Default()
	
	// Configure router to only accept connections from localhost
	router.Use(func(c *gin.Context) {
		// Check if the request is coming from localhost
		if c.ClientIP() != "127.0.0.1" && c.ClientIP() != "::1" {
			c.AbortWithStatus(403) // Forbidden
			return
		}
		c.Next()
	})
	
	// Create a new server instance
	s := &Server{
		router: router,
	}
	
	// Initialize routes
	s.setupRoutes()
	
	return s
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// API version group
	api := s.router.Group("/api/v1")
	
	// Migrations group
	migrations := api.Group("/migrations")
	
	// Register all route modules
	start.RegisterRoutes(migrations)
	stop.RegisterRoutes(migrations)
	pause.RegisterRoutes(migrations)
	resume.RegisterRoutes(migrations)
	status.RegisterRoutes(migrations)
}

// Run starts the API server on the specified address
func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}