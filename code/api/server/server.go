package server

import (
	"github.com/gin-gonic/gin"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/start"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/stop"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/pause"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/resume"
	"github.com/Voltaic314/ByteWave/code/api/routes/migration/status"
)

// Server represents the API server
type Server struct {
	router *gin.Engine
}

// NewServer creates a new API server instance
func NewServer() *Server {
	router := gin.Default()
	
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
	v1 := s.router.Group("/api/v1")
	
	// Migrations group
	migrations := v1.Group("/migrations")
	
	// Register migration routes
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