package server

import (
	"github.com/gin-gonic/gin"
	"github.com/Voltaic314/ByteWave/code/api/routes"
)

// Server represents the API server
type Server struct {
	router *gin.Engine
}

// NewServer creates and configures a new API server
func NewServer() *Server {
	// Create a new server instance
	server := &Server{
		router: gin.Default(),
	}

	// Configure the router
	server.setupRoutes()

	return server
}

// setupRoutes configures all the routes for the API
func (s *Server) setupRoutes() {
	// API version group
	v1 := s.router.Group("/api/v1")
	
	// Register migration routes
	routes.RegisterMigrationRoutes(v1)
	
	// Additional route groups can be added here as the API expands
	// Example: routes.RegisterSettingsRoutes(v1)
}

// Run starts the HTTP server
func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}