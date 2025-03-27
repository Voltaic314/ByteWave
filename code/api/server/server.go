package server

import (
	"github.com/Voltaic314/ByteWave/code/api/routes/migration"
	"github.com/gin-gonic/gin"
	"net/http"
)

// Server represents the API server
type Server struct {
	router *gin.Engine
}

// NewServer creates a new API server instance
func NewServer() *Server {
	// Set Gin to release mode in production
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()
	
	// Configure router to only listen on localhost
	router.Use(func(c *gin.Context) {
		// Only allow requests from localhost
		if c.ClientIP() != "127.0.0.1" && c.ClientIP() != "::1" {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		c.Next()
	})

	// Create a new server instance
	s := &Server{
		router: router,
	}

	// Setup routes
	s.setupRoutes()

	return s
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// API version group
	api := s.router.Group("/api/v1")

	// Register migration routes
	migrationRoutes := api.Group("/migrations")
	migration.RegisterRoutes(migrationRoutes)
}

// Run starts the API server on the specified address
func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}