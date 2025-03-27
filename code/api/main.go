package api

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// StartAPI initializes and starts the API server
func StartAPI(port string) error {
	// Create a new migration manager
	migrationMgr := NewMigrationManager()
	
	// Create a new server
	server := NewServer(migrationMgr)
	
	// Setup graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		
		log.Println("Shutting down API server...")
		// TODO: Implement graceful shutdown
		os.Exit(0)
	}()
	
	// Start the server
	log.Printf("Starting API server on http://127.0.0.1:%s\n", port)
	return server.Start(port)
}