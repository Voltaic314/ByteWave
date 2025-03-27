package main

import (
	"log"

	"github.com/Voltaic314/ByteWave/code/api/server"
)

func main() {
	// Initialize and start the API server
	srv := server.NewServer()
	
	log.Println("Starting ByteWave API server on http://localhost:8080")
	if err := srv.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}