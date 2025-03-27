package main

import (
	"github.com/Voltaic314/ByteWave/code/api/server"
	"log"
)

func main() {
	// Initialize and start the API server
	srv := server.NewServer()
	log.Println("Starting ByteWave API server on localhost:8080")
	if err := srv.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}