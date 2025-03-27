package main

import (
	"fmt"
	"log"

	"github.com/Voltaic314/ByteWave/code/api/server"
)

func main() {
	// Initialize and start the API server
	apiServer := server.NewServer()
	
	// Start the server on localhost only
	port := 8080
	address := fmt.Sprintf("127.0.0.1:%d", port)
	
	log.Printf("Starting ByteWave API server on %s", address)
	if err := apiServer.Run(address); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}