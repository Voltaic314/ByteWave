package main

import (
	"fmt"
	"log"

	"github.com/Voltaic314/ByteWave/code/api/server"
)

func main() {
	// Initialize and start the API server
	apiServer := server.NewServer()
	
	fmt.Println("ByteWave API server starting on http://localhost:8080")
	if err := apiServer.Run(":8080"); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}
}