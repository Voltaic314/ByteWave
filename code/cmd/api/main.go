package main

import (
	"flag"
	"log"

	"github.com/Voltaic314/ByteWave/code/api"
)

func main() {
	// Parse command line flags
	port := flag.String("port", "8080", "Port to run the API server on")
	flag.Parse()
	
	// Start the API server
	if err := api.StartAPI(*port); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}
}