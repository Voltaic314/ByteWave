package main

import (
	"fmt"
	"time"
	"github.com/Voltaic314/ByteWave/code/cli"
)

func main() {
	fmt.Println("🚀 ByteWave is starting up...")

	// Optional: spawn the log viewer in a new terminal
	err := cli.SpawnReceiverTerminal()
	if err != nil {
		fmt.Println("❌ Could not launch log terminal:", err)
	}

	// Continue normal startup (logger, db, etc.)
	// logger.InitLogger(), db.Init(), traversal.Start()

	for {
		time.Sleep(1 * time.Second)
	}
}
