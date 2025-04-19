package main

import (
	"fmt"
	"time"

	"github.com/Voltaic314/ByteWave/code/cli"
	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/processing"
)

func main() {
	fmt.Println("🚀 ByteWave is starting up...")

	// Spawn UDP log viewer
	err := cli.SpawnReceiverTerminal()
	if err != nil {
		fmt.Println("❌ Could not launch log terminal:", err)
	}

	// Initialize logger (UDP-only for now)
	core.InitLogger("C:\\Users\\golde\\OneDrive\\Documents\\GitHub\\ByteWave\\settings\\log_settings.json")

	// Give the log terminal a sec to boot up
	time.Sleep(3 * time.Second)

	// 🔊 DEBUG: Send a test log to verify logger is working
	core.GlobalLogger.LogMessage("info", "Test log: Logger is alive and ready 🚦", map[string]any{
		"origin": "main.go",
		"status": "init-complete",
	})
	
	// Start the Conductor — now self-contained (handles its own DB + logger)
	conductor := processing.NewConductor(
		"C:\\Users\\golde\\OneDrive\\Documents\\GitHub\\ByteWave\\tests\\traversal_tests\\test_src_traversal.db",
		3,  // retry threshold
		10, // batch size
	)

	if conductor == nil {
		fmt.Println("❌ Failed to initialize Conductor.")
		return
	}

	conductor.StartTraversal()

	// Keep main alive so everything can run
	for {
		time.Sleep(1 * time.Second)
	}
}
