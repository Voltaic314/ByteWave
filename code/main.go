package main

import (
	"fmt"
	"time"

	"github.com/Voltaic314/ByteWave/code/cli"
	"github.com/Voltaic314/ByteWave/code/core"
	"github.com/Voltaic314/ByteWave/code/core/db"
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

	// Connect to DB
	dbPath := "C:\\Users\\golde\\OneDrive\\Documents\\GitHub\\ByteWave\\tests\\traversal_tests\\test_src_traversal.db"
	dbInstance, err := db.NewDB(dbPath)
	if err != nil {
		fmt.Println("❌ Failed to connect to DuckDB:", err)
		return
	}

	// Register logger to DB
	core.GlobalLogger.RegisterDB(dbInstance)

	// Initialize Conductor and start traversal
	conductor := processing.NewConductor(dbPath, 3, 10, core.GlobalLogger)
	conductor.StartTraversal()

	// Keep main alive
	for {
		time.Sleep(1 * time.Second)
	}
}
