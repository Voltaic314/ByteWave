/*
ByteWave — Free and Open Source File Migration Tool
Licensed under the ByteWave License v1.0
See LICENSE.md for full terms.

You may use, modify, and distribute this software freely for internal or
service-based commercial purposes, but resale as a stand-alone software product
is prohibited. Attribution to the ByteWave Project is required in all
public-facing deployments.
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Voltaic314/ByteWave/code/cli"
	"github.com/Voltaic314/ByteWave/code/logging"
	"github.com/Voltaic314/ByteWave/code/processing"
	"github.com/Voltaic314/ByteWave/code/signals"
	typesdb "github.com/Voltaic314/ByteWave/code/types/db"
)

func main() {
	fmt.Println("🚀 ByteWave is starting up...")

	fmt.Println("🔖 Licensed under the ByteWave License v1.0 — see LICENSE.txt or visit https://bytewave.stream/license")

	// Spawn UDP log viewer
	err := cli.SpawnReceiverTerminal()
	if err != nil {
		fmt.Println("❌ Could not launch log terminal:", err)
	}

	// Get current working directory for dynamic path resolution
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("❌ Could not determine current working directory:", err)
		return
	}

	// Initialize logger (UDP-only for now)
	logSettingsPath := filepath.Join(cwd, "settings", "log_settings.json")
	logging.InitLogger(logSettingsPath)

	// Initialize Signal Router for task publication broadcasts
	signals.InitSignalRouter()

	// Give the log terminal a sec to boot up
	time.Sleep(3 * time.Second)

	// 🔊 DEBUG: Send a test log to verify logger is working
	logging.GlobalLogger.LogMessage("info", "Test log: Logger is alive and ready 🚦", map[string]any{
		"origin": "main.go",
		"status": "init-complete",
	})

	// Start the Conductor — now self-contained (handles its own DB + logger)
	dbPath := filepath.Join(cwd, "tests", "traversal_tests", "test_traversal.db")
	conductor := processing.NewConductor(
		dbPath,
		3,    // retry threshold
		1000, // batch size
	)

	if conductor == nil {
		fmt.Println("❌ Failed to initialize Conductor.")
		return
	}

	// Initialize write queues before starting traversal
	conductor.DB.InitWriteQueue("audit_log", typesdb.LogWriteQueue, 50, 5*time.Second)
	conductor.DB.InitWriteQueue("source_nodes", typesdb.NodeWriteQueue, 100, 5*time.Second)
	conductor.DB.InitWriteQueue("destination_nodes", typesdb.NodeWriteQueue, 100, 5*time.Second)

	// Register the logger with the DB
	// This is basically so that the logger can write logs persistently to the DB
	logging.GlobalLogger.RegisterDB(conductor.DB)

	// Start timing the traversal
	startTime := time.Now()
	fmt.Printf("🚀 Starting traversal at %s\n", startTime.Format("15:04:05"))

	go conductor.StartTraversal()

	// Keep main alive until all traversals are complete (blocking)
	for {
		time.Sleep(50 * time.Millisecond)

		// Check if all queues have been torn down (traversal complete)
		if len(conductor.QP.Queues) == 0 {
			endTime := time.Now()
			duration := endTime.Sub(startTime)
			fmt.Printf("✅ All traversals complete! ByteWave shutting down...\n")
			fmt.Printf("⏱️  Total traversal time: %v\n", duration)
			break
		}
	}

	// Clean shutdown
	conductor.DB.Close()
	logging.GlobalLogger.Stop()
	fmt.Println("🛑 ByteWave shutdown complete.")
}
