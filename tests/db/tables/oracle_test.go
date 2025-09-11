package tables

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestOracleSchema(t *testing.T) {
	// Load config
	configData, err := os.ReadFile("../../traversal_tests/mock_fs_config.json")
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	}

	var config OracleTestConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// Create test runner
	runner, err := NewOracleTestRunner(&config)
	if err != nil {
		t.Fatalf("Failed to create test runner: %v", err)
	}
	defer runner.Close()

	// Clean up any existing test files
	runner.Cleanup()

	// Recreate runner after cleanup
	runner, err = NewOracleTestRunner(&config)
	if err != nil {
		t.Fatalf("Failed to recreate test runner: %v", err)
	}
	defer runner.Close()

	// Initialize tables
	ctx := context.Background()
	if err := runner.InitTables(ctx); err != nil {
		t.Fatalf("Failed to initialize tables: %v", err)
	}

	// Test that tables exist by querying them
	var count int
	err = runner.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM source_nodes").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query source_nodes: %v", err)
	}

	err = runner.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM destination_nodes").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query destination_nodes: %v", err)
	}

	t.Logf("✅ Oracle schema test passed - both tables created successfully")
}
