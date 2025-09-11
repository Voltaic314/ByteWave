package tables

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

// OracleTestRunner handles Oracle DB operations for testing
type OracleTestRunner struct {
	db     *sql.DB
	config *OracleTestConfig
}

// NewOracleTestRunner creates a new test runner with the given config
func NewOracleTestRunner(config *OracleTestConfig) (*OracleTestRunner, error) {
	db, err := sql.Open("duckdb", config.Databases.Oracle)
	if err != nil {
		return nil, fmt.Errorf("open oracle db: %w", err)
	}

	return &OracleTestRunner{
		db:     db,
		config: config,
	}, nil
}

// Close closes the database connection
func (r *OracleTestRunner) Close() error {
	return r.db.Close()
}

// InitTables creates all Oracle tables
func (r *OracleTestRunner) InitTables(ctx context.Context) error {
	tables := []interface {
		Name() string
		Schema() string
	}{
		OracleSrcNodesTable{},
		OracleDstNodesTable{},
	}

	for _, table := range tables {
		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table.Name(), table.Schema())
		if _, err := r.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("creating table %q: %w", table.Name(), err)
		}
		fmt.Printf("📜 Created Oracle table: %s\n", table.Name())
	}

	return nil
}

// Cleanup removes the Oracle database file
func (r *OracleTestRunner) Cleanup() error {
	if err := r.db.Close(); err != nil {
		return err
	}

	// Remove the database file
	if err := os.Remove(r.config.Databases.Oracle); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove oracle db file: %w", err)
	}

	// Remove WAL file if it exists
	walFile := r.config.Databases.Oracle + ".wal"
	if err := os.Remove(walFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove oracle wal file: %w", err)
	}

	fmt.Println("🧹 Cleaned up Oracle database files")
	return nil
}
