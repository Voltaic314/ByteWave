package database

import "log"

// InitializeErrorsTable sets up the node_errors table.
func (db *DB) InitializeErrorsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS node_errors (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		node_id INTEGER NOT NULL,
		error_type TEXT NOT NULL,
		error_message TEXT NOT NULL,
		error_details TEXT,
		timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		retry_count INTEGER DEFAULT 0,
		FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
	);
	`
	if err := db.Write(query); err != nil {
		log.Printf("Failed to create node_errors table: %v", err)
		return err
	}
	return nil
}
