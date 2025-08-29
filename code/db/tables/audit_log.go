package tables

import (
	"github.com/Voltaic314/ByteWave/code/db"
)

// AuditLogTable defines the schema for the "audit_log" table.
type AuditLogTable struct{}

// Name returns the name of the audit log table.
func (t AuditLogTable) Name() string {
	return "audit_log"
}

// Schema returns the DuckDB-compatible schema definition.
func (t AuditLogTable) Schema() string {
	return `
		id VARCHAR PRIMARY KEY, 
		-- Use a UUID for the primary key
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		level VARCHAR NOT NULL CHECK(level IN ('trace', 'debug', 'info', 'warning', 'error', 'critical')),
		entity VARCHAR DEFAULT NULL,
		-- Entity type: 'worker', 'user', 'system', 'QP', 'Conductor', 'API', 'cloud storage service', etc.
		entity_id VARCHAR DEFAULT NULL,
		-- Unique identifier for the entity (worker ID, service ID, etc.)
		details VARCHAR DEFAULT NULL,
		message VARCHAR NOT NULL,
		action VARCHAR DEFAULT NULL,
		-- Action like 'CREATE_WORKER' or 'INSERT_TASKS' capital snake case style 
		topic VARCHAR DEFAULT NULL,
		-- Topic like 'Traversal' and subtopic like 'src'
		subtopic VARCHAR DEFAULT NULL,
		queue VARCHAR DEFAULT NULL
	`
}

// Init creates the audit log table if it doesn't exist.
func (t AuditLogTable) Init(db *db.DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}
