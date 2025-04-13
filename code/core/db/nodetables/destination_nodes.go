package nodetables

import "github.com/Voltaic314/ByteWave/code/core/db"

type DestinationNodesTable struct{}

func (t DestinationNodesTable) Name() string {
	return "destination_nodes"
}

func (t DestinationNodesTable) Schema() string {
	return `
		path VARCHAR NOT NULL UNIQUE,
		identifier VARCHAR,  -- NULLable for filesystems that don't provide IDs
		type VARCHAR NOT NULL CHECK(type IN ('file', 'folder')),
		level INTEGER NOT NULL,
		size BIGINT,
		last_modified TIMESTAMP,
		traversal_status VARCHAR NOT NULL CHECK(traversal_status IN ('pending', 'successful', 'failed')),
		traversal_attempts INTEGER DEFAULT 0,
		error_ids VARCHAR DEFAULT NULL,
		FOREIGN KEY (error_ids) REFERENCES node_errors(id) ON DELETE SET NULL
	`
}

// Init creates the destination_nodes table asynchronously.
func (t DestinationNodesTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
