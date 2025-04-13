package nodetables

import "github.com/Voltaic314/ByteWave/code/core/db"

type SourceNodesTable struct{}

func (t SourceNodesTable) Name() string {
	return "source_nodes"
}

func (t SourceNodesTable) Schema() string {
	return `
		path VARCHAR NOT NULL UNIQUE,
		identifier VARCHAR,  -- NULLable for filesystems that don't provide IDs
		type VARCHAR NOT NULL CHECK(type IN ('file', 'folder')),
		level INTEGER NOT NULL,
		size BIGINT,
		last_modified TIMESTAMP NOT NULL,
		traversal_status VARCHAR NOT NULL CHECK(traversal_status IN ('pending', 'successful', 'failed')),
		upload_status VARCHAR NOT NULL CHECK(upload_status IN ('pending', 'successful', 'failed')),
		traversal_attempts INTEGER DEFAULT 0,
		upload_attempts INTEGER DEFAULT 0,
		error_ids VARCHAR DEFAULT NULL,
		FOREIGN KEY (error_ids) REFERENCES node_errors(id) ON DELETE SET NULL
	`
}

// Init creates the source_nodes table asynchronously.
func (t SourceNodesTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
