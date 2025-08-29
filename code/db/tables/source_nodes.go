package tables

import "github.com/Voltaic314/ByteWave/code/db"

type SourceNodesTable struct{}

func (t SourceNodesTable) Name() string {
	return "source_nodes"
}

func (t SourceNodesTable) Schema() string {
	return `
		path VARCHAR NOT NULL UNIQUE,
		name VARCHAR NOT NULL,
		identifier VARCHAR NOT NULL,
		parent_id VARCHAR NOT NULL,
		type VARCHAR NOT NULL CHECK(type IN ('file', 'folder', 'drive')),
		level INTEGER NOT NULL,
		size BIGINT,
		last_modified TIMESTAMP NOT NULL,
		traversal_status VARCHAR NOT NULL CHECK(traversal_status IN ('pending', 'queued', 'successful', 'failed')),
		upload_status VARCHAR NOT NULL CHECK(upload_status IN ('pending', 'queued', 'successful', 'failed')),
		traversal_attempts INTEGER DEFAULT 0,
		upload_attempts INTEGER DEFAULT 0,
		error_ids VARCHAR DEFAULT NULL
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
