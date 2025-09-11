package tables

import "github.com/Voltaic314/ByteWave/tests/db"

type OracleSrcNodesTable struct{}

func (t OracleSrcNodesTable) Name() string {
	return "source_nodes"
}

func (t OracleSrcNodesTable) Schema() string {
	return `
		id VARCHAR NOT NULL PRIMARY KEY,
		parent_id VARCHAR NOT NULL,
		name VARCHAR NOT NULL,
		path VARCHAR NOT NULL,
		type VARCHAR NOT NULL CHECK(type IN ('file', 'folder')),
		size BIGINT,
		level INTEGER NOT NULL,
		checked BOOLEAN NOT NULL DEFAULT FALSE
	`
}

// Init creates the source_nodes table asynchronously.
func (t OracleSrcNodesTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
