package tables

import "github.com/Voltaic314/ByteWave/tests/db"

type OracleDstNodesTable struct{}

func (t OracleDstNodesTable) Name() string {
	return "destination_nodes"
}

func (t OracleDstNodesTable) Schema() string {
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

// Init creates the destination_nodes table asynchronously.
func (t OracleDstNodesTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
