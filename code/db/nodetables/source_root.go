package nodetables

import "github.com/Voltaic314/ByteWave/code/db"

type SourceRootTable struct{}

func (t SourceRootTable) Name() string {
	return "source_root"
}

func (t SourceRootTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY DEFAULT 1,
		path VARCHAR NOT NULL,
		name VARCHAR NOT NULL,
		identifier VARCHAR NOT NULL,
		last_modified TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	`
}

// Init creates the source_root table asynchronously.
func (t SourceRootTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
