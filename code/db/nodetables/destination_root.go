package nodetables

import "github.com/Voltaic314/ByteWave/code/db"

type DestinationRootTable struct{}

func (t DestinationRootTable) Name() string {
	return "destination_root"
}

func (t DestinationRootTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY DEFAULT 1,
		path VARCHAR NOT NULL,
		name VARCHAR NOT NULL,
		identifier VARCHAR NOT NULL,
		last_modified TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	`
}

// Init creates the destination_root table asynchronously.
func (t DestinationRootTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
