package nodetables

import "github.com/Voltaic314/Data_Migration_Tool/db"

type Table interface {
	Name() string
	Schema() string
	Init(db *db.DB) error
}

// NodesTable defines the schema for the "nodes" table.
type NodesTable struct{}

func (t NodesTable) Name() string {
	return "nodes"
}

func (t NodesTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		parent_id INTEGER,
		name TEXT NOT NULL,
		type TEXT NOT NULL CHECK(type IN ('file', 'folder')),
		level INTEGER NOT NULL,
		last_updated TEXT NOT NULL,
		FOREIGN KEY (parent_id) REFERENCES nodes(id) ON DELETE CASCADE
	`
}

func (t NodesTable) Init(db *db.DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}
