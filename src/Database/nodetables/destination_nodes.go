package nodetables

import "github.com/Voltaic314/Data_Migration_Tool/database"

type DestinationNodesTable struct{}

func (t DestinationNodesTable) Name() string {
	return "destination_nodes"
}

func (t DestinationNodesTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
		path TEXT NOT NULL,
		identifier TEXT,
		size INTEGER,
		last_modified TEXT,
		exists BOOLEAN NOT NULL DEFAULT 0,
		traversal_status TEXT NOT NULL CHECK(traversal_status IN ('pending', 'successful', 'failed')),
		upload_status TEXT NOT NULL CHECK(upload_status IN ('pending', 'successful', 'failed')),
		traversal_attempts INTEGER DEFAULT 0,
		upload_attempts INTEGER DEFAULT 0,
		error_id INTEGER DEFAULT NULL,
		FOREIGN KEY (error_id) REFERENCES node_errors(id) ON DELETE SET NULL
	`
}

func (t DestinationNodesTable) Init(db *database.DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}
