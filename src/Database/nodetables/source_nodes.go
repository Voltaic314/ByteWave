package nodetables

import "github.com/Voltaic314/Data_Migration_Tool/database"

type SourceNodesTable struct{}

func (t SourceNodesTable) Name() string {
	return "source_nodes"
}

func (t SourceNodesTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
		path TEXT NOT NULL,
		identifier TEXT NOT NULL,
		size INTEGER,
		last_modified TEXT NOT NULL,
		traversal_status TEXT NOT NULL CHECK(traversal_status IN ('pending', 'successful', 'failed')),
		download_status TEXT NOT NULL CHECK(download_status IN ('pending', 'successful', 'failed')),
		traversal_attempts INTEGER DEFAULT 0,
		download_attempts INTEGER DEFAULT 0,
		error_id INTEGER DEFAULT NULL,
		FOREIGN KEY (error_id) REFERENCES node_errors(id) ON DELETE SET NULL
	`
}

func (t SourceNodesTable) Init(db *database.DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}
