package nodetables

import "github.com/Voltaic314/Data_Migration_Tool/db"

type SourceNodesTable struct{}

func (t SourceNodesTable) Name() string {
	return "source_nodes"
}

func (t SourceNodesTable) Schema() string {
	return `
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		path TEXT NOT NULL UNIQUE,
		identifier TEXT,  -- NULLable for filesystems that don't provide IDs
		type TEXT NOT NULL CHECK(type IN ('file', 'folder')),
		level INTEGER NOT NULL,
		size INTEGER,
		last_modified TEXT NOT NULL,
		traversal_status TEXT NOT NULL CHECK(traversal_status IN ('pending', 'successful', 'failed')),
		upload_status TEXT NOT NULL CHECK(upload_status IN ('pending', 'successful', 'failed')),
		traversal_attempts INTEGER DEFAULT 0,
		upload_attempts INTEGER DEFAULT 0,
		error_id INTEGER DEFAULT NULL,
		FOREIGN KEY (error_id) REFERENCES node_errors(id) ON DELETE SET NULL
	`
}

func (t SourceNodesTable) Init(db *db.DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}
