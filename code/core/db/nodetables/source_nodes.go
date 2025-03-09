package nodetables

import "github.com/Voltaic314/Data_Migration_Tool/code/core/db"

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
		error_ids TEXT DEFAULT NULL,  -- Store error IDs as a comma-separated list
		FOREIGN KEY (error_ids) REFERENCES node_errors(id) ON DELETE SET NULL
	`
}

// Init creates the source nodes table asynchronously.
func (t SourceNodesTable) Init(db *db.DB) error {
	done := make(chan error)
	go func() {
		done <- db.CreateTable(t.Name(), t.Schema())
	}()
	return <-done
}
