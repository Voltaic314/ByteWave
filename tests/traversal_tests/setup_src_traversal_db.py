import os
import json
import duckdb
from datetime import datetime

# Paths
json_path = r"C:\Users\golde\OneDrive\Documents\GitHub\ByteWave\tests\traversal_tests\test_src_path.json"
db_path = r"C:\Users\golde\OneDrive\Documents\GitHub\ByteWave\tests\traversal_tests\test_src_traversal.db"

# If the db file path exists, delete it first
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"🗑️ Deleted existing database file: {db_path}")

# Load and augment path data from JSON
with open(json_path, "r") as f:
    path_data = json.load(f)


# Connect to DuckDB
conn = duckdb.connect(database=db_path)

# Create source_nodes table (with parent_id and identifier as NOT NULL)
source_nodes_schema = """
CREATE TABLE IF NOT EXISTS source_nodes (
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
);
"""
conn.execute(source_nodes_schema)

# Create audit_log table (same as before)
audit_log_schema = """
CREATE TABLE IF NOT EXISTS audit_log (
    id VARCHAR PRIMARY KEY, 
    -- Use a UUID for the primary key
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR NOT NULL CHECK(level IN ('trace', 'debug', 'info', 'warning', 'error', 'critical')),
    details JSON DEFAULT NULL,
    message VARCHAR NOT NULL
);
"""
conn.execute(audit_log_schema)

# Insert source_nodes records
insert_query = """
INSERT INTO source_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, upload_status,
    traversal_attempts, upload_attempts, error_ids
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for item in path_data:
    conn.execute(insert_query, (
        item["path"],
        item["name"],
        item["identifier"],
        item["parent_id"],
        item["type"],
        item["level"],
        item.get("size"),
        datetime.strptime(item["last_modified"], "%Y-%m-%dT%H:%M:%SZ"),
        item["traversal_status"],
        item["upload_status"],
        item.get("traversal_attempts", 0),
        item.get("upload_attempts", 0),
        item.get("error_ids")
    ))

conn.close()
print("✅ source_nodes and audit_log tables created.")
print("🎉 Test data inserted into DuckDB!")
