# ByteWave Test Database Setup
# Sets up both source_nodes and destination_nodes tables for traversal testing
import os
import json
import duckdb
from datetime import datetime

# Paths
src_json_path = r"C:\Users\golde\OneDrive\Documents\GitHub\ByteWave\tests\traversal_tests\test_src_path.json"
dst_json_path = r"C:\Users\golde\OneDrive\Documents\GitHub\ByteWave\tests\traversal_tests\test_dst_path.json"
db_path = r"C:\Users\golde\OneDrive\Documents\GitHub\ByteWave\tests\traversal_tests\test_traversal.db"

# If the db file path exists, delete it first
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"🗑️ Deleted existing database file: {db_path}")

# Load source path data from JSON
with open(src_json_path, "r") as f:
    src_path_data = json.load(f)

# Load destination path data from JSON
with open(dst_json_path, "r") as f:
    dst_path_data = json.load(f)


# Connect to DuckDB
conn = duckdb.connect(database=db_path)

# Create error tables for future error tracking
src_errors_schema = """
CREATE TABLE IF NOT EXISTS src_nodes_errors (
    id VARCHAR PRIMARY KEY
);
"""
conn.execute(src_errors_schema)

dst_errors_schema = """
CREATE TABLE IF NOT EXISTS dst_nodes_errors (
    id VARCHAR PRIMARY KEY
);
"""
conn.execute(dst_errors_schema)

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

# Create destination_nodes table (matches source_nodes pattern, no foreign key for now)
destination_nodes_schema = """
CREATE TABLE IF NOT EXISTS destination_nodes (
    path VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    identifier VARCHAR NOT NULL,
    parent_id VARCHAR NOT NULL,
    type VARCHAR NOT NULL CHECK(type IN ('file', 'folder')),
    level INTEGER NOT NULL,
    size BIGINT,
    last_modified TIMESTAMP,
    traversal_status VARCHAR NOT NULL CHECK(traversal_status IN ('pending', 'queued', 'successful', 'failed')),
    traversal_attempts INTEGER DEFAULT 0,
    error_ids VARCHAR DEFAULT NULL
);
"""
conn.execute(destination_nodes_schema)

# Create audit_log table (same as before)
audit_log_schema = """
CREATE TABLE IF NOT EXISTS audit_log (
    id VARCHAR PRIMARY KEY, 
    -- Use a UUID for the primary key
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR NOT NULL CHECK(level IN ('trace', 'debug', 'info', 'warning', 'error', 'critical')),
    entity VARCHAR DEFAULT NULL,
    -- Entity type: 'worker', 'user', 'system', 'QP', 'Conductor', 'API', 'cloud storage service', etc.
    entity_id VARCHAR DEFAULT NULL,
    -- Unique identifier for the entity (worker ID, service ID, etc.)
    path VARCHAR DEFAULT NULL,
    -- Optional path for task-related logs
    details VARCHAR DEFAULT NULL,
    message VARCHAR NOT NULL
);
"""
conn.execute(audit_log_schema)

# Insert source_nodes records
src_insert_query = """
INSERT OR IGNORE INTO source_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, upload_status,
    traversal_attempts, upload_attempts, error_ids
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

print("📥 Inserting source node data...")
for item in src_path_data:
    conn.execute(src_insert_query, (
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

# Insert destination_nodes records (note: no upload_status field in destination_nodes)
dst_insert_query = """
INSERT OR IGNORE INTO destination_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, traversal_attempts, error_ids
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

print("📥 Inserting destination node data...")
for item in dst_path_data:
    conn.execute(dst_insert_query, (
        item["path"],
        item["name"],
        item["identifier"],
        item["parent_id"],
        item["type"],
        item["level"],
        item.get("size"),
        datetime.strptime(item["last_modified"], "%Y-%m-%dT%H:%M:%SZ"),
        item["traversal_status"],
        item.get("traversal_attempts", 0),
        item.get("error_ids")
    ))

conn.close()
print("✅ Tables created: source_nodes, destination_nodes, audit_log, src_nodes_errors, dst_nodes_errors")
print(f"🎉 Test data inserted into DuckDB! ({len(src_path_data)} source nodes, {len(dst_path_data)} destination nodes)")
