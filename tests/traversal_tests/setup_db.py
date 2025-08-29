# ByteWave Test Database Setup
# Sets up both source_nodes and destination_nodes tables for traversal testing
import os
import duckdb
from datetime import datetime

# Paths - dynamically built from current working directory
base_dir = os.getcwd()
db_path = os.path.join(base_dir, "tests", "traversal_tests", "test_traversal.db")

# Source and destination folder paths (relative to base_dir)
src_folder_rel_path = os.path.join("tests", "traversal_tests", "starting_src_folder")
dst_folder_rel_path = os.path.join("tests", "traversal_tests", "starting_dst_folder")

# Build absolute paths
src_folder_abs_path = os.path.join(base_dir, src_folder_rel_path)
dst_folder_abs_path = os.path.join(base_dir, dst_folder_rel_path)

print(f"📁 Base directory: {base_dir}")
print(f"📁 Source folder: {src_folder_abs_path}")
print(f"📁 Destination folder: {dst_folder_abs_path}")

# If the db file path exists, delete it first
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"🗑️ Deleted existing database file: {db_path}")

# Create test folders if they don't exist
os.makedirs(src_folder_abs_path, exist_ok=True)
os.makedirs(dst_folder_abs_path, exist_ok=True)


# Connect to DuckDB
conn = duckdb.connect(database=db_path)

# Create simple level 0 root entries (no hierarchy, no negative levels)
src_root_entry = {
    "path": "/",  # Root path is "/" for clear identification
    "name": os.path.basename(src_folder_abs_path),
    "identifier": src_folder_abs_path,  # Absolute path for OS service
    "parent_id": "",  # Root has no parent
    "type": "folder",
    "level": 0,
    "size": 0,
    "last_modified": "2025-01-01T00:00:00Z",
    "traversal_status": "pending",
    "upload_status": "pending",
    "traversal_attempts": 0,
    "upload_attempts": 0,
    "error_ids": None
}

dst_root_entry = {
    "path": "/",  # Root path is "/" for clear identification
    "name": os.path.basename(dst_folder_abs_path),
    "identifier": dst_folder_abs_path,  # Absolute path for OS service
    "parent_id": "",  # Root has no parent
    "type": "folder",
    "level": 0,
    "size": 0,
    "last_modified": "2025-01-01T00:00:00Z",
    "traversal_status": "pending",
    "traversal_attempts": 0,
    "error_ids": None
}

print(f"📁 Source root: {src_root_entry['identifier']}")
print(f"📁 Destination root: {dst_root_entry['identifier']}")

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

# Create audit_log table (updated schema)
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
    message VARCHAR NOT NULL,
    action VARCHAR DEFAULT NULL,
    -- Action like 'CREATE_WORKER' or 'INSERT_TASKS' capital snake case style 
    topic VARCHAR DEFAULT NULL,
    -- Topic like 'Traversal' and subtopic like 'src'
    subtopic VARCHAR DEFAULT NULL
);
"""
conn.execute(audit_log_schema)

# Insert level 0 root entries directly into nodes tables
print("📥 Inserting root data into nodes tables...")

# Insert source root entry
src_insert_query = """
INSERT OR IGNORE INTO source_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, upload_status,
    traversal_attempts, upload_attempts, error_ids
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

conn.execute(src_insert_query, (
    src_root_entry["path"],
    src_root_entry["name"],
    src_root_entry["identifier"],
    src_root_entry["parent_id"],
    src_root_entry["type"],
    src_root_entry["level"],
    src_root_entry["size"],
    datetime.strptime(src_root_entry["last_modified"], "%Y-%m-%dT%H:%M:%SZ"),
    src_root_entry["traversal_status"],
    src_root_entry["upload_status"],
    src_root_entry["traversal_attempts"],
    src_root_entry["upload_attempts"],
    src_root_entry["error_ids"]
))

# Insert destination root entry
dst_insert_query = """
INSERT OR IGNORE INTO destination_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, traversal_attempts, error_ids
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

conn.execute(dst_insert_query, (
    dst_root_entry["path"],
    dst_root_entry["name"],
    dst_root_entry["identifier"],
    dst_root_entry["parent_id"],
    dst_root_entry["type"],
    dst_root_entry["level"],
    dst_root_entry["size"],
    datetime.strptime(dst_root_entry["last_modified"], "%Y-%m-%dT%H:%M:%SZ"),
    dst_root_entry["traversal_status"],
    dst_root_entry["traversal_attempts"],
    dst_root_entry["error_ids"]
))

conn.close()
print("✅ Tables created: source_nodes, destination_nodes, audit_log, src_nodes_errors, dst_nodes_errors")
print("🎉 Test data inserted into DuckDB!")
