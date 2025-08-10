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

# Extract root information from the JSON data
def extract_root_info(path_data):
    # Find the level 0 entry (the root)
    for item in path_data:
        if item["level"] == 0:
            return {
                "path": item["path"],
                "name": item["name"], 
                "identifier": item["identifier"],
                "last_modified": item["last_modified"]
            }
    return None

src_root_info = extract_root_info(src_path_data)
dst_root_info = extract_root_info(dst_path_data)

print(f"📁 Source root: {src_root_info['path']}")
print(f"📁 Destination root: {dst_root_info['path']}")

# Function to convert absolute path to relative path
def make_relative_path(absolute_path, root_path):
    if absolute_path == root_path:
        return "/"
    elif absolute_path.startswith(root_path):
        relative = absolute_path[len(root_path):]
        # Ensure it starts with /
        if not relative.startswith("/"):
            relative = "/" + relative
        return relative
    else:
        return absolute_path  # fallback

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

# Create source_root table
source_root_schema = """
CREATE TABLE IF NOT EXISTS source_root (
    id INTEGER PRIMARY KEY DEFAULT 1,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    identifier VARCHAR NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""
conn.execute(source_root_schema)

# Create destination_root table
destination_root_schema = """
CREATE TABLE IF NOT EXISTS destination_root (
    id INTEGER PRIMARY KEY DEFAULT 1,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    identifier VARCHAR NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""
conn.execute(destination_root_schema)

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

# Insert root records first
print("📥 Inserting root data...")

# Insert source root
src_root_insert = """
INSERT OR IGNORE INTO source_root (path, name, identifier, last_modified)
VALUES (?, ?, ?, ?)
"""
conn.execute(src_root_insert, (
    src_root_info["path"],
    src_root_info["name"],
    src_root_info["identifier"],
    datetime.strptime(src_root_info["last_modified"], "%Y-%m-%dT%H:%M:%SZ")
))

# Insert destination root  
dst_root_insert = """
INSERT OR IGNORE INTO destination_root (path, name, identifier, last_modified)
VALUES (?, ?, ?, ?)
"""
conn.execute(dst_root_insert, (
    dst_root_info["path"],
    dst_root_info["name"],
    dst_root_info["identifier"],
    datetime.strptime(dst_root_info["last_modified"], "%Y-%m-%dT%H:%M:%SZ")
))

# Insert source_nodes records (excluding level 0 root, convert paths to relative)
src_insert_query = """
INSERT OR IGNORE INTO source_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, upload_status,
    traversal_attempts, upload_attempts, error_ids
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

print("📥 Inserting source node data...")
for item in src_path_data:
    # Skip the root (level 0) as it's now in the source_root table
    if item["level"] == 0:
        continue
        
    # Convert paths to relative format
    relative_path = make_relative_path(item["path"], src_root_info["path"])
    relative_parent_id = make_relative_path(item["parent_id"], src_root_info["path"]) if item["parent_id"] else "/"
    
    conn.execute(src_insert_query, (
        relative_path,
        item["name"],
        item["identifier"],  # Keep identifier as absolute for OS service
        relative_parent_id,
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
    # Skip the root (level 0) as it's now in the destination_root table
    if item["level"] == 0:
        continue
        
    # Convert paths to relative format
    relative_path = make_relative_path(item["path"], dst_root_info["path"])
    relative_parent_id = make_relative_path(item["parent_id"], dst_root_info["path"]) if item["parent_id"] else "/"
    
    conn.execute(dst_insert_query, (
        relative_path,
        item["name"],
        item["identifier"],  # Keep identifier as absolute for OS service
        relative_parent_id,
        item["type"],
        item["level"],
        item.get("size"),
        datetime.strptime(item["last_modified"], "%Y-%m-%dT%H:%M:%SZ"),
        item["traversal_status"],
        item.get("traversal_attempts", 0),
        item.get("error_ids")
    ))

conn.close()
print("✅ Tables created: source_root, destination_root, source_nodes, destination_nodes, audit_log, src_nodes_errors, dst_nodes_errors")
print(f"🎉 Test data inserted into DuckDB!")
print(f"   📁 Root tables: 2 entries (source + destination)")
print(f"   📂 Node tables: {len([item for item in src_path_data if item['level'] != 0])} source nodes, {len([item for item in dst_path_data if item['level'] != 0])} destination nodes")
