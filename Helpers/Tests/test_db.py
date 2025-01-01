import pytest
import aiosqlite
import asyncio
from datetime import datetime
from Helpers.db import DB

@pytest.fixture
async def db():
    # Create a temporary in-memory database for testing
    db_instance = DB(":memory:")
    await db_instance.initialize_db()
    yield db_instance
    # Cleanup
    await asyncio.sleep(0.1)  # Allow any pending tasks to complete

@pytest.mark.asyncio
async def test_setup_tables(db):
    async with aiosqlite.connect(db.db_name) as conn:
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in await cursor.fetchall()}
    assert "nodes" in tables
    assert "resource_monitoring" in tables
    assert "errors" in tables

@pytest.mark.asyncio
async def test_insert_and_fetch_node(db):
    node_data = {
        "parent_id": None,
        "name": "test_node",
        "type": "file",
        "source_identifier": "source1",
        "destination_identifier": "dest1",
        "traversal_status": "pending",
        "upload_status": "pending"
    }
    await db.insert_node(node_data)

    node = await db.fetch_node_by_id(1)
    assert node["name"] == "test_node"
    assert node["traversal_status"] == "pending"

@pytest.mark.asyncio
async def test_update_node_status(db):
    node_data = {
        "parent_id": None,
        "name": "test_node",
        "type": "file",
        "source_identifier": "source1",
        "destination_identifier": "dest1",
        "traversal_status": "pending",
        "upload_status": "pending"
    }
    await db.insert_node(node_data)
    await db.update_node_status(1, traversal_status="completed")

    node = await db.fetch_node_by_id(1)
    assert node["traversal_status"] == "completed"

@pytest.mark.asyncio
async def test_log_and_fetch_error(db):
    error_data = {
        "node_id": 1,
        "error_type": "validation_error",
        "error_message": "Sample error message",
        "error_details": "Sample error details",
        "timestamp": datetime.now().isoformat(),
        "retry_count": 0
    }
    await db.log_error(error_data)

    errors = await db.fetch_errors()
    assert len(errors) == 1
    assert errors[0]["error_message"] == "Sample error message"

@pytest.mark.asyncio
async def test_clear_nodes(db):
    node_data = {
        "parent_id": None,
        "name": "test_node",
        "type": "file",
        "source_identifier": "source1",
        "destination_identifier": "dest1",
        "traversal_status": "pending",
        "upload_status": "pending"
    }
    await db.insert_node(node_data)
    await db.clear_nodes()

    nodes = await db.fetch_nodes_by_status("pending", "traversal")
    assert len(nodes) == 0

@pytest.mark.asyncio
async def test_resource_monitoring(db):
    resource_data = [50.5, 60.7, 70.8]
    await db.log_resource_data("CPU", resource_data)

    fetched_data = await db.fetch_resource_data("CPU")
    assert len(fetched_data) == len(resource_data)
    assert fetched_data[0]["usage"] == 50.5

    await db.clear_resource_data("CPU")
    cleared_data = await db.fetch_resource_data("CPU")
    assert len(cleared_data) == 0
