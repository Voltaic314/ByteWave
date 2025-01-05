import pytest
from unittest.mock import AsyncMock
from Helpers.file import File, FileSubItem
from Helpers.folder import Folder, FolderSubItem
from Helpers.db import DB
from Helpers.file_system_trie import TrieNode, FileSystemTrie


@pytest.fixture
def mock_db():
    """Mock the database instance."""
    db = AsyncMock(DB(":memory:"))
    return db


@pytest.fixture
def trie(mock_db):
    """Fixture for the FileSystemTrie instance."""
    return FileSystemTrie(db=mock_db, max_traversal_retries=3, max_upload_retries=3, flush_threshold=2)


@pytest.fixture
def folder():
    """Fixture for a sample Folder object."""
    source = FolderSubItem(name="test_folder", identifier="folder1", path="/test_folder", parent_id=None)
    return Folder(source=source)


@pytest.fixture
def file():
    """Fixture for a sample File object."""
    source = FileSubItem(name="test_file", identifier="file1", path="/test_folder/test_file", parent_id="folder1")
    return File(source=source)


### TrieNode Tests ###
def test_trie_node_creation(folder):
    node = TrieNode(source=folder)
    assert node.source == folder
    assert node.traversal_status == "pending"
    assert node.upload_status == "pending"
    assert node.traversal_attempts == 0
    assert node.upload_attempts == 0


def test_add_child(folder, file):
    parent_node = TrieNode(source=folder)
    child_node = parent_node.add_child(child_source=file)
    assert child_node.source == file
    assert child_node.parent == parent_node
    assert file.source.name in parent_node.children


def test_update_status(folder):
    node = TrieNode(source=folder)
    node.update_traversal_status("successful")
    node.update_upload_status("failed")
    assert node.traversal_status == "successful"
    assert node.upload_status == "failed"


def test_increment_attempts(folder):
    node = TrieNode(source=folder)
    node.increment_traversal_attempts()
    node.increment_upload_attempts()
    assert node.traversal_attempts == 1
    assert node.upload_attempts == 1


### FileSystemTrie Tests ###
@pytest.mark.asyncio
async def test_trie_initialization(trie):
    assert trie.root.source.source.name == "root"
    assert trie.root.source.source.identifier == "root"
    assert len(trie.node_map) == 1  # Only root node initially


@pytest.mark.asyncio
async def test_add_item(trie, file, folder):
    # Add a folder to the trie
    folder_id = await trie.add_item(folder, parent_id="root")
    assert folder_id in trie.node_map
    assert trie.node_map[folder_id].source == folder

    # Add a file to the folder
    file_id = await trie.add_item(file, parent_id=folder_id)
    assert file_id in trie.node_map
    assert trie.node_map[file_id].source == file
    assert file.source.name in trie.node_map[folder_id].children


@pytest.mark.asyncio
async def test_update_node_status(trie, folder):
    folder_id = await trie.add_item(folder, parent_id="root")

    # Update traversal status
    await trie.update_node_status(folder_id, "traversal", "successful")
    assert trie.node_map[folder_id].traversal_status == "successful"

    # Update upload status
    await trie.update_node_status(folder_id, "upload", "failed")
    assert trie.node_map[folder_id].upload_status == "failed"


@pytest.mark.asyncio
async def test_update_node_attempts(trie, folder):
    folder_id = await trie.add_item(folder, parent_id="root")

    # Increment traversal attempts
    await trie.update_node_attempts(folder_id, "traversal")
    assert trie.node_map[folder_id].traversal_attempts == 1

    # Increment upload attempts
    await trie.update_node_attempts(folder_id, "upload")
    assert trie.node_map[folder_id].upload_attempts == 1


@pytest.mark.asyncio
async def test_flush_updates(trie, folder, file):
    folder_id = await trie.add_item(folder, parent_id="root")
    file_id = await trie.add_item(file, parent_id=folder_id)

    # Flush and check if mock DB methods were called
    await trie.flush_updates()
    assert trie.db.insert_node.called
    assert trie.db.update_nodes_status.called
    assert trie.db.update_nodes_attempts.called


@pytest.mark.asyncio
async def test_get_nodes_by_status(trie, mock_db):
    mock_db.fetch_nodes_by_status.return_value = [
        {"id": "node1", "traversal_status": "pending"},
        {"id": "node2", "traversal_status": "pending"},
    ]
    nodes = await trie.get_nodes_by_status("pending", "traversal")
    assert len(nodes) == 2
    assert nodes[0]["id"] == "node1"


@pytest.mark.asyncio
async def test_clear_trie(trie):
    # Add some nodes to the trie
    folder_source = FolderSubItem(name="folder1", identifier="folder1", path="/folder1")
    file_source = FileSubItem(name="file1", identifier="file1", path="/folder1/file1")

    await trie.add_item(Folder(source=folder_source), parent_id="root")
    await trie.add_item(File(source=file_source), parent_id="folder1")

    assert len(trie.node_map) > 1  # Nodes were added
    trie.clear()
    assert len(trie.node_map) == 1  # Only root node remains
    assert trie.root.source.source.name == "root"
