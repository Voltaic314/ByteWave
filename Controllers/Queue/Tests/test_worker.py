import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4
from Controllers.Queue.worker import TraverserWorker, UploaderWorker
from Controllers.Queue.task_queue import TaskQueue
from Controllers.Queue.task import Task
from Helpers.file_system_trie import FileSystemTrie, TrieNode
from Helpers.folder import Folder, FolderSubItem
from Helpers.file import File, FileSubItem


@pytest.fixture
def mock_queue():
    """Fixture for a mock TaskQueue."""
    return AsyncMock(spec=TaskQueue)


@pytest.fixture
def mock_trie():
    """Fixture for a mock FileSystemTrie."""
    trie = AsyncMock(spec=FileSystemTrie)
    trie.node_map = {}
    return trie


@pytest.fixture
def mock_service():
    """Fixture for a mock service."""
    return AsyncMock()


@pytest.fixture
def traverser_worker(mock_queue):
    """Fixture for a TraverserWorker."""
    return TraverserWorker(id=1, queue=mock_queue)


@pytest.fixture
def uploader_worker(mock_queue):
    """Fixture for an UploaderWorker."""
    return UploaderWorker(id=2, queue=mock_queue)


### TraverserWorker Tests ###
@pytest.mark.asyncio
async def test_traverser_worker_process_task_invalid_payload(traverser_worker):
    """Test TraverserWorker with invalid task payload."""
    task = Task(id="task1", type="traverse", payload={})
    task.mark_failed = AsyncMock()

    await traverser_worker.process_task(task)
    task.mark_failed.assert_called_once()


@pytest.mark.asyncio
async def test_traverser_worker_process_task_success(traverser_worker, mock_trie, mock_service):
    """Test TraverserWorker processes a traversal task successfully."""
    folder_sub_item = FolderSubItem(name="folder", path="/folder", identifier="123")
    folder_node = TrieNode(item=Folder(source=folder_sub_item))
    mock_trie.node_map["123"] = folder_node

    mock_service.get_all_items.return_value = [FolderSubItem(name="sub_folder", path="/folder/sub_folder", identifier="456")]

    task = Task(
        id="task1",
        type="traverse",
        payload={"node_id": "123", "trie": mock_trie, "service": mock_service},
    )
    task.mark_completed = AsyncMock()

    await traverser_worker.process_task(task)
    mock_service.get_all_items.assert_called_once()
    task.mark_completed.assert_called_once()


@pytest.mark.asyncio
async def test_traverser_worker_process_task_no_items(traverser_worker, mock_trie, mock_service):
    """Test TraverserWorker with a folder containing no items."""
    folder_sub_item = FolderSubItem(name="folder", path="/folder", identifier="123")
    folder_node = TrieNode(item=Folder(source=folder_sub_item))
    mock_trie.node_map["123"] = folder_node

    mock_service.get_all_items.return_value = []

    task = Task(
        id="task1",
        type="traverse",
        payload={"node_id": "123", "trie": mock_trie, "service": mock_service},
    )
    task.mark_failed = AsyncMock()

    await traverser_worker.process_task(task)
    mock_service.get_all_items.assert_called_once()
    task.mark_failed.assert_called_once()


### UploaderWorker Tests ###
@pytest.mark.asyncio
async def test_uploader_worker_process_task_invalid_payload(uploader_worker):
    """Test UploaderWorker with invalid task payload."""
    task = Task(id="task1", type="upload", payload={})
    task.mark_failed = AsyncMock()

    await uploader_worker.process_task(task)
    task.mark_failed.assert_called_once()


@pytest.mark.asyncio
async def test_uploader_worker_process_folder_success(uploader_worker, mock_trie, mock_service):
    """Test UploaderWorker uploads a folder successfully."""
    folder_sub_item = FolderSubItem(name="folder", path="/folder", identifier="123")
    folder_node = TrieNode(item=Folder(source=folder_sub_item))
    folder_node.parent = TrieNode(item=Folder(destination=folder_sub_item))
    mock_trie.node_map["123"] = folder_node

    mock_service.create_folder.return_value = "new_folder_id"

    task = Task(
        id="task1",
        type="upload",
        payload={"node_id": "123", "trie": mock_trie, "service": mock_service},
    )
    task.mark_completed = AsyncMock()

    await uploader_worker.process_task(task)
    mock_service.create_folder.assert_called_once()
    task.mark_completed.assert_called_once()


@pytest.mark.asyncio
async def test_uploader_worker_process_file_success(uploader_worker, mock_trie, mock_service):
    """Test UploaderWorker uploads a file successfully."""
    file_sub_item = FileSubItem(name="file.txt", path="/folder/file.txt", identifier="123")
    file_node = TrieNode(item=File(source=file_sub_item))
    file_node.parent = TrieNode(item=Folder(destination=FolderSubItem(identifier="parent_id")))
    mock_trie.node_map["123"] = file_node

    mock_service.upload_file = AsyncMock()

    task = Task(
        id="task1",
        type="upload",
        payload={"node_id": "123", "trie": mock_trie, "service": mock_service},
    )
    task.mark_completed = AsyncMock()

    await uploader_worker.process_task(task)
    mock_service.upload_file.assert_called_once()
    task.mark_completed.assert_called_once()
