import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from Controllers.Queue.task_queue import TaskQueue
from Controllers.Queue.worker import UploaderWorker
from Controllers.uploader_manager import UploaderManager
from Helpers.file_system_trie import FileSystemTrie, TrieNode
from Helpers.folder import Folder, FolderSubItem
from Helpers.file import File, FileSubItem


@pytest.fixture
def mock_queue():
    """Fixture for a mock TaskQueue."""
    return AsyncMock(spec=TaskQueue)


@pytest.fixture
def mock_file_tree():
    """Fixture for a mock FileSystemTrie."""
    trie = AsyncMock(spec=FileSystemTrie)
    trie.get_nodes_by_status = AsyncMock(return_value=[])
    return trie


@pytest.fixture
def mock_service():
    """Fixture for a mock service."""
    return AsyncMock()


@pytest.fixture
def uploader_manager(mock_queue, mock_file_tree, mock_service):
    """Fixture for an UploaderManager."""
    return UploaderManager(
        queue=mock_queue,
        file_tree=mock_file_tree,
        service=mock_service,
        max_workers=2,
    )


### Tests ###
def test_uploader_manager_initialization(uploader_manager):
    """Test initialization of UploaderManager."""
    assert uploader_manager.queue is not None
    assert uploader_manager.file_tree is not None
    assert uploader_manager.service is not None
    assert uploader_manager.max_workers == 2
    assert len(uploader_manager.workers) == 0


@pytest.mark.asyncio
async def test_start_adds_tasks_to_queue(uploader_manager, mock_file_tree, mock_queue):
    """Test that start adds pending upload tasks to the queue."""
    # Mock pending nodes
    mock_file = FileSubItem(name="file.txt", path="/folder/file.txt", identifier="123")
    mock_node = TrieNode(item=File(source=mock_file))
    mock_file_tree.get_nodes_by_status.return_value = [mock_node]

    # Start the UploaderManager
    with patch("Controllers.uploader_manager.UploaderWorker.start", new_callable=AsyncMock):
        await uploader_manager.start()

    # Verify tasks are added to the queue
    mock_queue.add_task.assert_called_once()
    assert len(uploader_manager.workers) == 2


@pytest.mark.asyncio
async def test_start_spawns_workers(uploader_manager):
    """Test that workers are spawned when start is called."""
    with patch("Controllers.uploader_manager.UploaderWorker.start", new_callable=AsyncMock):
        await uploader_manager.start()

    # Verify workers are created
    assert len(uploader_manager.workers) == 2
    for worker in uploader_manager.workers:
        assert isinstance(worker, UploaderWorker)


@pytest.mark.asyncio
async def test_stop_stops_all_workers(uploader_manager):
    """Test that stop stops all workers."""
    # Mock workers
    worker1 = MagicMock(spec=UploaderWorker)
    worker2 = MagicMock(spec=UploaderWorker)
    uploader_manager.workers = [worker1, worker2]

    # Stop the manager
    await uploader_manager.stop()

    # Verify workers are stopped
    worker1.stop.assert_called_once()
    worker2.stop.assert_called_once()
    assert len(uploader_manager.workers) == 0
