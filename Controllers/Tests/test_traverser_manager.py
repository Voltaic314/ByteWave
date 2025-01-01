import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from Controllers.Queue.task_queue import TaskQueue
from Controllers.Queue.worker import TraverserWorker
from Controllers.traverser_manager import TraverserManager
from Helpers.file_system_trie import FileSystemTrie, TrieNode
from Helpers.path_verifier import PathVerifier
from Helpers.folder import Folder, FolderSubItem


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
def mock_verifier():
    """Fixture for a mock PathVerifier."""
    return AsyncMock(spec=PathVerifier)


@pytest.fixture
def mock_service():
    """Fixture for a mock service."""
    return AsyncMock()


@pytest.fixture
def traverser_manager(mock_queue, mock_file_tree, mock_verifier, mock_service):
    """Fixture for a TraverserManager."""
    return TraverserManager(
        queue=mock_queue,
        file_tree=mock_file_tree,
        verifier=mock_verifier,
        service=mock_service,
        directories=["/root/dir1", "/root/dir2"],
        max_workers=2,
    )


### Tests ###
def test_traverser_manager_initialization(traverser_manager):
    """Test initialization of TraverserManager."""
    assert traverser_manager.queue is not None
    assert traverser_manager.file_tree is not None
    assert traverser_manager.verifier is not None
    assert traverser_manager.service is not None
    assert traverser_manager.directories == ["/root/dir1", "/root/dir2"]
    assert traverser_manager.max_workers == 2
    assert len(traverser_manager.workers) == 0


@pytest.mark.asyncio
async def test_start_adds_tasks_to_queue(traverser_manager, mock_file_tree, mock_queue):
    """Test that start adds pending traversal tasks to the queue."""
    # Mock pending nodes
    mock_folder = FolderSubItem(name="folder", path="/root/folder", identifier="123")
    mock_node = TrieNode(item=Folder(source=mock_folder))
    mock_file_tree.get_nodes_by_status.return_value = [mock_node]

    # Start the TraverserManager
    with patch("Controllers.traverser_manager.TraverserWorker.start", new_callable=AsyncMock):
        await traverser_manager.start()

    # Verify tasks are added to the queue
    mock_queue.add_task.assert_called_once()
    assert len(traverser_manager.workers) == 2


@pytest.mark.asyncio
async def test_start_spawns_workers(traverser_manager):
    """Test that workers are spawned when start is called."""
    with patch("Controllers.traverser_manager.TraverserWorker.start", new_callable=AsyncMock):
        await traverser_manager.start()

    # Verify workers are created
    assert len(traverser_manager.workers) == 2
    for worker in traverser_manager.workers:
        assert isinstance(worker, TraverserWorker)


@pytest.mark.asyncio
async def test_stop_stops_all_workers(traverser_manager):
    """Test that stop stops all workers."""
    # Mock workers
    worker1 = MagicMock(spec=TraverserWorker)
    worker2 = MagicMock(spec=TraverserWorker)
    traverser_manager.workers = [worker1, worker2]

    # Stop the manager
    await traverser_manager.stop()

    # Verify workers are stopped
    worker1.stop.assert_called_once()
    worker2.stop.assert_called_once()
    assert len(traverser_manager.workers) == 0
