import pytest
from unittest.mock import AsyncMock, MagicMock
from Controllers.migrator import Migrator


@pytest.fixture
def mock_traverser_manager():
    """Fixture for a mock TraverserManager."""
    manager = AsyncMock()
    manager.workers = [MagicMock(), MagicMock()]  # Simulate active workers
    return manager


@pytest.fixture
def mock_uploader_manager():
    """Fixture for a mock UploaderManager."""
    manager = AsyncMock()
    manager.workers = [MagicMock()]  # Simulate active workers
    return manager


@pytest.fixture
def migrator(mock_traverser_manager, mock_uploader_manager):
    """Fixture for a Migrator instance."""
    return Migrator(
        traverser_manager=mock_traverser_manager,
        uploader_manager=mock_uploader_manager,
    )


### Tests ###
def test_migrator_initialization(migrator):
    """Test Migrator initialization."""
    assert migrator.state == "idle"
    assert migrator.mode is None
    assert migrator.current_task is None


@pytest.mark.asyncio
async def test_start_traversal(migrator, mock_traverser_manager):
    """Test starting the traversal process."""
    await migrator.start_traversal()

    mock_traverser_manager.start.assert_called_once()
    assert migrator.state == "idle"
    assert migrator.mode == "traverse"


@pytest.mark.asyncio
async def test_start_upload(migrator, mock_uploader_manager):
    """Test starting the upload process."""
    await migrator.start_upload()

    mock_uploader_manager.start.assert_called_once()
    assert migrator.state == "idle"
    assert migrator.mode == "upload"


@pytest.mark.asyncio
async def test_stop_traversal(migrator, mock_traverser_manager):
    """Test stopping the traversal process."""
    migrator.mode = "traverse"
    migrator.state = "traversing"

    await migrator.stop()

    mock_traverser_manager.stop.assert_called_once()
    assert migrator.state == "stopped"


@pytest.mark.asyncio
async def test_stop_upload(migrator, mock_uploader_manager):
    """Test stopping the upload process."""
    migrator.mode = "upload"
    migrator.state = "uploading"

    await migrator.stop()

    mock_uploader_manager.stop.assert_called_once()
    assert migrator.state == "stopped"


@pytest.mark.asyncio
async def test_pause_operation(migrator):
    """Test pausing an operation."""
    migrator.state = "traversing"

    await migrator.pause()

    assert migrator.state == "paused"


@pytest.mark.asyncio
async def test_resume_traversal(migrator, mock_traverser_manager):
    """Test resuming the traversal process."""
    migrator.state = "paused"
    migrator.mode = "traverse"

    await migrator.resume()

    mock_traverser_manager.start.assert_called_once()
    assert migrator.state == "idle"


@pytest.mark.asyncio
async def test_resume_upload(migrator, mock_uploader_manager):
    """Test resuming the upload process."""
    migrator.state = "paused"
    migrator.mode = "upload"

    await migrator.resume()

    mock_uploader_manager.start.assert_called_once()
    assert migrator.state == "idle"


def test_get_status(migrator, mock_traverser_manager, mock_uploader_manager):
    """Test getting the migrator's status."""
    migrator.state = "traversing"
    migrator.mode = "traverse"

    status = migrator.get_status()
    assert status["state"] == "traversing"
    assert status["mode"] == "traverse"
    assert status["workers_traversing"] == len(mock_traverser_manager.workers)
    assert status["workers_uploading"] == len(mock_uploader_manager.workers)
