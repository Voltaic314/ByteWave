import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from Services.box import Box
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem


@pytest.fixture
def mock_request():
    """Fixture for initializing the Box API class with mock request data."""
    return {
        "access_token": "initial_access_token",
        "refresh_token": "initial_refresh_token",
        "expires_in": 3600,
        "service_name": "Box",
    }


@pytest.fixture
def mock_box(mock_request):
    """Fixture for initializing the Box class with a mock request."""
    with patch("Services.box.Client"), patch("Services.box.OAuth2"):
        return Box(mock_request)


### Tests ###
def test_initialize_box_client(mock_box):
    """Test the initialization of the Box client."""
    assert mock_box.access_token == "initial_access_token"
    assert mock_box.service_name == "Box"
    assert mock_box.limit == 1000


@pytest.mark.asyncio
async def test_refresh_token(mock_box):
    """Test the refresh_token method."""
    mock_box.stax_api = AsyncMock()
    mock_box.stax_api.refresh_token.return_value = "new_access_token"

    new_token = await mock_box.refresh_token()
    assert new_token == "new_access_token"


@pytest.mark.asyncio
async def test_is_directory(mock_box):
    """Test the is_directory method."""
    mock_box.client.item = AsyncMock(return_value=MagicMock(type="folder"))
    assert await mock_box.is_directory("1234") is True

    mock_box.client.item = AsyncMock(return_value=MagicMock(type="file"))
    assert await mock_box.is_directory("5678") is False


@pytest.mark.asyncio
async def test_get_all_items(mock_box):
    """Test the get_all_items method."""
    mock_folder = FolderSubItem(name="test_folder", path="/test_folder", identifier="1234")
    mock_item_folder = MagicMock(type="folder", name="sub_folder", id="5678")
    mock_item_file = MagicMock(type="file", name="file.txt", id="7890", size=1024)
    
    mock_box.client.folder = MagicMock(return_value=MagicMock(get_items=AsyncMock(return_value=[mock_item_folder, mock_item_file])))

    items = await mock_box.get_all_items(mock_folder)
    assert len(items) == 2

    folder_item = items[0]
    assert isinstance(folder_item, FolderSubItem)
    assert folder_item.name == "sub_folder"

    file_item = items[1]
    assert isinstance(file_item, FileSubItem)
    assert file_item.name == "file.txt"
    assert file_item.size == 1024


@pytest.mark.asyncio
async def test_get_file_contents(mock_box):
    """Test the get_file_contents method."""
    mock_file = FileSubItem(name="file.txt", path="/test_folder/file.txt", identifier="7890")
    mock_box.client.file = MagicMock(return_value=MagicMock(content=AsyncMock(return_value=b"file content")))

    content = await mock_box.get_file_contents(mock_file)
    assert content == b"file content"


@pytest.mark.asyncio
async def test_create_folder(mock_box):
    """Test the create_folder method."""
    parent_folder = FolderSubItem(name="parent_folder", path="/parent_folder", identifier="1234")
    new_folder = MagicMock(name="new_folder", id="5678")

    mock_box.client.folder = MagicMock(return_value=MagicMock(create_subfolder=AsyncMock(return_value=new_folder)))

    created_folder = await mock_box.create_folder(parent_folder, "new_folder")
    assert isinstance(created_folder, FolderSubItem)
    assert created_folder.name == "new_folder"
    assert created_folder.identifier == "5678"
    assert created_folder.path == "/parent_folder/new_folder"


@pytest.mark.asyncio
async def test_upload_file(mock_box):
    """Test the upload_file method."""
    parent_folder = FolderSubItem(name="parent_folder", path="/parent_folder", identifier="1234")
    file = FileSubItem(name="file.txt", path="/parent_folder/file.txt", identifier="7890", size=1024)

    uploaded_file = MagicMock(name="file.txt", id="7890", size=1024)
    mock_box.client.folder = MagicMock(return_value=MagicMock(upload_stream=AsyncMock(return_value=uploaded_file)))

    uploaded_item = await mock_box.upload_file(parent_folder, file, b"file content")
    assert isinstance(uploaded_item, FileSubItem)
    assert uploaded_item.name == "file.txt"
    assert uploaded_item.size == 1024
    assert uploaded_item.identifier == "7890"
    assert uploaded_item.path == "/parent_folder/file.txt"
