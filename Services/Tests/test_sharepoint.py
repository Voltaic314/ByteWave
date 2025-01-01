import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from Services.sharepoint import SharePoint
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem


@pytest.fixture
def mock_request():
    """Fixture for initializing the SharePoint class with mock request data."""
    return {
        "access_token": "initial_access_token",
        "refresh_token": "initial_refresh_token",
        "expires_in": 3600,
        "service_name": "SharePoint",
    }


@pytest.fixture
def mock_sharepoint(mock_request):
    """Fixture for initializing the SharePoint class with a mock request."""
    with patch("Services.sharepoint.APIBase.make_request", new_callable=AsyncMock):
        return SharePoint(mock_request)


### Tests ###
def test_initialize_sharepoint(mock_sharepoint):
    """Test the initialization of the SharePoint API."""
    assert mock_sharepoint.access_token == "initial_access_token"
    assert mock_sharepoint.api_url == "https://graph.microsoft.com/v1.0"
    assert mock_sharepoint.site_id == ""
    assert mock_sharepoint.limit == 1000


@pytest.mark.asyncio
async def test_is_directory(mock_sharepoint):
    """Test the is_directory method."""
    # Mock the response to indicate a folder
    mock_sharepoint.make_request.return_value = MagicMock(json=MagicMock(return_value={"folder": {}}))
    assert await mock_sharepoint.is_directory("1234") is True

    # Mock the response to indicate a file
    mock_sharepoint.make_request.return_value = MagicMock(json=MagicMock(return_value={"file": {}}))
    assert await mock_sharepoint.is_directory("5678") is False


@pytest.mark.asyncio
async def test_get_all_items(mock_sharepoint):
    """Test the get_all_items method."""
    mock_folder = FolderSubItem(name="test_folder", path="/test_folder", identifier="1234")
    mock_response_data = {
        "value": [
            {"name": "sub_folder", "id": "5678", "folder": {}},
            {"name": "file.txt", "id": "7890", "file": {"size": 1024}}
        ]
    }

    # Mock paginated responses
    mock_sharepoint.make_request.side_effect = [
        MagicMock(json=MagicMock(return_value=mock_response_data)),
        None  # Indicating no more pages
    ]

    items = await mock_sharepoint.get_all_items(mock_folder)
    assert len(items) == 2

    folder_item = items[0]
    assert isinstance(folder_item, FolderSubItem)
    assert folder_item.name == "sub_folder"

    file_item = items[1]
    assert isinstance(file_item, FileSubItem)
    assert file_item.name == "file.txt"
    assert file_item.size == 1024


@pytest.mark.asyncio
async def test_create_folder(mock_sharepoint):
    """Test the create_folder method."""
    parent_folder = FolderSubItem(name="parent_folder", path="/parent_folder", identifier="1234")
    mock_created_folder = {"name": "new_folder", "id": "5678"}

    # Mock the POST request
    mock_sharepoint.make_request.return_value = MagicMock(json=MagicMock(return_value=mock_created_folder))

    created_folder = await mock_sharepoint.create_folder(parent_folder, "new_folder")
    assert isinstance(created_folder, FolderSubItem)
    assert created_folder.name == "new_folder"
    assert created_folder.identifier == "5678"
    assert created_folder.path == "/parent_folder/new_folder"


@pytest.mark.asyncio
async def test_upload_file(mock_sharepoint):
    """Test the upload_file method."""
    parent_folder = FolderSubItem(name="parent_folder", path="/parent_folder", identifier="1234")
    file = FileSubItem(name="file.txt", path="/parent_folder/file.txt", identifier="7890", size=1024)
    mock_uploaded_file = {"name": "file.txt", "id": "7890", "size": 1024}

    # Mock the PUT request
    mock_sharepoint.make_request.return_value = MagicMock(json=MagicMock(return_value=mock_uploaded_file))

    uploaded_item = await mock_sharepoint.upload_file(parent_folder, file, b"file content")
    assert isinstance(uploaded_item, FileSubItem)
    assert uploaded_item.name == "file.txt"
    assert uploaded_item.size == 1024
    assert uploaded_item.identifier == "7890"
    assert uploaded_item.path == "/parent_folder/file.txt"


@pytest.mark.asyncio
async def test_get_file_contents(mock_sharepoint):
    """Test the get_file_contents method."""
    mock_file = FileSubItem(name="file.txt", path="/test_folder/file.txt", identifier="7890")

    # Mock the GET request for file content
    mock_sharepoint.make_request.return_value = MagicMock(content=b"file content")

    content = await mock_sharepoint.get_file_contents(mock_file)
    assert content == b"file content"
