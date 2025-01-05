import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem
from Services.os_class import OS_Class


@pytest.fixture
def os_class():
    """Fixture to initialize the OS_Class."""
    return OS_Class()


### Tests ###
@pytest.mark.asyncio
async def test_is_directory(os_class):
    """Test the is_directory method."""
    with patch("os.path.isdir", return_value=True) as mock_isdir:
        assert await os_class.is_directory("/path/to/folder") is True
        mock_isdir.assert_called_once_with("/path/to/folder")


@pytest.mark.asyncio
async def test_get_all_items(os_class):
    """Test the get_all_items method."""
    folder = FolderSubItem(name="test_folder", path="/path/to/folder", identifier="/path/to/folder")
    raw_items = ["file1.txt", "subfolder"]
    
    with patch("os.listdir", return_value=raw_items) as mock_listdir, \
         patch("os.path.isdir", side_effect=[False, True]) as mock_isdir, \
         patch("os.path.getsize", return_value=1024) as mock_getsize:
        
        items = await os_class.get_all_items(folder)
        assert len(items) == 2
        
        file_item = items[0]
        assert isinstance(file_item, FileSubItem)
        assert file_item.name == "file1.txt"
        assert file_item.size == 1024

        folder_item = items[1]
        assert isinstance(folder_item, FolderSubItem)
        assert folder_item.name == "subfolder"

        mock_listdir.assert_called_once_with("/path/to/folder")
        mock_isdir.assert_any_call("/path/to/folder/file1.txt")
        mock_isdir.assert_any_call("/path/to/folder/subfolder")
        mock_getsize.assert_called_once_with("/path/to/folder/file1.txt")


@pytest.mark.asyncio
async def test_get_file_contents(os_class):
    """Test the get_file_contents method."""
    file = FileSubItem(name="file.txt", path="/path/to/file.txt", identifier="/path/to/file.txt")

    with patch("aiofiles.open", new_callable=AsyncMock) as mock_open:
        mock_file = MagicMock()
        mock_open.return_value.__aenter__.return_value = mock_file
        mock_file.read.return_value = b"file content"

        content = await os_class.get_file_contents(file)
        assert content == b"file content"
        mock_open.assert_called_once_with("/path/to/file.txt", "rb")


@pytest.mark.asyncio
async def test_create_folder(os_class):
    """Test the create_folder method."""
    folder = FolderSubItem(name="test_folder", path="/path/to/folder", identifier="/path/to/folder")

    with patch("os.path.exists", return_value=False) as mock_exists, \
         patch("os.makedirs") as mock_makedirs:
        
        await os_class.create_folder(folder)
        mock_exists.assert_called_once_with("/path/to/folder")
        mock_makedirs.assert_called_once_with("/path/to/folder")


@pytest.mark.asyncio
async def test_upload_file(os_class):
    """Test the upload_file method."""
    file = FileSubItem(name="file.txt", path="/path/to/file.txt", identifier="/path/to/file.txt")
    content = b"file content"

    with patch("aiofiles.open", new_callable=AsyncMock) as mock_open:
        mock_file = MagicMock()
        mock_open.return_value.__aenter__.return_value = mock_file

        await os_class.upload_file(file, content)
        mock_open.assert_called_once_with("/path/to/file.txt", "wb")
        mock_file.write.assert_called_once_with(content)


def test_normalize_path():
    """Test the normalize_path method."""
    path = "C:\\path\\to\\file.txt"
    normalized = OS_Class.normalize_path(path)
    assert normalized == "C:/path/to/file.txt"
