from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem
import os
import asyncio
import aiofiles


class OS_Class:
    """
    Handles file and folder operations for the OS as a source or destination.
    """

    def __init__(self):
        """
        Initialize the OS class with tracking attributes.
        """
        self.total_disk_reads = 0
        self.total_disk_writes = 0

    async def is_directory(self, identifier: str) -> bool:
        """
        Determine if an item is a directory.

        Args:
            identifier (str): The item's identifier (path for OS).

        Returns:
            bool: True if the item is a directory, False otherwise.
        """
        self.total_disk_reads += 1  # Increment for the check
        return await asyncio.to_thread(os.path.isdir, identifier)

    async def get_all_items(self, folder: FolderSubItem) -> list:
        """
        Retrieve all items in a folder asynchronously.

        Args:
            folder (FolderSubItem): The folder to retrieve items from.

        Returns:
            list: A list of FileSubItem or FolderSubItem objects.
        """
        folder_path = self.normalize_path(folder.path)
        self.total_disk_reads += 1  # Increment for accessing the directory
        items = []

        try:
            # Use asyncio.to_thread to run os.listdir in a non-blocking manner
            raw_items = await asyncio.to_thread(os.listdir, folder_path)
            for item_name in raw_items:
                item_path = self.normalize_path(os.path.join(folder_path, item_name))
                if await self.is_directory(item_path):
                    # Create FolderSubItem
                    items.append(
                        FolderSubItem(
                            name=item_name,
                            path=item_path,
                            identifier=item_path,
                            parent_id=folder.identifier,
                        )
                    )
                else:
                    # Create FileSubItem
                    items.append(
                        FileSubItem(
                            name=item_name,
                            path=item_path,
                            identifier=item_path,
                            parent_id=folder.identifier,
                            size=await asyncio.to_thread(os.path.getsize, item_path),
                        )
                    )
                    self.total_disk_reads += 1  # Increment for getsize
        except Exception as e:
            print(f"Error reading folder {folder.path}: {str(e)}")

        return items

    async def get_file_contents(self, file: FileSubItem) -> bytes:
        """
        Retrieve the contents of a file asynchronously.

        Args:
            file (FileSubItem): The file to retrieve contents from.

        Returns:
            bytes: The contents of the file.
        """
        self.total_disk_reads += 1  # Increment for accessing the file

        try:
            async with aiofiles.open(file.identifier, "rb") as f:
                return await f.read()
        except Exception as e:
            print(f"Error reading file {file.identifier}: {str(e)}")
            return b""

    async def create_folder(self, folder: FolderSubItem):
        """
        Create a folder asynchronously if it does not already exist.

        Args:
            folder (FolderSubItem): The folder to create.
        """
        folder_path = self.normalize_path(folder.path)

        try:
            # Use asyncio.to_thread for non-blocking directory creation
            if not os.path.exists(folder_path):
                await asyncio.to_thread(os.makedirs, folder_path)
                self.total_disk_writes += 1  # Increment for folder creation
        except Exception as e:
            print(f"Error creating folder {folder.path}: {str(e)}")

    async def upload_file(self, file: FileSubItem, contents: bytes):
        """
        Write the contents of a file to the disk asynchronously.

        Args:
            file (FileSubItem): The file to write.
            contents (bytes): The contents to write to the file.
        """
        self.total_disk_writes += 1  # Increment for file creation

        try:
            async with aiofiles.open(file.identifier, "wb") as f:
                await f.write(contents)
        except Exception as e:
            print(f"Error writing file {file.identifier}: {str(e)}")

    @staticmethod
    def normalize_path(path: str) -> str:
        """
        Ensure consistent path formatting with forward slashes.

        Args:
            path (str): The path to normalize.

        Returns:
            str: The normalized path.
        """
        return path.replace("\\", "/")
