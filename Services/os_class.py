import os
import asyncio
import aiofiles


class OS_Class:

    def __init__(self):
        """
        Initialize the OS class with request parameters and tracking attributes.
        """
        self.total_disk_reads = 0
        self.total_disk_writes = 0

    async def get_all_items(self, folder_metadata: dict) -> list:
        """
        Retrieve all items in a folder asynchronously.

        Args:
            folder_metadata (dict): Metadata containing the folder's path and other details.

        Returns:
            list: A list of metadata dictionaries for the items in the folder.
        """
        folder_path = self.normalize_path(folder_metadata["path"])
        self.total_disk_reads += 1  # Increment disk reads for accessing the directory
        items_metadata = []

        try:
            # Use asyncio.to_thread to run os.listdir in a non-blocking manner
            items = await asyncio.to_thread(os.listdir, folder_path)
            for item_name in items:
                item_path = self.normalize_path(os.path.join(folder_path, item_name))
                item_type = "folder" if os.path.isdir(item_path) else "file"
                items_metadata.append(self.to_metadata(item_name, item_path, item_type))
        except Exception as e:
            print(f"Error reading folder {folder_path}: {str(e)}")

        return items_metadata

    async def get_file_contents(self, file_metadata: dict) -> bytes:
        """
        Retrieve the contents of a file asynchronously.

        Args:
            file_metadata (dict): Metadata containing the file's path.

        Returns:
            bytes: The contents of the file.
        """
        file_path = self.normalize_path(file_metadata["path"])
        self.total_disk_reads += 1  # Increment disk reads for accessing the file

        try:
            async with aiofiles.open(file_path, "rb") as file:
                return await file.read()
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            return b''

    async def create_folder(self, folder_metadata: dict):
        """
        Create a folder asynchronously if it does not already exist.

        Args:
            folder_metadata (dict): Metadata containing the folder's path.
        """
        folder_path = self.normalize_path(folder_metadata["path"])

        try:
            # Use asyncio.to_thread for non-blocking directory creation
            if not os.path.exists(folder_path):
                await asyncio.to_thread(os.makedirs, folder_path)
                self.total_disk_writes += 1  # Increment disk writes for folder creation
        except Exception as e:
            print(f"Error creating folder {folder_path}: {str(e)}")

    async def upload_file(self, file_metadata: dict):
        """
        Write the contents of a file to the disk asynchronously.

        Args:
            file_metadata (dict): Metadata containing the file's path and contents.
        """
        file_path = self.normalize_path(file_metadata["path"])
        file_contents = file_metadata.get("contents", b'')

        try:
            async with aiofiles.open(file_path, "wb") as file:
                await file.write(file_contents)
                self.total_disk_writes += 1  # Increment disk writes for file creation
        except Exception as e:
            print(f"Error writing file {file_path}: {str(e)}")

    def to_metadata(self, name: str, path: str, item_type: str) -> dict:
        """
        Convert file/folder attributes to a consistent metadata structure.

        Args:
            name (str): Name of the file or folder.
            path (str): Absolute path of the file or folder.
            item_type (str): Type of the item ("folder" or "file").

        Returns:
            dict: Metadata dictionary.
        """
        return {
            "type": item_type,
            "name": name,
            "path": path,
            "identifier": path,  # For OS, path is the identifier
        }

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
