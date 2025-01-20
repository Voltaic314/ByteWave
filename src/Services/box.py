from boxsdk import OAuth2, Client
from Services._api_base import APIBase
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem


class Box(APIBase):
    
    def __init__(self, request: dict):
        super().__init__(request)
        self.initialize_box_client()

    def initialize_box_client(self):
        """Initialize the Box client and set the access token."""
        self.connection = self.stax_api.get_connection().get("data", {})
        self.access_token = self.refresh_token()

        if not self.access_token:
            raise ValueError("Access token is required to initialize Box API")

        self.auth = OAuth2(access_token=self.access_token)
        self.client = Client(self.auth)
        self.set_token_expiration(expires_in=3600)
        self.limit = 1000

    async def refresh_token(self):
        """Refresh the API token specific to Box."""
        new_access_token = await self.stax_api.refresh_token()
        self.set_token_expiration(expires_in=3600)
        return new_access_token

    def set_root_directory_first_folder_identifier(self):
        """Set the root folder identifier for Box."""
        return "0"  # Box uses "0" as the root folder ID

    async def is_directory(self, identifier: str) -> bool:
        """
        Determine if the item is a directory based on its metadata.

        Args:
            identifier (str): The item's identifier.

        Returns:
            bool: True if the item is a directory, False otherwise.
        """
        try:
            item = await self.try_sdk_request(self.client.item, identifier)
            return item.type == "folder"
        except Exception as e:
            print(f"Exception while checking if item is directory for ID {identifier}: {str(e)}")
            return False

    async def get_all_items(self, folder: FolderSubItem, **kwargs) -> list:
        """
        Retrieve all items in a Box folder using Box's API and handle pagination.

        Args:
            folder (FolderSubItem): The folder to retrieve items from.

        Returns:
            list: A list of FileSubItem or FolderSubItem objects.
        """
        items = []
        try:
            box_folder = self.client.folder(folder.identifier)
            box_items = await self.try_sdk_request(box_folder.get_items, limit=self.limit, **kwargs)
            api_calls_for_this_folder = (len(box_items) // self.limit) + 1
            self.api_calls_made += api_calls_for_this_folder

            for item in box_items:
                if item.type == "folder":
                    items.append(
                        FolderSubItem(
                            name=item.name,
                            path=f"{folder.path}/{item.name}".strip("/"),
                            identifier=item.id,
                            parent_id=folder.identifier,
                        )
                    )
                else:
                    items.append(
                        FileSubItem(
                            name=item.name,
                            path=f"{folder.path}/{item.name}".strip("/"),
                            identifier=item.id,
                            parent_id=folder.identifier,
                            size=item.size,
                        )
                    )

        except Exception as e:
            print(f"Exception while retrieving items for folder {folder.identifier}: {str(e)}")

        return items

    async def get_file_contents(self, file: FileSubItem) -> bytes:
        """
        Retrieve the contents of a file from Box as binary data.

        Args:
            file (FileSubItem): The file to retrieve contents from.

        Returns:
            bytes: The file's binary content.
        """
        try:
            return await self.try_sdk_request(self.client.file(file.identifier).content)
        except Exception as e:
            print(f"Exception while getting file contents for file ID {file.identifier}: {str(e)}")
            return b""

    async def create_folder(self, parent_folder: FolderSubItem, folder_name: str) -> FolderSubItem:
        """
        Create a folder in Box under the specified parent folder.

        Args:
            parent_folder (FolderSubItem): The parent folder where the new folder will be created.
            folder_name (str): The name of the folder to create.

        Returns:
            FolderSubItem: Metadata of the created folder.
        """
        try:
            box_parent_folder = self.client.folder(parent_folder.identifier)
            new_folder = await self.try_sdk_request(box_parent_folder.create_subfolder, folder_name)
            return FolderSubItem(
                name=new_folder.name,
                path=f"{parent_folder.path}/{new_folder.name}".strip("/"),
                identifier=new_folder.id,
                parent_id=parent_folder.identifier,
            )
        except Exception as e:
            print(f"Exception while creating folder {folder_name} in parent {parent_folder.identifier}: {str(e)}")
            return None

    async def upload_file(self, parent_folder: FolderSubItem, file: FileSubItem, file_content: bytes) -> FileSubItem:
        """
        Upload a file to Box under the specified parent folder.

        Args:
            parent_folder (FolderSubItem): The parent folder where the file will be uploaded.
            file (FileSubItem): Metadata of the file to upload.
            file_content (bytes): Binary content of the file.

        Returns:
            FileSubItem: Metadata of the uploaded file.
        """
        try:
            box_parent_folder = self.client.folder(parent_folder.identifier)
            uploaded_file = await self.try_sdk_request(box_parent_folder.upload_stream, file_content, file.name)
            return FileSubItem(
                name=uploaded_file.name,
                path=f"{parent_folder.path}/{uploaded_file.name}".strip("/"),
                identifier=uploaded_file.id,
                parent_id=parent_folder.identifier,
                size=uploaded_file.size,
            )
        except Exception as e:
            print(f"Exception while uploading file {file.name} to parent {parent_folder.identifier}: {str(e)}")
            return None
