from boxsdk import OAuth2, Client
from Services._api_base import APIBase

class Box(APIBase):

    def __init__(self, request: dict):
        super().__init__(request)  # Initialize base class properties
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
        self.limit = 1000  # Default limit for API calls


    async def refresh_token(self):
        """Refresh the API token specific to Box."""
        new_access_token = await self.stax_api.refresh_token()
        self.set_token_expiration(expires_in=3600)
        return new_access_token

    def to_metadata(self, item_name, item_id, item_type) -> dict:
        """
        Convert item details into a standardized metadata dictionary.

        Args:
            item_name (str): Name of the item (file or folder).
            item_id (str): ID of the item.
            item_type (str): Type of the item, either "file" or "folder".

        Returns:
            dict: Metadata for the item.
        """
        return {
            "type": item_type,
            "name": item_name,
            "identifier": item_id,
            "path": None  # Traverser/Uploader will populate this
        }

    def set_root_directory_first_folder_identifier(self):
        """Set the root folder identifier for Box."""
        return "0"  # Box uses "0" as the root folder ID

    def get_folder_info(self, item) -> tuple:
        """Return the name and ID of the folder, as expected by the base class."""
        return item.name, item.id

    async def get_all_items(self, folder_id, **kwargs):
        """
        Retrieve all items in a Box folder using Box's API and handle pagination.

        Args:
            folder_id (str): Identifier of the folder to retrieve items from.
            **kwargs: Additional arguments for SDK calls.

        Returns:
            list: A list of item metadata dictionaries.
        """
        items_metadata = []
        try:
            folder = self.client.folder(folder_id)
            items = await self.try_sdk_request(folder.get_items, limit=self.limit, **kwargs)
            api_calls_for_this_folder = (len(items) // self.limit) + 1
            self.api_calls_made += api_calls_for_this_folder

            for item in items:
                item_name, item_id = self.get_folder_info(item)
                item_type = "folder" if item.type == "folder" else "file"
                items_metadata.append(self.to_metadata(item_name, item_id, item_type))

        except Exception as e:
            print(f"Exception while retrieving items for folder {folder_id}: {str(e)}")

        return items_metadata

    async def get_file_contents(self, file_id: str) -> bytes:
        """Retrieve the contents of a file from Box as binary data."""
        try:
            return await self.try_sdk_request(self.client.file(file_id).content)
        except Exception as e:
            print(f"Exception while getting file contents for file ID {file_id}: {str(e)}")
            return b''

    async def create_folder(self, parent_id: str, folder_name: str) -> dict:
        """
        Create a folder in Box under the specified parent ID.

        Args:
            parent_id (str): The ID of the parent folder.
            folder_name (str): The name of the new folder.

        Returns:
            dict: Metadata of the created folder.
        """
        try:
            parent_folder = self.client.folder(parent_id)
            folder = await self.try_sdk_request(parent_folder.create_subfolder, folder_name)
            return self.to_metadata(folder.name, folder.id, "folder")
        except Exception as e:
            print(f"Exception while creating folder {folder_name} in parent {parent_id}: {str(e)}")
            return {}

    async def upload_file(self, parent_id: str, file_name: str, file_content: bytes) -> dict:
        """
        Upload a file to Box under the specified parent folder.

        Args:
            parent_id (str): The ID of the parent folder.
            file_name (str): The name of the file to upload.
            file_content (bytes): Binary content of the file.

        Returns:
            dict: Metadata of the uploaded file.
        """
        try:
            parent_folder = self.client.folder(parent_id)
            file = await self.try_sdk_request(parent_folder.upload_stream, file_content, file_name)
            return self.to_metadata(file.name, file.id, "file")
        except Exception as e:
            print(f"Exception while uploading file {file_name} to parent {parent_id}: {str(e)}")
            return {}

    def is_directory(self, item_metadata: dict) -> bool:
        """Determine if the item is a directory based on Box metadata."""
        return item_metadata.get("type", "").lower() == "folder"
