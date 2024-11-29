from Services._api_base import APIBase

class SharePoint(APIBase):

    def __init__(self, request: dict):
        super().__init__(request)  # Initialize base class properties
        self.connection = self.stax_api.get_connection().get("data", {})
        self.api_url = self.connection.get("api_url", "https://graph.microsoft.com/v1.0")
        self.site_id = self.connection.get("site", {}).get("id", "")
        self.set_token_expiration(expires_in=3600)  # Set token expiry
        self.cursor_keyword = "@odata.nextLink"
        self.limit = 1000  # Default limit for API calls

    def refresh_token(self):
        """Refresh the API token specific to SharePoint."""
        new_access_token = self.stax_api.refresh_token()
        self.set_token_expiration(expires_in=3600)
        return new_access_token

    def set_root_directory_first_folder_identifier(self):
        """Set the root folder identifier for SharePoint."""
        return "root"

    def get_folder_info(self, item: dict) -> tuple:
        """Return the folder's name and ID as expected by the base class."""
        return item.get("name"), item.get("id")

    def to_metadata(self, item: dict, parent_path: str) -> dict:
        """
        Convert a SharePoint item into a standardized metadata dictionary.

        Args:
            item (dict): The SharePoint item dictionary.
            parent_path (str): The path of the parent directory.

        Returns:
            dict: Metadata containing 'type', 'name', 'identifier', and 'path'.
        """
        item_type = "folder" if "folder" in item else "file"
        name = item.get("name")
        identifier = item.get("id")
        path = f"{parent_path}/{name}".strip("/")

        return {
            "type": item_type,
            "name": name,
            "identifier": identifier,
            "path": path,
        }

    async def get_all_items(self, folder_metadata: dict) -> list:
        """
        Retrieve all items in a SharePoint folder, handling pagination.

        Args:
            folder_metadata (dict): Metadata of the folder to retrieve items from.

        Returns:
            list: A list of standardized metadata dictionaries.
        """
        folder_id = folder_metadata.get("identifier")
        parent_path = folder_metadata.get("path", "")

        if folder_id == self.set_root_directory_first_folder_identifier():
            url = f"{self.api_url}/sites/{self.site_id}/drive/root/children"
        else:
            url = f"{self.api_url}/sites/{self.site_id}/drive/items/{folder_id}/children"

        url = f"{url}?$top={self.limit}"

        all_items = []
        next_url = url

        while next_url:
            response = await self.make_request("GET", next_url, headers=self.headers)
            if response is None:
                print(f"Failed to retrieve items for folder ID: {folder_id}")
                break

            response_data = response.json()
            items = response_data.get("value", [])
            all_items.extend([self.to_metadata(item, parent_path) for item in items])

            # Handle pagination
            next_url = response_data.get(self.cursor_keyword, None)

        return all_items

    async def create_folder(self, parent_id: str, folder_name: str) -> dict:
        """
        Create a folder in SharePoint under the specified parent ID.

        Args:
            parent_id (str): The identifier of the parent folder.
            folder_name (str): The name of the folder to create.

        Returns:
            dict: Metadata of the created folder.
        """
        url = f"{self.api_url}/sites/{self.site_id}/drive/items/{parent_id}/children"
        data = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename"
        }
        response = await self.make_request("POST", url, headers=self.headers, json=data)
        if response is None:
            raise Exception(f"Failed to create folder: {folder_name}")
        return response.json()

    async def upload_file(self, parent_id: str, file_name: str, file_content: bytes) -> dict:
        """
        Upload a file to SharePoint.

        Args:
            parent_id (str): The identifier of the parent folder.
            file_name (str): The name of the file to upload.
            file_content (bytes): Binary content of the file to upload.

        Returns:
            dict: Metadata of the uploaded file.
        """
        url = f"{self.api_url}/sites/{self.site_id}/drive/items/{parent_id}:/{file_name}:/content"
        headers = {**self.headers, "Content-Type": "application/octet-stream"}
        response = await self.make_request("PUT", url, headers=headers, data=file_content)
        if response is None:
            raise Exception(f"Failed to upload file: {file_name}")
        return response.json()

    async def get_file_contents(self, file_id: str) -> bytes:
        """Retrieve the contents of a SharePoint file as binary data."""
        try:
            url = f"{self.api_url}/sites/{self.site_id}/drive/items/{file_id}/content"
            response = await self.make_request("GET", url, headers=self.headers, stream=True)
            return response.content if response else b''
        except Exception as e:
            print(f"Exception while retrieving file content for file ID {file_id}: {str(e)}")
            return b''

    def is_directory(self, item_metadata: dict) -> bool:
        """Determine if the item is a directory based on SharePoint metadata."""
        return str(item_metadata.get("type", "")).lower() == "folder"
