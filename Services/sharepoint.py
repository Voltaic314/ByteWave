from Services._api_base import APIBase
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem


class SharePoint(APIBase):

    def __init__(self, request: dict):
        super().__init__(request)
        self.connection = self.stax_api.get_connection().get("data", {})
        self.api_url = self.connection.get("api_url", "https://graph.microsoft.com/v1.0")
        self.site_id = self.connection.get("site", {}).get("id", "")
        self.set_token_expiration(expires_in=3600)
        self.cursor_keyword = "@odata.nextLink"
        self.limit = 1000

    def refresh_token(self):
        """Refresh the API token specific to SharePoint."""
        new_access_token = self.stax_api.refresh_token()
        self.set_token_expiration(expires_in=3600)
        return new_access_token

    def set_root_directory_first_folder_identifier(self):
        """Set the root folder identifier for SharePoint."""
        return "root"

    async def is_directory(self, identifier: str) -> bool:
        """
        Determine if the item is a directory based on SharePoint metadata.

        Args:
            identifier (str): The item's identifier.

        Returns:
            bool: True if the item is a folder, False otherwise.
        """
        response = await self.make_request("GET", f"{self.api_url}/sites/{self.site_id}/drive/items/{identifier}")
        if response is None:
            return False
        return "folder" in response.json()

    async def get_all_items(self, folder: FolderSubItem) -> list:
        """
        Retrieve all items in a SharePoint folder, handling pagination.

        Args:
            folder (FolderSubItem): The folder to retrieve items from.

        Returns:
            list: A list of FileSubItem or FolderSubItem objects.
        """
        folder_id = folder.identifier
        parent_path = folder.path

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
            for item in items:
                item_type = "folder" if "folder" in item else "file"
                name = item.get("name")
                identifier = item.get("id")
                path = f"{parent_path}/{name}".strip("/")

                if item_type == "folder":
                    all_items.append(
                        FolderSubItem(name=name, path=path, identifier=identifier, parent_id=folder.identifier)
                    )
                else:
                    size = item.get("size", None)
                    all_items.append(
                        FileSubItem(name=name, path=path, identifier=identifier, parent_id=folder.identifier, size=size)
                    )

            # Handle pagination
            next_url = response_data.get(self.cursor_keyword, None)

        return all_items

    async def create_folder(self, parent_folder: FolderSubItem, folder_name: str) -> FolderSubItem:
        """
        Create a folder in SharePoint under the specified parent folder.

        Args:
            parent_folder (FolderSubItem): The parent folder where the new folder will be created.
            folder_name (str): The name of the folder to create.

        Returns:
            FolderSubItem: Metadata of the created folder.
        """
        url = f"{self.api_url}/sites/{self.site_id}/drive/items/{parent_folder.identifier}/children"
        data = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename"
        }
        response = await self.make_request("POST", url, headers=self.headers, json=data)
        if response is None:
            raise Exception(f"Failed to create folder: {folder_name}")
        created_folder = response.json()
        return FolderSubItem(
            name=created_folder.get("name"),
            path=f"{parent_folder.path}/{created_folder.get('name')}".strip("/"),
            identifier=created_folder.get("id"),
            parent_id=parent_folder.identifier
        )

    async def upload_file(self, parent_folder: FolderSubItem, file: FileSubItem, file_content: bytes) -> FileSubItem:
        """
        Upload a file to SharePoint.

        Args:
            parent_folder (FolderSubItem): The parent folder where the file will be uploaded.
            file (FileSubItem): The file to upload.
            file_content (bytes): Binary content of the file to upload.

        Returns:
            FileSubItem: Metadata of the uploaded file.
        """
        url = f"{self.api_url}/sites/{self.site_id}/drive/items/{parent_folder.identifier}:/{file.name}:/content"
        headers = {**self.headers, "Content-Type": "application/octet-stream"}
        response = await self.make_request("PUT", url, headers=headers, data=file_content)
        if response is None:
            raise Exception(f"Failed to upload file: {file.name}")
        uploaded_file = response.json()
        return FileSubItem(
            name=uploaded_file.get("name"),
            path=f"{parent_folder.path}/{uploaded_file.get('name')}".strip("/"),
            identifier=uploaded_file.get("id"),
            parent_id=parent_folder.identifier,
            size=uploaded_file.get("size", None)
        )

    async def get_file_contents(self, file: FileSubItem) -> bytes:
        """
        Retrieve the contents of a SharePoint file as binary data.

        Args:
            file (FileSubItem): The file to retrieve contents from.

        Returns:
            bytes: The binary content of the file.
        """
        try:
            url = f"{self.api_url}/sites/{self.site_id}/drive/items/{file.identifier}/content"
            response = await self.make_request("GET", url, headers=self.headers, stream=True)
            return response.content if response else b''
        except Exception as e:
            print(f"Exception while retrieving file content for file ID {file.identifier}: {str(e)}")
            return b''
