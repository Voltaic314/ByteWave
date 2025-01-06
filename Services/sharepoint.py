from Services._api_base import APIBase
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem


class SharePoint(APIBase):
    def __init__(self, request: dict, db, client_id: str, client_secret: str, tenant_id: str, roles: list = []):
        """
        Initialize the SharePoint service integration.

        Args:
            request (dict): Dictionary containing token and other initialization data.
            db (DB): Database instance for token storage and updates.
            client_id (str): Client ID for OAuth.
            client_secret (str): Client secret for OAuth.
            tenant_id (str): Tenant ID for the SharePoint organization.
            roles (list): List of roles for this instance (e.g., ["source", "destination"]).
        """
        # Construct the token manager through APIBase
        request.update({
            "refresh_token_url": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        })
        super().__init__(request, db, roles)

        # SharePoint-specific attributes
        self.api_url = "https://graph.microsoft.com/v1.0"
        self.site_id = request.get("site_id", "")
        self.cursor_keyword = "@odata.nextLink"
        self.limit = 1000

    async def _refresh_token_from_service(self) -> dict:
        """
        Refresh the token by calling SharePoint's token refresh endpoint.

        Returns:
            dict: A dictionary containing the refreshed token data.
        """
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.token_manager.token_cache["refresh_token"],
            "client_id": self.token_manager.client_id,
            "client_secret": self.token_manager.client_secret,
            "scope": self.token_manager.scope,
        }

        async with self.session.post(self.token_manager.refresh_token_url, data=payload) as response:
            if response.status != 200:
                error_message = await response.text()
                raise Exception(f"Failed to refresh token for SharePoint: {response.status} - {error_message}")
            return await response.json()

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
