class Uploader:
    def __init__(self, request, service, file_tree):
        """
        Initialize the Uploader with a service (OS or API subclass),
        a file tree for tracking upload state.

        Args:
            request (dict): Configuration parameters for the upload.
            service (object): The service class instance (e.g., SharePoint, Windows).
            file_tree (FileSystemTrie): The file tree instance for managing upload data.
        """
        self.service = service
        self.file_tree = file_tree

    async def upload(self, source_folder_id, destination_folder_id):
        """
        Recursively process a folder and its contents for uploading.

        Args:
            source_folder_id (str): Identifier for the source folder.
            destination_folder_id (str): Identifier for the destination folder.
        """
        try:
            items = await self.service.get_all_items(source_folder_id)
            for item in items:
                self.file_tree.update_status(item["identifier"], "in_progress")

                if item["type"] == "folder":
                    # Create a folder and process its contents
                    new_folder_id = await self._create_folder(item, destination_folder_id)
                    await self._process_folder(item["identifier"], new_folder_id)
                else:
                    # Upload a file
                    await self._upload_file(item, destination_folder_id)

                # Mark the item as successfully uploaded
                self.file_tree.update_status(item["identifier"], "completed")

        except Exception as e:
            # Log the failure and mark the item as failed
            print(f"Error processing folder {source_folder_id}: {str(e)}")
            self.file_tree.update_status(source_folder_id, "failed")

    async def _create_folder(self, folder_metadata, destination_folder_id):
        """
        Create a folder in the destination service.

        Args:
            folder_metadata (dict): Metadata for the folder being created.
            destination_folder_id (str): Identifier for the parent folder in the destination service.

        Returns:
            str: Identifier of the newly created folder.
        """
        try:
            folder_name = folder_metadata["name"]
            return await self.service.create_folder(folder_name, destination_folder_id)
        except Exception as e:
            print(f"Error creating folder '{folder_metadata['name']}': {str(e)}")
            self.file_tree.update_status(folder_metadata["identifier"], "failed")
            return None

    async def _upload_file(self, file_metadata, destination_folder_id):
        """
        Upload a file to the destination service.

        Args:
            file_metadata (dict): Metadata for the file being uploaded.
            destination_folder_id (str): Identifier for the parent folder in the destination service.
        """
        try:
            file_name = file_metadata["name"]
            file_contents = await self.service.get_file_contents(file_metadata["identifier"])
            await self.service.upload_file(file_name, file_contents, destination_folder_id)
        except Exception as e:
            print(f"Error uploading file '{file_metadata['name']}': {str(e)}")
            self.file_tree.update_status(file_metadata["identifier"], "failed")
