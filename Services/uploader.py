import asyncio
from Helpers.file import File
from Helpers.folder import Folder
from Helpers.file_system_trie import FileSystemTrie


class Uploader:
    def __init__(self, request, service, file_tree: FileSystemTrie):
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
        self.run_event = asyncio.Event()  # Event for pause/resume control
        self.run_event.set()  # Initially allow uploading to proceed
        self.state = "idle"  # Current state of the uploader

    async def upload(self, source_folder: Folder, destination_folder: Folder):
        """
        Recursively process a folder and its contents for uploading.

        Args:
            source_folder (Folder): Source folder object.
            destination_folder (Folder): Destination folder object.
        """
        self.state = "running"
        try:
            await self._process_folder(source_folder, destination_folder)
        except asyncio.CancelledError:
            print("Upload cancelled.")
        finally:
            self.state = "idle"

    async def _process_folder(self, source_folder: Folder, destination_folder: Folder):
        """
        Process the contents of a folder and upload them.

        Args:
            source_folder (Folder): Source folder object.
            destination_folder (Folder): Destination folder object.
        """
        try:
            # Get items in the source folder
            items = await self.service.get_all_items(source_folder.source)
            for item in items:
                await self.run_event.wait()  # Wait for the run_event before processing

                # Create a File or Folder object
                node = File(source=item) if item.type == "file" else Folder(source=item)
                self.file_tree.update_node_upload_status(node.source.identifier, "in_progress")

                if isinstance(node, Folder):
                    # Create a folder in the destination and process its contents
                    new_destination = await self._create_folder(node, destination_folder)
                    if new_destination:
                        await self._process_folder(node, new_destination)
                else:
                    # Upload a file
                    await self._upload_file(node, destination_folder)

                # Mark the item as successfully uploaded
                self.file_tree.update_node_upload_status(node.source.identifier, "successful")

        except asyncio.CancelledError:
            print(f"Upload of folder {source_folder.source.identifier} cancelled.")
        except Exception as e:
            print(f"Error processing folder {source_folder.source.identifier}: {e}")
            self.file_tree.update_node_upload_status(source_folder.source.identifier, "failed")

    async def _create_folder(self, folder: Folder, destination_folder: Folder):
        """
        Create a folder in the destination service.

        Args:
            folder (Folder): Folder object to create.
            destination_folder (Folder): Parent folder in the destination service.

        Returns:
            Folder: Newly created destination folder object.
        """
        try:
            folder_name = folder.source.name
            destination_id = destination_folder.destination.identifier
            new_folder_id = await self.service.create_folder(folder_name, destination_id)
            return Folder(destination=self.service.get_folder_sub_item(new_folder_id))
        except Exception as e:
            print(f"Error creating folder '{folder.source.name}': {e}")
            self.file_tree.update_node_upload_status(folder.source.identifier, "failed")
            return None

    async def _upload_file(self, file: File, destination_folder: Folder):
        """
        Upload a file to the destination service.

        Args:
            file (File): File object to upload.
            destination_folder (Folder): Parent folder in the destination service.
        """
        try:
            file_name = file.source.name
            file_contents = await self.service.get_file_contents(file.source.identifier)
            destination_id = destination_folder.destination.identifier
            await self.service.upload_file(file_name, file_contents, destination_id)
        except Exception as e:
            print(f"Error uploading file '{file.source.name}': {e}")
            self.file_tree.update_node_upload_status(file.source.identifier, "failed")

    async def pause(self):
        """Pause the upload process."""
        print("Pausing upload...")
        self.run_event.clear()
        self.state = "paused"

    async def resume(self):
        """Resume the upload process."""
        print("Resuming upload...")
        self.run_event.set()
        self.state = "running"

    async def stop(self):
        """Stop the upload process completely."""
        print("Stopping upload...")
        self.run_event.clear()
        self.state = "stopped"
