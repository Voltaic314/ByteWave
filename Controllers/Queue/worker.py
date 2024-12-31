import asyncio
from abc import ABC, abstractmethod
from Controllers.Queue.task_queue import TaskQueue
from Helpers.folder import Folder, FolderSubItem
from Helpers.file import File, FileSubItem
from Helpers.file_system_trie import FileSystemTrie
from Helpers.path_verifier import PathVerifier


class Worker(ABC):
    def __init__(self, id: int, queue: TaskQueue):
        """
        Initialize a Worker.

        Args:
            id (int): Worker ID for logging.
            queue (TaskQueue): The task queue to process tasks from.
        """
        self.id = id
        self.queue = queue
        self.running = True

    async def start(self):
        """
        Start processing tasks from the queue.
        """
        while self.running:
            try:
                task = await self.queue.get_task()
                await self.process_task(task)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Worker {self.id} encountered an error: {e}")

    @abstractmethod
    async def process_task(self, task: dict):
        """
        Abstract method to process a task.

        Args:
            task (dict): The task to process.
        """
        pass

    def stop(self):
        """
        Stop the worker gracefully.
        """
        self.running = False


class TraverserWorker(Worker):
    async def process_task(self, task: dict):
        """
        Process a traversal task.

        Args:
            task (dict): The task containing folder information.
        """
        identifier = task.get("identifier")
        folder: Folder = task.get("folder")  # Folder object for this task
        trie: FileSystemTrie = task.get("trie")  # FileSystemTrie instance
        verifier: PathVerifier = task.get("verifier")  # PathVerifier instance
        service = task.get("service")  # Service class for API/OS operations

        if not folder or not trie or not verifier or not service:
            print(f"Invalid task: {task}")
            return

        try:
            # Add the folder to the trie
            trie.add_item(folder)

            # Retrieve items within the folder using the service
            items = await service.get_all_items(folder.source)
            if not items:
                print(f"Unable to retrieve items for: {folder.source.path}")
                await trie.update_node_traversal_status(folder.source.identifier, "failed")
                return

            # Process each item
            for item_sub_item in items:
                # Create File or Folder object from the item's sub-item
                if isinstance(item_sub_item, FolderSubItem):
                    item = Folder(source=item_sub_item)
                else:
                    item = File(source=item_sub_item)

                # Skip invalid items
                if not verifier.is_valid_item(item.source):
                    print(f"Skipping invalid item: {item.source.path}")
                    continue

                # Add the item to the trie
                trie.add_item(item)

                # Add subfolders as new tasks
                if isinstance(item, Folder):
                    new_task = {
                        "identifier": item.source.identifier,
                        "folder": item,
                        "trie": trie,
                        "verifier": verifier,
                        "service": service,
                    }
                    await self.queue.add_task(new_task)

            # Mark folder as successfully processed
            await trie.update_node_traversal_status(folder.source.identifier, "successful")

        except asyncio.CancelledError:
            print(f"Traversal of folder {folder.source.path} cancelled.")
            await trie.update_node_traversal_status(folder.source.identifier, "failed")
        except Exception as e:
            print(f"Error while traversing folder {folder.source.path}: {e}")
            await trie.update_node_traversal_status(folder.source.identifier, "failed")


class UploaderWorker(Worker):
    async def process_task(self, task: dict):
        """
        Process an upload task.

        Args:
            task (dict): The task containing file or folder information.
        """
        trie: FileSystemTrie = task.get("trie")  # FileSystemTrie instance
        service = task.get("service")  # Service instance
        node = task.get("node")  # File or Folder node from the trie

        if not trie or not service or not node:
            print(f"Invalid task: {task}")
            return

        try:
            if isinstance(node, Folder):
                await self._upload_folder(node, trie, service)
            elif isinstance(node, File):
                await self._upload_file(node, trie, service)
        except asyncio.CancelledError:
            print(f"Task cancelled for {node.source.identifier}.")
            trie.update_node_upload_status(node.source.identifier, "failed")
        except Exception as e:
            print(f"Error during task for {node.source.identifier}: {e}")
            trie.update_node_upload_status(node.source.identifier, "failed")

    async def _upload_folder(self, folder: Folder, trie: FileSystemTrie, service):
        """
        Upload a folder by creating it in the destination service.

        Args:
            folder (Folder): The folder to upload.
            trie (FileSystemTrie): Trie instance for status updates.
            service: Service instance for folder creation.
        """
        try:
            folder_name = folder.source.name
            destination_id = folder.parent.destination.identifier
            new_folder_id = await service.create_folder(folder_name, destination_id)
            folder.destination = service.get_folder_sub_item(new_folder_id)
            trie.update_node_upload_status(folder.source.identifier, "successful")
        except Exception as e:
            print(f"Error creating folder '{folder.source.name}': {e}")
            trie.update_node_upload_status(folder.source.identifier, "failed")

    async def _upload_file(self, file: File, trie: FileSystemTrie, service):
        """
        Upload a file to the destination service.

        Args:
            file (File): The file to upload.
            trie (FileSystemTrie): Trie instance for status updates.
            service: Service instance for file uploads.
        """
        try:
            file_name = file.source.name
            file_contents = await service.get_file_contents(file.source.identifier)
            destination_id = file.parent.destination.identifier
            await service.upload_file(file_name, file_contents, destination_id)
            trie.update_node_upload_status(file.source.identifier, "successful")
        except Exception as e:
            print(f"Error uploading file '{file.source.name}': {e}")
            trie.update_node_upload_status(file.source.identifier, "failed")

