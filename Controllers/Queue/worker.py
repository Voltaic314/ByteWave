import asyncio
from uuid import uuid4
from abc import ABC, abstractmethod
from Controllers.Queue.task_queue import TaskQueue
from Helpers.folder import Folder, FolderSubItem
from Helpers.file import File
from Helpers.file_system_trie import FileSystemTrie, TrieNode
from Helpers.path_verifier import PathVerifier
from Controllers.Queue.task import Task 


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
                task = await self.queue.get_next_task()
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
    async def process_task(self, task: Task):
        """
        Process a traversal task.

        Args:
            task (Task): The task containing folder information.
        """
        node_id = task.payload.get("node_id")
        trie: FileSystemTrie = task.payload.get("trie")
        service = task.payload.get("service")

        if not node_id or not trie or not service:
            print(f"Invalid task payload: {task.payload}")
            task.mark_failed()
            return

        # Initialize PathVerifier once
        verifier = PathVerifier()

        try:
            # Retrieve the node from the trie
            node = trie.node_map.get(node_id)
            if not node:
                print(f"Node {node_id} not found in trie.")
                task.mark_failed()
                return

            # Retrieve items within the folder
            items = await service.get_all_items(node.item.source)
            if not items:
                print(f"No items found in folder: {node.item.source.path}")
                await trie.update_node_status(node_id, "traversal", "failed")
                task.mark_failed()
                return

            # Process items
            for item_sub_item in items:
                item = (
                    Folder(source=item_sub_item)
                    if isinstance(item_sub_item, FolderSubItem)
                    else File(source=item_sub_item)
                )

                # Skip invalid items using the verifier
                if not verifier.is_valid_item(item.source):
                    print(f"Skipping invalid item: {item.source.path}")
                    continue

                # Add the item to the trie and generate a new node ID
                child_node_id = await trie.add_item(item, parent_id=node_id)

                # Enqueue new tasks for subfolders
                if isinstance(item, Folder):
                    new_task = Task(
                        id=uuid4().hex,
                        type="traverse",
                        payload={
                            "node_id": child_node_id,
                            "trie": trie,
                            "service": service,
                        },
                    )
                    await self.queue.add_task(new_task)

            # Mark traversal as successful
            await trie.update_node_status(node_id, "traversal", "successful")
            task.mark_completed()

        except Exception as e:
            print(f"Error traversing folder {node_id}: {e}")
            await trie.update_node_status(node_id, "traversal", "failed")
            task.mark_failed()

            
class UploaderWorker(Worker):
    async def process_task(self, task: Task):
        """
        Process an upload task.

        Args:
            task (Task): The task containing file or folder information.
        """
        node_id = task.payload.get("node_id")
        trie: FileSystemTrie = task.payload.get("trie")
        service = task.payload.get("service")

        if not node_id or not trie or not service:
            print(f"Invalid task payload: {task.payload}")
            task.mark_failed()
            return

        try:
            node = trie.node_map.get(node_id)
            if not node:
                print(f"Node {node_id} not found in trie.")
                task.mark_failed()
                return

            if node.item.type == "folder":
                await self._upload_folder(node, trie, service)
            elif node.item.type == "file":
                await self._upload_file(node, trie, service)
        except Exception as e:
            print(f"Error processing task for node {node_id}: {e}")
            await trie.update_node_status(node_id, "upload", "failed")
            task.mark_failed()

    async def _upload_folder(self, folder: TrieNode, trie: FileSystemTrie, service):
        try:
            folder_name = folder.item.source.name
            destination_id = folder.parent.item.destination.identifier
            new_folder_id = await service.create_folder(folder_name, destination_id)
            folder.item.destination = service.get_folder_sub_item(new_folder_id)
            await trie.update_node_status(folder.item.source.identifier, "upload", "successful")
        except Exception as e:
            print(f"Error creating folder {folder.item.source.name}: {e}")
            await trie.update_node_status(folder.item.source.identifier, "upload", "failed")

    async def _upload_file(self, file: TrieNode, trie: FileSystemTrie, service):
        try:
            file_name = file.item.source.name
            file_contents = await service.get_file_contents(file.item.source.identifier)
            destination_id = file.parent.item.destination.identifier
            await service.upload_file(file_name, file_contents, destination_id)
            await trie.update_node_status(file.item.source.identifier, "upload", "successful")
        except Exception as e:
            print(f"Error uploading file {file.item.source.name}: {e}")
            await trie.update_node_status(file.item.source.identifier, "upload", "failed")
