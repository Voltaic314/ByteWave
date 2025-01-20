from Controllers.Queue.task_queue import TaskQueue
from Controllers.Queue.worker import TraverserWorker
from Helpers.path_verifier import PathVerifier
from Helpers.file_system_trie import FileSystemTrie
import asyncio


class TraverserManager:
    def __init__(self, queue: TaskQueue, file_tree: FileSystemTrie, verifier: PathVerifier, service, directories: list, max_workers: int = 4):
        """
        Initialize the TraverserManager.

        Args:
            queue (TaskQueue): The task queue for managing traversal tasks.
            file_tree (FileSystemTrie): The trie to log paths.
            verifier (PathVerifier): Verifies folder/file validity.
            service: The service handling API/OS-level operations.
            directories (list): Root directories to traverse.
            max_workers (int): Maximum number of concurrent workers.
        """
        self.queue = queue
        self.file_tree = file_tree
        self.verifier = verifier
        self.service = service
        self.directories = directories
        self.workers = []
        self.max_workers = max_workers

    async def start(self):
        """
        Start the traversal process by initializing the queue and spawning workers.
        """
        # Fetch all `pending traversal` nodes from cache or database
        pending_nodes = self.file_tree.get_nodes_by_status("pending", status_type="traversal")

        # Add these tasks to the queue
        for node in pending_nodes:
            task = {
                "identifier": node.item.source.identifier,
                "folder": node.item,
                "trie": self.file_tree,
                "verifier": self.verifier,
                "service": self.service,
            }
            await self.queue.add_task(task)

        # Spawn workers
        for i in range(self.max_workers):
            worker = TraverserWorker(id=i, queue=self.queue)
            self.workers.append(worker)
            asyncio.create_task(worker.start())

    async def stop(self):
        """
        Stop all workers gracefully.
        """
        for worker in self.workers:
            worker.stop()
        self.workers.clear()

