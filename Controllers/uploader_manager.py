from Controllers.Queue.task_queue import TaskQueue
from Controllers.Queue.worker import UploaderWorker
from Helpers.file_system_trie import FileSystemTrie
import asyncio


class UploaderManager:
    def __init__(self, queue: TaskQueue, file_tree: FileSystemTrie, service, max_workers: int = 4):
        """
        Initialize the UploaderManager.

        Args:
            queue (TaskQueue): The task queue for managing upload tasks.
            file_tree (FileSystemTrie): The trie to log paths.
            service: The service handling API/OS-level operations.
            max_workers (int): Maximum number of concurrent workers.
        """
        self.queue = queue
        self.file_tree = file_tree
        self.service = service
        self.workers = []
        self.max_workers = max_workers

    async def start(self):
        """
        Start the upload process by initializing the queue and spawning workers.
        """
        # Fetch all `pending upload` nodes from cache or database
        pending_nodes = self.file_tree.get_nodes_by_status("pending", status_type="upload")

        # Add these tasks to the queue
        for node in pending_nodes:
            task = {"trie": self.file_tree, "service": self.service, "node": node}
            await self.queue.add_task(task)

        # Spawn workers
        for i in range(self.max_workers):
            worker = UploaderWorker(id=i, queue=self.queue)
            self.workers.append(worker)
            asyncio.create_task(worker.start())

    async def stop(self):
        """
        Stop all workers gracefully.
        """
        for worker in self.workers:
            worker.stop()
        self.workers.clear()
