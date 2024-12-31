import asyncio
from typing import List


class TaskQueue:
    
    def __init__(self, max_size: int = 0):
        """
        Initialize the TaskQueue.

        Args:
            max_size (int): Maximum size of the queue. 0 means no limit.
        """
        self.queue = asyncio.Queue(maxsize=max_size)
        self.task_count = 0
        self.failed_tasks = []  # Tracks tasks that fail permanently

    async def add_task(self, task: dict):
        """
        Add a task to the queue.

        Args:
            task (dict): Task to add to the queue.
        """
        await self.queue.put(task)
        self.task_count += 1

    async def get_task(self) -> dict:
        """
        Retrieve a task from the queue.

        Returns:
            dict: The next task in the queue.
        """
        return await self.queue.get()

    async def mark_task_failed(self, task: dict):
        """
        Mark a task as failed and potentially retry it.

        Args:
            task (dict): The failed task.
        """
        retries_left = task.get('retries', 0)
        if retries_left > 0:
            task['retries'] = retries_left - 1
            await self.add_task(task)  # Re-queue the task with decremented retries
        else:
            self.failed_tasks.append(task)  # Store permanently failed tasks

    def get_failed_tasks(self) -> List[dict]:
        """
        Retrieve all failed tasks.

        Returns:
            List[dict]: List of permanently failed tasks.
        """
        return self.failed_tasks

    def size(self) -> int:
        """
        Get the current size of the queue.

        Returns:
            int: The number of tasks in the queue.
        """
        return self.queue.qsize()

    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns:
            bool: True if the queue is empty, False otherwise.
        """
        return self.queue.empty()
