from collections import deque
import asyncio

class TaskQueue:
    """
    A thread-safe, in-memory task queue for managing tasks with FIFO behavior.
    """
    def __init__(self):
        self.queue = deque()  # Use deque for FIFO
        self.lock = asyncio.Lock()  # Ensure thread safety with asyncio
        self.task_status = {}  # Tracks the status of each task

    async def add_task(self, task_id, task_data):
        """
        Add a new task to the queue.
        
        Args:
            task_id: Unique identifier for the task.
            task_data: The data or metadata for the task.
        """
        async with self.lock:
            self.queue.append(task_id)  # Add to the end of the queue
            self.task_status[task_id] = {
                "status": "pending",  # Initial state
                "data": task_data,
                "attempts": 0
            }

    async def get_next_task(self):
        """
        Retrieve and remove the next pending task in FIFO order.
        
        Returns:
            task_id and task_data if available, else None.
        """
        async with self.lock:
            while self.queue:
                task_id = self.queue.popleft()  # FIFO: Pop from the front
                task_info = self.task_status.get(task_id)
                if task_info and task_info["status"] == "pending":
                    task_info["status"] = "in_progress"  # Mark as in progress
                    return task_id, task_info["data"]
        return None, None

    async def mark_task_completed(self, task_id):
        """
        Mark a task as completed and remove it from task_status.
        
        Args:
            task_id: Unique identifier for the task.
        """
        async with self.lock:
            if task_id in self.task_status:
                del self.task_status[task_id]  # Remove completed task

    async def mark_task_failed(self, task_id, retry_limit=3):
        """
        Mark a task as failed. Optionally requeue if retry limit isn't exceeded.
        
        Args:
            task_id: Unique identifier for the task.
            retry_limit: Maximum number of retry attempts.
        """
        async with self.lock:
            if task_id in self.task_status:
                task_info = self.task_status[task_id]
                task_info["status"] = "failed"
                task_info["attempts"] += 1

                if task_info["attempts"] < retry_limit:
                    task_info["status"] = "pending"  # Requeue the task
                    self.queue.append(task_id)  # Add back to the end of the queue
                else:
                    print(f"Task {task_id} reached max retries. Removing from queue.")
                    del self.task_status[task_id]  # Remove from tracking

    async def get_status_summary(self):
        """
        Get a summary of tasks by their status.
        
        Returns:
            A dictionary summarizing the task counts for each status.
        """
        async with self.lock:
            summary = {
                "pending": 0,
                "in_progress": 0,
                "completed": 0,
                "failed": 0,
            }
            for task in self.task_status.values():
                summary[task["status"]] += 1
            return summary
