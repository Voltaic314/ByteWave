class Task:
    def __init__(self, id: str, type: str, payload: dict, retries: int = 3):
        """
        Initialize a Task.

        Args:
            id (str): Unique identifier for the task.
            type (str): Type of task ('traverse' or 'upload').
            payload (dict): Task-specific data.
            retries (int): Number of retries allowed.
        """
        self.id = id
        self.type = type
        self.payload = payload
        self.retries = retries
        self.status = "pending"

    def mark_failed(self):
        """
        Mark the task as failed and decrement retries.
        """
        self.retries -= 1
        self.status = "failed" if self.retries <= 0 else "pending"

    def mark_completed(self):
        """
        Mark the task as completed.
        """
        self.status = "completed"

    def to_dict(self):
        """
        Serialize the task to a dictionary.
        """
        return {
            "id": self.id,
            "type": self.type,
            "payload": self.payload,
            "retries": self.retries,
            "status": self.status,
        }
