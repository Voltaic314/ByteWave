import asyncio
from Controllers.traverser_manager import TraverserManager
from Controllers.uploader_manager import UploaderManager


class Migrator:
    """
    The Migrator class acts as a controller for managing the traversal and upload processes.
    It coordinates the TraverserManager and UploaderManager, tracks states, and provides control methods.
    """

    def __init__(self, traverser_manager: TraverserManager, uploader_manager: UploaderManager):
        """
        Initialize the Migrator.

        Args:
            traverser_manager (TraverserManager): Instance of the TraverserManager class.
            uploader_manager (UploaderManager): Instance of the UploaderManager class.
        """
        self.traverser_manager = traverser_manager
        self.uploader_manager = uploader_manager
        self.state = "idle"  # idle, traversing, uploading, paused, stopped
        self.mode = None  # "traverse" or "upload"
        self.current_task = None  # Tracks the active asyncio task

    async def start_traversal(self):
        """
        Start the traversal process.
        """
        if self.state not in ["idle", "paused"]:
            print(f"Cannot start traversal. Current state: {self.state}")
            return

        self.state = "traversing"
        self.mode = "traverse"

        try:
            print("Starting traversal...")
            await self.traverser_manager.start()
            print("Traversal completed successfully.")
            self.state = "idle"
        except asyncio.CancelledError:
            print("Traversal cancelled.")
            self.state = "stopped"
        except Exception as e:
            print(f"Error during traversal: {e}")
            self.state = "stopped"

    async def start_upload(self):
        """
        Start the upload process.
        """
        if self.state not in ["idle", "paused"]:
            print(f"Cannot start upload. Current state: {self.state}")
            return

        self.state = "uploading"
        self.mode = "upload"

        try:
            print("Starting upload...")
            await self.uploader_manager.start()
            print("Upload completed successfully.")
            self.state = "idle"
        except asyncio.CancelledError:
            print("Upload cancelled.")
            self.state = "stopped"
        except Exception as e:
            print(f"Error during upload: {e}")
            self.state = "stopped"

    async def pause(self):
        """
        Pause the current operation (traversal or upload).
        """
        if self.state not in ["traversing", "uploading"]:
            print(f"Cannot pause. Current state: {self.state}")
            return

        print("Pausing current operation...")
        self.state = "paused"

        # Pausing logic will depend on the ability to halt workers or manage task queues
        # This can be extended to include specific worker/task-level pausing logic.

    async def resume(self):
        """
        Resume the paused operation.
        """
        if self.state != "paused":
            print(f"Cannot resume. Current state: {self.state}")
            return

        print("Resuming operation...")
        if self.mode == "traverse":
            await self.start_traversal()
        elif self.mode == "upload":
            await self.start_upload()

    async def stop(self):
        """
        Stop the current operation entirely.
        """
        if self.state in ["idle", "stopped"]:
            print(f"Cannot stop. Current state: {self.state}")
            return

        print("Stopping current operation...")
        if self.mode == "traverse":
            await self.traverser_manager.stop()
        elif self.mode == "upload":
            await self.uploader_manager.stop()

        self.state = "stopped"

    def get_status(self):
        """
        Get the current status of the migration process.

        Returns:
            dict: A dictionary containing the current state, mode, and progress.
        """
        return {
            "state": self.state,
            "mode": self.mode,
            "workers_traversing": len(self.traverser_manager.workers),
            "workers_uploading": len(self.uploader_manager.workers),
        }
