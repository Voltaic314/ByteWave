import asyncio
from Services.traverser import Traverser
from Services.uploader import Uploader


class Migrator:
    """
    The Migrator class acts as a controller for managing the traversal and upload processes.
    It coordinates the Traverser and Uploader, tracks states, and provides control methods.
    """

    def __init__(self, traverser: Traverser, uploader: Uploader):
        """
        Initialize the Migrator.

        Args:
            traverser (Traverser): Instance of the Traverser class.
            uploader (Uploader): Instance of the Uploader class.
        """
        self.traverser = traverser
        self.uploader = uploader
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
            self.current_task = asyncio.create_task(self.traverser.traverse())
            await self.current_task
            print("Traversal completed successfully.")
            self.state = "idle"
        except asyncio.CancelledError:
            print("Traversal cancelled.")
            self.state = "stopped"
        except Exception as e:
            print(f"Error during traversal: {e}")
            self.state = "stopped"

    async def start_upload(self, source_folder_id, destination_folder_id):
        """
        Start the upload process.

        Args:
            source_folder_id (str): Identifier for the source folder.
            destination_folder_id (str): Identifier for the destination folder.
        """
        if self.state not in ["idle", "paused"]:
            print(f"Cannot start upload. Current state: {self.state}")
            return

        self.state = "uploading"
        self.mode = "upload"

        try:
            print("Starting upload...")
            self.current_task = asyncio.create_task(self.uploader.upload(source_folder_id, destination_folder_id))
            await self.current_task
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
        await self.traverser.pause()
        await self.uploader.pause()
        self.state = "paused"

    async def resume(self):
        """
        Resume the paused operation.
        """
        if self.state != "paused":
            print(f"Cannot resume. Current state: {self.state}")
            return

        print("Resuming operation...")
        await self.traverser.resume()
        await self.uploader.resume()
        self.state = self.mode

    async def stop(self):
        """
        Stop the current operation entirely.
        """
        if self.state in ["idle", "stopped"]:
            print(f"Cannot stop. Current state: {self.state}")
            return

        print("Stopping current operation...")
        if self.current_task:
            self.current_task.cancel()
        await self.traverser.stop()
        await self.uploader.stop()
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
            "traverser_state": self.traverser.state,
            "uploader_state": self.uploader.state,
        }
