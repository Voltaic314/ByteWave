import asyncio
from src import Response
from Controllers.traverser_manager import TraverserManager
from Controllers.uploader_manager import UploaderManager


## TODO: This code seems inconsistent. We should take a close look at it and fix it.
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
            response = Response(success=False)
            response.add_warning(
                warning_type="StateWarning", 
                message=f"Cannot start traversal unless in idle or paused state.", 
                metadata={"current_state": self.state}
            )
            return response

        self.state = "traversing"
        self.mode = "traverse"

        try:
            await self.traverser_manager.start()
            self.state = "idle"
            return Response(success=True, message="Traversal process started successfully.")
        except asyncio.CancelledError:
            self.state = "stopped"
            return Response(success=False, message="Traversal process cancelled.")
        except Exception as e:
            self.state = "stopped"
            response = Response(success=False)
            response.add_error(
                error_type="StateChangeError",
                message="Error starting the traversal process.",
                details=str(e)
            )
            return response


    async def start_upload(self):
        """
        Start the upload process.
        """
        if self.state not in ["idle", "paused"]:
            return Response(success=False, message=f"Cannot start upload unless in idle or paused state. Current state: {self.state}")

        self.state = "uploading"
        self.mode = "upload"

        try:
            await self.uploader_manager.start()
            self.state = "idle"
            return Response(success=True, message="Upload process started successfully.")
        except asyncio.CancelledError:
            self.state = "stopped"
            return Response(success=False, message="Upload process cancelled.")
        except Exception as e:
            print(f"Error during upload: {e}")
            self.state = "stopped"
            response = Response(success=False)
            response.add_error(
                error_type="StateChangeError",
                message="Error starting the upload process.",
                details=str(e)
            )
            return response

    async def pause(self):
        """
        Pause the current operation (traversal or upload).
        """
        if self.state not in ["traversing", "uploading"]:
            response = Response(success=False)
            response.add_warning(
                warning_type="StateWarning", 
                message=f"Cannot pause unless traversing or uploading.", 
                metadata={"current_state": self.state}
            )
            return response

        print("Pausing current operation...")
        self.state = "paused"

        # Pausing logic will depend on the ability to halt workers or manage task queues
        # This can be extended to include specific worker/task-level pausing logic.
        return Response(success=True, message="Operation paused successfully.")

    async def resume(self):
        """
        Resume the paused operation.
        """
        if self.state != "paused":
            response = Response(success=False)
            response.add_warning(
                warning_type="StateWarning", 
                message=f"Cannot resume unless paused.", 
                metadata={"current_state": self.state}
            )
            return response

        if self.mode == "traverse":
            return await self.traverser_manager.resume()
        elif self.mode == "upload":
            return await self.uploader_manager.resume()

    async def stop(self):
        """
        Stop the current operation entirely.
        """
        if self.state in ["idle", "stopped"]:
            response = Response(success=False)
            response.add_warning(
                warning_type="StateWarning", 
                message=f"Cannot stop unless traversing or uploading.", 
                metadata={"current_state": self.state}
            )
            return response

        if self.mode == "traverse":
            response = await self.traverser_manager.stop()
            if response.success:
                self.state = "idle"
        
        elif self.mode == "upload":
            response = await self.uploader_manager.stop()
            if response.success:
                self.state = "idle"
        
        return Response(success=True, message="Operation stopped successfully.")

    def get_status(self):
        """
        Get the current status of the migration process.

        Returns:
            dict: A dictionary containing the current state, mode, and progress.
        """
        return Response(success=True, response={
            "state": self.state,
            "mode": self.mode,
            "workers_traversing": len(self.traverser_manager.workers),
            "workers_uploading": len(self.uploader_manager.workers),
        })
