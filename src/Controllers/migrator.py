import asyncio
from src import Response, Error, Warning
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

    async def start_traversal(self):
        """
        Start the traversal process.

        Returns:
            Response: Success or failure with appropriate errors.
        """
        if self.state not in ["idle", "paused"]:
            return Response(
                success=False,
                warnings=[Warning("StateWarning", "Cannot start traversal unless in idle or paused state.", {"current_state": self.state})]
            )

        self.state = "traversing"
        self.mode = "traverse"

        try:
            response = await self.traverser_manager.start()
            if response.success:
                self.state = "idle"
            return response
        except asyncio.CancelledError:
            self.state = "stopped"
            return Response(success=False, errors=[Error("TaskCancelled", "Traversal process was cancelled.")])
        except Exception as e:
            self.state = "stopped"
            return Response(
                success=False,
                errors=[Error("TraversalError", "Error during traversal process.", details=str(e))]
            )

    async def start_upload(self):
        """
        Start the upload process.

        Returns:
            Response: Success or failure with appropriate errors.
        """
        if self.state not in ["idle", "paused"]:
            return Response(
                success=False,
                warnings=[Warning("StateWarning", "Cannot start upload unless in idle or paused state.", {"current_state": self.state})]
            )

        self.state = "uploading"
        self.mode = "upload"

        try:
            response = await self.uploader_manager.start()
            if response.success:
                self.state = "idle"
            return response
        except asyncio.CancelledError:
            self.state = "stopped"
            return Response(success=False, errors=[Error("TaskCancelled", "Upload process was cancelled.")])
        except Exception as e:
            self.state = "stopped"
            return Response(
                success=False,
                errors=[Error("UploadError", "Error during upload process.", details=str(e))]
            )

    async def pause(self):
        """
        Pause the current operation (traversal or upload).

        Returns:
            Response: Success or failure with warnings if applicable.
        """
        if self.state not in ["traversing", "uploading"]:
            return Response(
                success=False,
                warnings=[Warning("StateWarning", "Cannot pause unless actively traversing or uploading.", {"current_state": self.state})]
            )

        self.state = "paused"
        return Response(success=True, response="Operation paused successfully.")

    async def resume(self):
        """
        Resume the paused operation.

        Returns:
            Response: Success or failure with warnings if applicable.
        """
        if self.state != "paused":
            return Response(
                success=False,
                warnings=[Warning("StateWarning", "Cannot resume unless paused.", {"current_state": self.state})]
            )

        if self.mode == "traverse":
            return await self.traverser_manager.resume()
        elif self.mode == "upload":
            return await self.uploader_manager.resume()
        else:
            return Response(
                success=False,
                errors=[Error("InvalidState", "Resume failed because no valid mode was set.")]
            )

    async def stop(self):
        """
        Stop the current operation entirely.

        Returns:
            Response: Success or failure with warnings if applicable.
        """
        if self.state in ["idle", "stopped"]:
            return Response(
                success=False,
                warnings=[Warning("StateWarning", "Cannot stop unless actively traversing or uploading.", {"current_state": self.state})]
            )

        response = None
        if self.mode == "traverse":
            response = await self.traverser_manager.stop()
        elif self.mode == "upload":
            response = await self.uploader_manager.stop()

        if response and response.success:
            self.state = "idle"

        return response if response else Response(success=False, errors=[Error("StopFailure", "Failed to stop operation.")])

    def get_status(self):
        """
        Get the current status of the migration process.

        Returns:
            Response: Success with migration state details.
        """
        return Response(success=True, response={
            "state": self.state,
            "mode": self.mode,
            "workers_traversing": len(self.traverser_manager.workers),
            "workers_uploading": len(self.uploader_manager.workers),
        })
