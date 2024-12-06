import asyncio
from Helpers.path_verifier import PathVerifier
from Helpers.file_system_trie import FileSystemTrie


class Traverser:
    """
    Traverser handles directory traversal, applies filtering rules,
    and logs the resulting structure to a file system trie.
    """

    def __init__(self, request: dict, service, file_tree: FileSystemTrie):
        """
        Initialize the Traverser class.

        Args:
            request (dict): Configuration parameters for traversal.
            service: The service class handling API/OS-level operations.
            file_tree: FileSystemTrie instance for logging paths.
        """
        self.file_tree = file_tree  # FileSystemTrie instance for managing paths
        self.verifier = PathVerifier(request)  # Path verifier for filtering logic
        self.service = service  # Service class for API/OS operations

        # Directories to traverse, cleaned for redundancy
        self.directories = self.filter_redundant_paths(
            [d.replace("\\", "/").strip("/") for d in request.get("directories", [])]
        )

        self.run_event = asyncio.Event()  # Event for pause/resume control
        self.run_event.set()  # Initially allow traversal to proceed
        self.state = "idle"  # Current state of the traverser

    @staticmethod
    def filter_redundant_paths(directories):
        """Remove redundant paths to optimize traversal."""
        if not directories or len(directories) == 1:
            return directories
        sorted_paths = sorted(directories, key=len)
        filtered_paths = []
        for path in sorted_paths:
            if not any(path.startswith(p + "/") for p in filtered_paths):
                filtered_paths.append(path)
        return filtered_paths

    async def traverse(self):
        """
        Start traversal for all configured directories or resume from failed nodes.
        """
        self.state = "running"
        try:
            # Resume from failed nodes, if any
            failed_nodes = self.file_tree.get_failed_nodes()
            if failed_nodes:
                print(f"Resuming from failed nodes: {len(failed_nodes)} found.")
                for node in failed_nodes:
                    await self.traverse_folder(node.item.to_dict())
                return

            # Traverse from configured directories if no failed nodes
            for directory in self.directories:
                await self.run_event.wait()  # Wait for the run_event before processing

                # Start traversal at the target directory
                root_metadata = {
                    "path": directory,
                    "name": directory.split("/")[-1],
                    "type": "folder",
                }
                if not self.verifier.is_valid_item(root_metadata):
                    print(f"Skipping directory due to invalid path: {directory}")
                    continue

                await self.traverse_folder(root_metadata)

        except asyncio.CancelledError:
            print("Traversal cancelled.")
        finally:
            self.state = "idle"

    async def traverse_folder(self, folder_metadata: dict):
        """
        Recursively traverse a folder and log its structure.

        Args:
            folder_metadata (dict): Metadata of the folder to traverse.
        """
        try:
            await self.run_event.wait()  # Wait for the run_event before proceeding

            # Add the folder to the trie
            await self.file_tree.insert_path(folder_metadata)

            # Retrieve items within the folder using the service
            items = await self.service.get_all_items(folder_metadata)
            if not items:
                print(f"Unable to retrieve items for: {folder_metadata['path']}")
                await self.file_tree.update_node_status(folder_metadata["identifier"], "failed")
                return

            # Process each item
            for item_metadata in items:
                await self.run_event.wait()  # Wait for the run_event before processing
                if not self.verifier.is_valid_item(item_metadata):
                    print(f"Skipping invalid item: {item_metadata['path']}")
                    continue

                await self.file_tree.insert_path(item_metadata)

                # Recurse into folders
                if item_metadata["type"] == "folder":
                    await self.traverse_folder(item_metadata)

            # Mark folder as successfully processed
            await self.file_tree.update_node_status(folder_metadata["identifier"], "successful")

        except asyncio.CancelledError:
            print(f"Traversal of folder {folder_metadata['path']} cancelled.")
            await self.file_tree.update_node_status(folder_metadata["identifier"], "failed")
        except Exception as e:
            print(f"Error while traversing folder {folder_metadata['path']}: {e}")
            await self.file_tree.update_node_status(folder_metadata["identifier"], "failed")

    async def pause(self):
        """Pause the traversal."""
        print("Pausing traversal...")
        self.run_event.clear()
        self.state = "paused"

    async def resume(self):
        """Resume the traversal."""
        print("Resuming traversal...")
        self.run_event.set()
        self.state = "running"

    async def stop(self):
        """Stop the traversal completely."""
        print("Stopping traversal...")
        self.run_event.clear()
        self.state = "stopped"
