import asyncio
from Helpers.path_verifier import PathVerifier
from Helpers.file_system_trie import FileSystemTrie
from Helpers.folder import Folder
from Helpers.file import File


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
            failed_nodes = self.file_tree.get_failed_traversal_nodes()
            if failed_nodes:
                print(f"Resuming from failed nodes: {len(failed_nodes)} found.")
                for node in failed_nodes:
                    await self.traverse_folder(node.item)
                return

            # Traverse from configured directories if no failed nodes
            for directory in self.directories:
                await self.run_event.wait()  # Wait for the run_event before processing

                # Get the root folder object from the service
                root_folder_sub_item = self.service.get_root_directory_sub_item(directory)
                root_folder = Folder(source=root_folder_sub_item)

                if not self.verifier.is_valid_item(root_folder.source):
                    print(f"Skipping invalid directory: {directory}")
                    continue

                await self.traverse_folder(root_folder)

        except asyncio.CancelledError:
            print("Traversal cancelled.")
        finally:
            self.state = "idle"

    async def traverse_folder(self, folder: Folder):
        """
        Recursively traverse a folder and log its structure.

        Args:
            folder (Folder): The Folder object to traverse.
        """
        try:
            await self.run_event.wait()  # Wait for the run_event before proceeding

            # Add the folder to the trie
            self.file_tree.add_item(folder)

            # Retrieve items within the folder using the service
            items = await self.service.get_all_items(folder.source)
            if not items:
                print(f"Unable to retrieve items for: {folder.source.path}")
                await self.file_tree.update_node_traversal_status(folder.source.identifier, "failed")
                return

            # Process each item
            for item_sub_item in items:
                await self.run_event.wait()  # Wait for the run_event before processing

                # Create File or Folder object from the item's sub-item
                if isinstance(item_sub_item, FolderSubItem):
                    item = Folder(source=item_sub_item)
                else:
                    item = File(source=item_sub_item)

                if not self.verifier.is_valid_item(item.source):
                    print(f"Skipping invalid item: {item.source.path}")
                    continue

                self.file_tree.add_item(item)

                # Recurse into folders
                if isinstance(item, Folder):
                    await self.traverse_folder(item)

            # Mark folder as successfully processed
            await self.file_tree.update_node_traversal_status(folder.source.identifier, "successful")

        except asyncio.CancelledError:
            print(f"Traversal of folder {folder.source.path} cancelled.")
            await self.file_tree.update_node_traversal_status(folder.source.identifier, "failed")
        except Exception as e:
            print(f"Error while traversing folder {folder.source.path}: {e}")
            await self.file_tree.update_node_traversal_status(folder.source.identifier, "failed")

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
