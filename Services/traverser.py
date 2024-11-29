from Helpers.path_verifier import PathVerifier
from Helpers.path_helper import PathHelper

class Traverser:
    """
    Traverser handles directory traversal, applies filtering rules,
    and logs the resulting structure to a file system trie.
    """

    def __init__(self, request: dict, service, file_tree):
        """
        Initialize the Traverser class.

        Args:
            request (dict): Configuration parameters for traversal.
            service: The service class handling API or OS-level operations.
        """
        self.file_tree = file_tree # FileSystemTrie instance for logging paths
        self.verifier = PathVerifier(request)  # Path verifier for filtering logic
        self.service = service  # Service class for API/OS operations
        self.path_helper = PathHelper()  # Path helper for building paths

        # Directories to traverse, cleaned for redundancy
        self.directories = self.filter_redundant_paths(
            [d.replace("\\", "/").strip("/") for d in request.get("directories", [])]
        )

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
        Start traversal for all configured directories.
        Builds intermediate paths and initiates traversal for each directory.
        """
        for directory in self.directories:
            # Split the directory into parts and build intermediate paths
            parts = directory.split("/")
            path_so_far = ""
            for index, part in enumerate(parts):
                path_so_far = f"{path_so_far}/{part}".strip("/")
                intermediate_metadata = {
                    "path": path_so_far,
                    "name": part,
                    "type": "folder",
                }
                if not self.verifier.is_valid_item(intermediate_metadata):
                    print(f"Skipping invalid intermediate path: {path_so_far}")
                    break
                await self.file_tree.insert_path(intermediate_metadata)

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

    async def traverse_folder(self, folder_metadata: dict):
        """
        Recursively traverse a folder and log its structure.

        Args:
            folder_metadata (dict): Metadata of the folder to traverse.
        """
        try:
            # Add the folder to the trie
            await self.file_tree.insert_path(folder_metadata)

            # Push folder metadata to path helper
            self.path_helper.add(folder_metadata)

            # Retrieve items within the folder using the service
            items = await self.service.get_all_items(folder_metadata)
            if not items:
                print(f"Unable to retrieve items for: {folder_metadata['path']}")
                self.path_helper.remove()  # Remove folder from path helper
                return

            # Process each item
            for item_metadata in items:
                # Construct full path for each item using the path helper
                item_metadata["path"] = f"{self.path_helper.get_current_path()}/{item_metadata['name']}"
                if not self.verifier.is_valid_item(item_metadata):
                    print(f"Skipping invalid item: {item_metadata['path']}")
                    continue

                await self.file_tree.insert_path(item_metadata)

                # Recurse into folders
                if item_metadata["type"] == "folder":
                    await self.traverse_folder(item_metadata)

            # Pop folder metadata from path helper when done
            self.path_helper.remove()

        except Exception as e:
            print(f"Error while traversing folder {folder_metadata['path']}: {e}")
            self.path_helper.remove()
