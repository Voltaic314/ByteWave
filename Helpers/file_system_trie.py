from Helpers.db import DB
from Helpers.identifier_set import IdentifierSet


class TrieNode:
    """
    Represents a node in the Trie structure with metadata about a file or folder.
    """
    def __init__(self, name, identifier=None, is_folder=True, parent=None):
        self.name = name
        self.identifier = identifier
        self.is_folder = is_folder
        self.children = {}
        self.parent = parent

    def add_child(self, name, identifier=None, is_folder=True):
        if name not in self.children:
            self.children[name] = TrieNode(name, identifier, is_folder, parent=self)
        return self.children[name]

    def get_full_path(self):
        """
        Construct and return the full path for this node by walking up the tree to the root.
        """
        parts = []
        current = self
        while current:
            parts.insert(0, current.name)
            current = current.parent
        return "/".join(parts).strip("/")


class FileSystemTrie:
    """
    A Trie data structure for managing hierarchical paths for file and folder creation.
    Integrates async MongoDB operations and identifier management.
    """
    def __init__(self, db: DB):
        self.db = db  # Inject the database dependency
        self.identifier_set = IdentifierSet(db)  # Load identifiers from the DB
        self.root = TrieNode("root")  # Root node for the Trie
        self.new_identifiers = []  # Buffer for batch inserts to MongoDB
        self.flush_threshold = 50  # Batch size for flushing
        self.status_update_queue = []  # Queue for status updates

    async def insert_path(self, metadata):
        """
        Insert a path into the Trie, queuing entries for MongoDB flush.
        """
        name = metadata.get("name", "")
        identifier = metadata.get("identifier", "")
        is_folder = metadata.get("type", "folder") == "folder"

        # Skip if the identifier already exists in the IdentifierSet
        if self.identifier_set.check_if_exists(identifier):
            return

        # Traverse and insert each component from path_metadata
        parts = metadata.get("path", "").strip("/").split("/")
        current = self.root

        for part in parts:
            current = current.add_child(part, identifier, is_folder)

        # Add to the batch queue and IdentifierSet
        self.new_identifiers.append(metadata)
        self.identifier_set.add(identifier)

        # Flush to DB if the threshold is reached
        if len(self.new_identifiers) >= self.flush_threshold:
            await self.flush_to_db()

    async def flush_to_db(self):
        """
        Batch insert new identifiers to MongoDB and clear the buffer.
        """
        if self.new_identifiers:
            folders = [entry["identifier"] for entry in self.new_identifiers if entry["type"] == "folder"]
            files = [entry["identifier"] for entry in self.new_identifiers if entry["type"] == "file"]

            # Batch update identifiers to DB via IdentifierSet
            if folders:
                self.identifier_set.batch_update_to_db(self.db, folders, is_folder=True)
            if files:
                self.identifier_set.batch_update_to_db(self.db, files, is_folder=False)

            print(f"Inserted {len(self.new_identifiers)} new nodes into MongoDB.")
            self.new_identifiers.clear()

    async def finalize(self):
        """
        Force a final flush to MongoDB to log any remaining nodes in the batch.
        """
        await self.flush_to_db()
        await self.flush_status_updates()

    async def get_children(self, identifier):
        """
        Retrieve children of the node specified by the identifier.

        Args:
            identifier (str): The identifier of the node whose children we want to retrieve.

        Returns:
            List[dict]: A list of dictionaries containing metadata for each child node.
        """
        def find_node_by_identifier(node, identifier):
            """Recursive helper to find the node with the specified identifier."""
            if node.identifier == identifier:
                return node
            for child in node.children.values():
                result = find_node_by_identifier(child, identifier)
                if result:
                    return result
            return None

        # Start search from the root to find the target node by its identifier
        target_node = find_node_by_identifier(self.root, identifier)
        if not target_node:
            return []

        # Collect metadata for each child of the target node
        return [
            {
                "name": child.name,
                "identifier": child.identifier,
                "type": "folder" if child.is_folder else "file",
                "path": child.get_full_path()
            }
            for child in target_node.children.values()
        ]

    async def check_path(self, path: str) -> bool:
        """
        Check if a path exists in the Trie by matching path components.
        """
        parts = path.strip("/").split("/")
        current = self.root

        for part in parts:
            if part in current.children:
                current = current.children[part]
            else:
                return False
        return True

    async def update_status(self, identifier: str, new_status: str):
        """
        Queue a status update for the given identifier.

        Args:
            identifier (str): The identifier of the node to update.
            new_status (str): The new status to set for the node.

        Returns:
            None
        """
        # Add the status update to the queue
        self.status_update_queue.append({"identifier": identifier, "status": new_status})

        # Flush the queue if the threshold is exceeded
        if len(self.status_update_queue) >= self.flush_threshold:
            await self.flush_status_updates()

    async def flush_status_updates(self):
        """
        Flush all queued status updates to the database in a single batch operation.

        Returns:
            None
        """
        if self.status_update_queue:
            bulk_operations = []
            for update in self.status_update_queue:
                identifier = update["identifier"]
                new_status = update["status"]
                
                # Prepare a single update operation
                bulk_operations.append(
                    {
                        "filter": {"identifier": identifier},
                        "update": {"$set": {"status": new_status}},
                        "upsert": True  # Ensures entries are created if not present
                    }
                )

            # Perform batch update using the `DB` class
            await self.db.update_entries(
                collection_name="folders",
                bulk_operations=bulk_operations
            )

            print(f"Flushed {len(self.status_update_queue)} status updates to the database.")
            self.status_update_queue.clear()
