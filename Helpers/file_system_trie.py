from Helpers.db import DB
from typing import Union
from Helpers.file import File
from Helpers.folder import Folder


class TrieNode:
    """
    Represents a node in the Trie structure. Can reference either a File or a Folder.
    """
    def __init__(self, item: Union[File, Folder], parent=None):
        """
        Initialize a TrieNode with a File or Folder item.

        Args:
            item (File | Folder): The file or folder this node represents.
            parent (TrieNode): The parent node.
        """
        self.item = item  # Either a File or Folder instance
        self.parent = parent
        self.children = {}  # Child nodes keyed by their names
        self.status = "pending"  # Default status: "pending", "successful", "failed"
        self.retry_attempts = 0  # Tracks how many retries have been attempted

    def add_child(self, child_item):
        """
        Add a child node to this node.

        Args:
            child_item (File | Folder): The file or folder object for the child.

        Returns:
            TrieNode: The newly created or existing child node.
        """
        if child_item.name not in self.children:
            self.children[child_item.name] = TrieNode(item=child_item, parent=self)
        return self.children[child_item.name]

    def update_status(self, new_status: str):
        """
        Update the status of the node.

        Args:
            new_status (str): The new status to set ("successful", "failed", etc.).
        """
        self.status = new_status

    def increment_retry(self):
        """
        Increment the retry attempt counter.
        """
        self.retry_attempts += 1


class FileSystemTrie:
    """
    A Trie data structure for managing hierarchical paths for file and folder creation.
    """
    def __init__(self, db: DB, max_retries: int = 0):
        """
        Initialize the FileSystemTrie.

        Args:
            db (DB): Async database dependency for storing data.
            max_retries (int): Maximum number of retry attempts allowed for each node. Default is 0 (no retries).
        """
        self.db = db  # Inject the async database dependency
        self.root = TrieNode(Folder(name="root", identifier="root", path="/", parent_id=None))  # Root node
        self.node_map = {"root": self.root}  # Hash map for efficient lookups
        self.status_updates = []  # Queue for batch status updates
        self.flush_threshold = 50  # Batch size for flushing
        self.max_retries = max_retries  # Maximum retries allowed

    async def update_node_status(self, identifier: str, new_status: str):
        """
        Update the status of a node and queue the change for database flushing.

        Args:
            identifier (str): The identifier of the node to update.
            new_status (str): The new status to set ("successful", "failed", etc.).
        """
        node = self.find_node(identifier)
        if not node:
            print(f"Node with identifier {identifier} not found.")
            return

        if new_status == "failed" and self.max_retries > 0:
            if node.retry_attempts < self.max_retries:
                node.increment_retry()
                print(f"Retrying node {identifier}. Attempt {node.retry_attempts}/{self.max_retries}.")
                # Logic for reprocessing the node should go here
                return
            else:
                print(f"Max retries reached for node {identifier}.")
        
        node.update_status(new_status)
        self.status_updates.append({
            "identifier": identifier,
            "status": new_status,
            "retry_attempts": node.retry_attempts
        })

        if len(self.status_updates) >= self.flush_threshold:
            await self.flush_status_updates()

    async def flush_status_updates(self):
        """
        Batch update node statuses in MongoDB and clear the buffer asynchronously.
        """
        if self.status_updates:
            bulk_operations = [
                {
                    "filter": {"identifier": update["identifier"]},
                    "update": {"$set": {
                        "status": update["status"],
                        "retry_attempts": update["retry_attempts"]
                    }},
                    "upsert": True
                }
                for update in self.status_updates
            ]
            try:
                await self.db.bulk_write("items", bulk_operations)
                print(f"Flushed {len(self.status_updates)} status updates to the database.")
                self.status_updates.clear()
            except Exception as e:
                print(f"Error flushing status updates: {e}")

    def find_node(self, identifier: str):
        """
        Find a node by its identifier.

        Args:
            identifier (str): The identifier to look up.

        Returns:
            TrieNode | None: The node if found, else None.
        """
        return self.node_map.get(identifier)

    async def clear(self):
        """
        Clear the Trie structure and metadata.
        """
        self.root = TrieNode(Folder(name="root", identifier="root", path="/", parent_id=None))
        self.node_map = {"root": self.root}
        self.status_updates.clear()
        print("FileSystemTrie cleared.")
