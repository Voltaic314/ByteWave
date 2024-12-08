from Helpers.db import DB
from typing import Union
from Helpers.file import File
from Helpers.folder import Folder


class TrieNode:
    """
    Represents a node in the Trie structure. Can reference either a File or a Folder.
    """
    def __init__(self, item: Union[File, Folder], parent=None):
        self.item = item  # Either a File or Folder instance
        self.parent = parent
        self.children = {}  # Child nodes keyed by their names

        # Separate statuses for traversal and upload
        self.traversal_status = "pending"  # "pending", "successful", "failed"
        self.upload_status = "pending"  # "pending", "successful", "failed"
        self.traversal_attempts = 0
        self.upload_attempts = 0

    def add_child(self, child_item):
        """
        Add a child node to this node and update the parent-child relationship.

        Args:
            child_item (File | Folder): The file or folder object for the child.

        Returns:
            TrieNode: The newly created or existing child node.
        """
        if child_item.name not in self.children:
            child_node = TrieNode(item=child_item, parent=self)
            self.children[child_item.name] = child_node
            return child_node
        return self.children[child_item.name]

    def update_traversal_status(self, new_status: str):
        self.traversal_status = new_status

    def update_upload_status(self, new_status: str):
        self.upload_status = new_status

    def increment_traversal_attempts(self):
        self.traversal_attempts += 1

    def increment_upload_attempts(self):
        self.upload_attempts += 1

    def to_dict(self):
        """
        Serialize the node to a dictionary for database storage.
        """
        return {
            "identifier": self.item.source.identifier if self.item.source else None,
            "parent_id": self.parent.item.source.identifier if self.parent and self.parent.item.source else None,
            "children": [child.item.source.identifier for child in self.children.values()],
            "traversal_status": self.traversal_status,
            "upload_status": self.upload_status,
            "traversal_attempts": self.traversal_attempts,
            "upload_attempts": self.upload_attempts,
        }


class FileSystemTrie:
    """
    A Trie data structure for managing hierarchical paths for file and folder creation.
    """
    def __init__(self, db: DB, max_traversal_retries: int = 0, max_upload_retries: int = 0):
        self.db = db
        self.root = TrieNode(Folder(name="root", identifier="root", path="/", parent_id=None))  # Root node
        self.node_map = {"root": self.root}
        self.status_updates = []  # Batch of updates for MongoDB
        self.child_updates = []  # Batch updates for parent-child relationships
        self.node_updates = []  # Updates for serializing entire nodes
        self.flush_threshold = 50
        self.max_traversal_retries = max_traversal_retries
        self.max_upload_retries = max_upload_retries

    def find_node(self, identifier: str):
        return self.node_map.get(identifier)

    def add_item(self, item: Union[File, Folder]):
        """
        Add an item to the Trie, ensuring parent-child relationships are updated.

        Args:
            item (File | Folder): The item to add to the Trie.
        """
        if item.source and item.source.identifier in self.node_map:
            return  # Skip if the identifier already exists

        current = self.root
        parts = item.source.path.strip("/").split("/")
        for part in parts[:-1]:
            if part not in current.children:
                new_folder = Folder(
                    name=part, path="/".join(parts[:parts.index(part) + 1]),
                    identifier=None, parent_id=current.item.source.identifier
                )
                current = current.add_child(new_folder)
            else:
                current = current.children[part]

        # Add the final item
        new_node = current.add_child(item)
        self.node_map[item.source.identifier] = new_node

        # Queue parent-child update for the database
        if new_node.parent and new_node.parent.item.source:
            parent_identifier = new_node.parent.item.source.identifier
            self.child_updates.append({
                "filter": {"identifier": parent_identifier},
                "update": {"$addToSet": {"children": item.source.identifier}},
                "upsert": True
            })

        # Queue node serialization update
        self.node_updates.append(new_node.to_dict())

    async def flush_updates(self):
        """
        Flush batched status, child updates, and node serializations to the database.
        """
        try:
            if self.status_updates:
                await self.db.update_entries("nodes", updates=self.status_updates)
                print(f"Flushed {len(self.status_updates)} status updates to the database.")
                self.status_updates.clear()

            if self.child_updates:
                await self.db.update_entries("nodes", updates=self.child_updates)
                print(f"Flushed {len(self.child_updates)} parent-child updates to the database.")
                self.child_updates.clear()

            if self.node_updates:
                await self.db.insert_entries("nodes", self.node_updates)
                print(f"Serialized {len(self.node_updates)} nodes to the database.")
                self.node_updates.clear()
        except Exception as e:
            print(f"Error flushing updates: {e}")

    async def update_node_traversal_status(self, identifier: str, new_status: str):
        """
        Update the traversal status of a node and queue the change for database flushing.

        Args:
            identifier (str): The identifier of the node to update.
            new_status (str): The new status to set ("successful", "failed").
        """
        node = self.find_node(identifier)
        if not node:
            print(f"Node with identifier {identifier} not found.")
            return

        if new_status == "failed" and self.max_traversal_retries > 0:
            if node.traversal_attempts < self.max_traversal_retries:
                node.increment_traversal_attempts()
                print(f"Retrying traversal for node {identifier}. Attempt {node.traversal_attempts}/{self.max_traversal_retries}.")
                return
            else:
                print(f"Max traversal retries reached for node {identifier}.")

        node.update_traversal_status(new_status)
        self.status_updates.append({
            "filter": {"identifier": identifier},
            "update": {"$set": {
                "traversal_status": new_status,
                "traversal_attempts": node.traversal_attempts
            }},
            "upsert": True
        })

        if len(self.status_updates) >= self.flush_threshold:
            await self.flush_updates()

    async def update_node_upload_status(self, identifier: str, new_status: str):
        """
        Update the upload status of a node and queue the change for database flushing.

        Args:
            identifier (str): The identifier of the node to update.
            new_status (str): The new status to set ("successful", "failed").
        """
        node = self.find_node(identifier)
        if not node:
            print(f"Node with identifier {identifier} not found.")
            return

        if new_status == "failed" and self.max_upload_retries > 0:
            if node.upload_attempts < self.max_upload_retries:
                node.increment_upload_attempts()
                print(f"Retrying upload for node {identifier}. Attempt {node.upload_attempts}/{self.max_upload_retries}.")
                return
            else:
                print(f"Max upload retries reached for node {identifier}.")

        node.update_upload_status(new_status)
        self.status_updates.append({
            "filter": {"identifier": identifier},
            "update": {"$set": {
                "upload_status": new_status,
                "upload_attempts": node.upload_attempts
            }},
            "upsert": True
        })

        if len(self.status_updates) >= self.flush_threshold:
            await self.flush_updates()

    async def clear(self):
        """
        Clear the Trie structure and metadata.
        """
        self.root = TrieNode(Folder(name="root", identifier="root", path="/", parent_id=None))
        self.node_map = {"root": self.root}
        self.status_updates.clear()
        self.child_updates.clear()
        self.node_updates.clear()
        print("FileSystemTrie cleared.")
