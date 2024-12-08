from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime


class DB:
    def __init__(self, db_name="migration_db"):
        # Connect to MongoDB asynchronously
        self.client = AsyncIOMotorClient()  # Add MongoDB connection parameters if needed
        self.db = self.client[db_name]
        self.collections = {
            "files": self.db.files,
            "folders": self.db.folders,
            "nodes": self.db.nodes,  # Added nodes collection
            "resource_monitoring": self.db.resource_monitoring,
        }

    async def execute_db_operation(self, operation_type: str, collection_name: str, **kwargs):
        """
        Generic method to handle fetch, update, delete, or insert operations.

        Args:
            operation_type (str): Operation type, e.g., "fetch", "update", "delete", "insert".
            collection_name (str): The collection to operate on.
            **kwargs: Additional arguments for MongoDB queries (e.g., filters, updates, etc.).

        Returns:
            Depends on the operation_type:
                - "fetch": List of documents.
                - "update", "delete", "insert": Operation result.
        """
        collection = self.collections.get(collection_name)
        if not collection:
            raise ValueError(f"Collection '{collection_name}' does not exist.")

        if operation_type == "fetch":
            query = kwargs.get("query", {})
            projection = kwargs.get("projection", None)
            return [doc async for doc in collection.find(query, projection)]

        if operation_type == "insert":
            data = kwargs.get("data", [])
            if isinstance(data, list):
                return await collection.insert_many(data) if data else None
            return await collection.insert_one(data)

        if operation_type == "update":
            updates = kwargs.get("updates", [])
            if updates:
                return await collection.bulk_write(updates)
            filter_ = kwargs.get("filter", {})
            update_data = kwargs.get("update", {})
            return await collection.update_one(filter_, update_data)

        if operation_type == "delete":
            filter_ = kwargs.get("filter", {})
            return await collection.delete_many(filter_)

        raise ValueError(f"Unsupported operation type: {operation_type}")

    # Convenience methods

    async def fetch_entries(self, collection_name, query=None, projection=None):
        """Fetch entries from the specified collection."""
        return await self.execute_db_operation("fetch", collection_name, query=query, projection=projection)

    async def insert_entries(self, collection_name, data):
        """Insert entries into the specified collection."""
        return await self.execute_db_operation("insert", collection_name, data=data)

    async def update_entries(self, collection_name, updates=None, filter_=None, update_data=None):
        """
        Update entries in the specified collection. Supports both bulk updates and single updates.
        """
        return await self.execute_db_operation(
            "update", collection_name, updates=updates, filter=filter_, update=update_data
        )

    async def delete_entries(self, collection_name, filter_):
        """Delete entries from the specified collection."""
        return await self.execute_db_operation("delete", collection_name, filter=filter_)

    # Specialized methods for nodes
    async def serialize_node(self, node_data: dict):
        """
        Serialize a node into the 'nodes' collection.

        Args:
            node_data (dict): The node data to store.
        """
        return await self.insert_entries("nodes", data=node_data)

    async def fetch_nodes(self, query=None, projection=None):
        """
        Fetch nodes from the 'nodes' collection.

        Args:
            query (dict): Query parameters for fetching nodes.
            projection (dict): Projection parameters for limiting fields.

        Returns:
            list: A list of node documents.
        """
        return await self.fetch_entries("nodes", query=query, projection=projection)

    async def clear_nodes(self):
        """
        Clear all nodes from the 'nodes' collection.
        """
        return await self.delete_entries("nodes", filter_={})

    # Specialized methods for resource monitoring
    async def log_resource_data(self, resource_type: str, resource_data: list):
        """Log resource monitoring data."""
        data = {
            "resource_type": resource_type,
            "data": resource_data,
            "timestamp": datetime.now().isoformat(),
        }
        return await self.insert_entries("resource_monitoring", data)

    async def fetch_resource_data(self, resource_type: str):
        """Fetch resource monitoring data."""
        query = {"resource_type": resource_type}
        return await self.fetch_entries("resource_monitoring", query=query)

    async def clear_resource_data(self, resource_type: str):
        """Clear all resource monitoring data for a given type."""
        query = {"resource_type": resource_type}
        return await self.delete_entries("resource_monitoring", filter_=query)
