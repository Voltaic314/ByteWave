import aiosqlite
from datetime import datetime

class DB:
    def __init__(self, db_name="migration_db.sqlite"):
        """
        Initialize the SQLite database connection.
        
        Args:
            db_name (str): Name of the SQLite database file.
        """
        self.db_name = db_name

    async def setup_nodes_table(self):
        """
        Create the 'nodes' table if it doesn't exist.
        """
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                source_identifier TEXT,
                destination_identifier TEXT,
                traversal_status TEXT NOT NULL,
                upload_status TEXT NOT NULL,
                traversal_attempts INTEGER DEFAULT 0,
                upload_attempts INTEGER DEFAULT 0,
                FOREIGN KEY (parent_id) REFERENCES nodes (id)
            )
            """)
            await db.commit()

    async def setup_resource_monitoring_table(self):
        """
        Create the 'resource_monitoring' table if it doesn't exist.
        """
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS resource_monitoring (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_type TEXT NOT NULL,
                usage REAL NOT NULL,
                timestamp TEXT NOT NULL
            )
            """)
            await db.commit()

    async def setup_errors_table(self):
        """
        Create the 'errors' table if it doesn't exist.
        """
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS errors (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                error_details TEXT,
                timestamp TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                FOREIGN KEY (node_id) REFERENCES nodes (id)
            )
            """)
            await db.commit()

    async def initialize_db(self):
        await self.setup_nodes_table()
        await self.setup_resource_monitoring_table()
        await self.setup_errors_table()

    # General-purpose query execution
    async def execute_query(self, query, params=None, fetch=False, fetchall=False):
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(query, params or ()) as cursor:
                if fetch:
                    return await cursor.fetchone()
                if fetchall:
                    return await cursor.fetchall()
            await db.commit()

    # Errors table methods
    async def log_error(self, error_data: dict):
        """
        Log an error in the 'errors' table.

        Args:
            error_data (dict): Dictionary containing error details.
        """
        query = """
        INSERT INTO errors (node_id, error_type, error_message, error_details, timestamp, retry_count)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            error_data.get("node_id"),
            error_data["error_type"],
            error_data["error_message"],
            error_data.get("error_details"),
            error_data.get("timestamp", datetime.now().isoformat()),
            error_data.get("retry_count", 0)
        )
        await self.execute_query(query, params)

    async def fetch_errors(self, limit=None):
        """
        Fetch errors from the 'errors' table.

        Args:
            limit (int): Optional limit on the number of errors to fetch.

        Returns:
            list: A list of error rows.
        """
        query = "SELECT * FROM errors ORDER BY timestamp DESC"
        if limit:
            query += " LIMIT ?"
            return await self.execute_query(query, (limit,), fetchall=True)
        return await self.execute_query(query, fetchall=True)

    async def delete_errors(self):
        """
        Delete all errors from the 'errors' table.
        """
        query = "DELETE FROM errors"
        await self.execute_query(query)

    # Convenience methods for nodes

    async def insert_node(self, node_data: dict):
        query = """
        INSERT INTO nodes (parent_id, name, type, source_identifier, destination_identifier, 
                           traversal_status, upload_status, traversal_attempts, upload_attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            node_data.get("parent_id"),
            node_data["name"],
            node_data["type"],
            node_data.get("source_identifier"),
            node_data.get("destination_identifier"),
            node_data["traversal_status"],
            node_data["upload_status"],
            node_data.get("traversal_attempts", 0),
            node_data.get("upload_attempts", 0)
        )
        return await self.execute_query(query, params)

    async def fetch_node_by_id(self, node_id: int):
        query = "SELECT * FROM nodes WHERE id = ?"
        result = await self.execute_query(query, (node_id,), fetch=True)
        return dict(result) if result else None

    async def fetch_children(self, parent_id: int):
        query = "SELECT * FROM nodes WHERE parent_id = ?"
        results = await self.execute_query(query, (parent_id,), fetchall=True)
        return [dict(row) for row in results] if results else []

    async def update_node_status(self, node_id: int, traversal_status=None, upload_status=None):
        updates = []
        params = []
        if traversal_status:
            updates.append("traversal_status = ?")
            params.append(traversal_status)
        if upload_status:
            updates.append("upload_status = ?")
            params.append(upload_status)
        params.append(node_id)
        query = f"UPDATE nodes SET {', '.join(updates)} WHERE id = ?"
        await self.execute_query(query, params)

    async def clear_nodes(self):
        query = "DELETE FROM nodes"
        await self.execute_query(query)

    async def fetch_nodes_by_status(self, status: str, status_type: str):
        """
        Fetch nodes from the 'nodes' table based on their status.

        Args:
            status (str): The desired status ('pending', 'successful', 'failed').
            status_type (str): The type of status ('traversal' or 'upload').

        Returns:
            list: List of node rows matching the specified status.
        """
        column = "traversal_status" if status_type == "traversal" else "upload_status"
        query = f"SELECT * FROM nodes WHERE {column} = ?"
        return await self.execute_query(query, (status,), fetchall=True)

    async def update_node_status(self, node_id: int, traversal_status=None, upload_status=None):
        """
        Update the status of a node in the 'nodes' table.

        Args:
            node_id (int): The ID of the node to update.
            traversal_status (str): New traversal status, if applicable.
            upload_status (str): New upload status, if applicable.
        """
        updates = []
        params = []
        if traversal_status:
            updates.append("traversal_status = ?")
            params.append(traversal_status)
        if upload_status:
            updates.append("upload_status = ?")
            params.append(upload_status)
        params.append(node_id)

        query = f"UPDATE nodes SET {', '.join(updates)} WHERE id = ?"
        await self.execute_query(query, params)

    # Resource monitoring methods
    async def log_resource_data(self, resource_type: str, resource_data: list):
        """
        Batch insert resource monitoring data.

        Args:
            resource_type (str): Type of resource (e.g., CPU, memory).
            resource_data (list): List of usage values.
        """
        timestamp = datetime.now().isoformat()
        query = """
        INSERT INTO resource_monitoring (resource_type, usage, timestamp)
        VALUES (?, ?, ?)
        """
        params = [
            (resource_type, usage, timestamp) for usage in resource_data
        ]
        async with aiosqlite.connect(self.db_name) as db:
            await db.executemany(query, params)
            await db.commit()

    async def fetch_resource_data(self, resource_type: str, since=None):
        """
        Fetch resource monitoring data.

        Args:
            resource_type (str): Type of resource to fetch.
            since (str): Optional ISO timestamp to fetch data since that time.

        Returns:
            list: A list of resource monitoring data.
        """
        query = "SELECT * FROM resource_monitoring WHERE resource_type = ?"
        params = [resource_type]
        if since:
            query += " AND timestamp >= ?"
            params.append(since)
        return await self.execute_query(query, params, fetchall=True)

    async def clear_resource_data(self, resource_type: str):
        """
        Clear all resource monitoring data for a given type.

        Args:
            resource_type (str): Type of resource to clear.
        """
        query = "DELETE FROM resource_monitoring WHERE resource_type = ?"
        await self.execute_query(query, (resource_type,))
