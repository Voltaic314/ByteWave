
class IdentifierSet:
    def __init__(self, db):
        # Initialize with identifiers from both `files` and `folders` collections
        self.identifiers = self._load_identifiers_from_collections(db)

    def _load_identifiers_from_collections(self, db):
        """
        Load all identifiers from the `files` and `folders` collections based on path_metadata.identifier.
        """
        identifiers = set()
        # Load identifiers from `files` collection
        files_cursor = db.files.find({}, {"path_metadata.identifier": 1})
        identifiers.update(doc["path_metadata"]["identifier"] for doc in files_cursor)

        # Load identifiers from `folders` collection
        folders_cursor = db.folders.find({}, {"path_metadata.identifier": 1})
        identifiers.update(doc["path_metadata"]["identifier"] for doc in folders_cursor)

        return identifiers

    def add(self, identifier):
        """Add a new identifier to the in-memory set."""
        self.identifiers.add(identifier)

    def check_if_exists(self, identifier):
        """Check if the identifier exists in the in-memory set."""
        return identifier in self.identifiers

    def batch_update_to_db(self, db, new_identifiers, is_folder=True):
        """
        Batch write new identifiers to the appropriate MongoDB collection.
        """
        collection = db.folders if is_folder else db.files
        documents = [{"path_metadata": {"identifier": id}} for id in new_identifiers]
        
        if new_identifiers:
            collection.insert_many(documents)
            self.identifiers.update(new_identifiers)
            print(f"Batch inserted {len(new_identifiers)} new {'folders' if is_folder else 'files'}.")
