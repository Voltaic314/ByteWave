class FolderSubItem:
    """
    Represents a basic folder with attributes like name, path, identifier, and parent_id.
    """

    def __init__(self, name, path, identifier, parent_id=None):
        self.name = name
        self.path = path
        self.identifier = identifier
        self.parent_id = parent_id

    def __repr__(self):
        return f"FolderSubItem(name={self.name}, path={self.path})"


class Folder:
    """
    Represents a folder with separate source and destination attributes.
    """

    def __init__(self, source: FolderSubItem = None, destination: FolderSubItem = None):
        self.source = source
        self.destination = destination

    def __repr__(self):
        return (
            f"Folder(Source={self.source}, "
            f"Destination={self.destination})"
        )
