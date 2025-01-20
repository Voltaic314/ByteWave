class FileSubItem:
    """
    Represents a basic file with attributes like name, path, size, and parent_id.
    """

    def __init__(self, name, path, identifier, parent_id=None, size=None):
        self.name = name
        self.path = path
        self.identifier = identifier
        self.parent_id = parent_id
        self.size = size

    def __repr__(self):
        return f"FileSubItem(name={self.name}, path={self.path}, size={self.size})"


class File:
    """
    Represents a file with separate source and destination attributes.
    """

    def __init__(self, source: FileSubItem = None, destination: FileSubItem = None):
        self.source = source
        self.destination = destination

    def __repr__(self):
        return (
            f"File(Source={self.source}, "
            f"Destination={self.destination})"
        )
