class Folder:
    """
    Represents a folder with separate attributes for source and destination metadata.
    """

    def __init__(self, source: dict = {}, destination: dict = {}):
        """
        Initialize a Folder object with source and destination attributes.

        Args:
            source (dict): Metadata for the folder on the source system.
            destination (dict): Metadata for the folder on the destination system.
        """
        # Build the source folder attributes dynamically
        self.source = type("Source", (), {})()  
        self.source.name = source.get("name", "")
        self.source.path = source.get("path", "")
        self.source.identifier = source.get("identifier", "")
        self.source.parent_id = source.get("parent_id", "")

        # Build the destination folder attributes dynamically
        self.destination = type("Destination", (), {})()  
        self.destination.name = destination.get("name", "")
        self.destination.path = destination.get("path", "")
        self.destination.identifier = destination.get("identifier", "")
        self.destination.parent_id = destination.get("parent_id", "")
