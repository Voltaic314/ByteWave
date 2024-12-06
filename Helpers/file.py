class File:

    def __init__(self, source: dict = {}, destination: dict = {}):
        # build the source file info
        self.source = type("Source", (), {})()  
        self.source.name = source.get("name", "")
        self.source.path = source.get("path", "")
        self.source.identifier = source.get("identifier", "")
        self.source.parent_id = source.get("parent_id", "")
        self.source.size = source.get("size", "")

        # build the destination file info
        self.destination = type("Destination", (), {})()  
        self.destination.name = destination.get("name", "")
        self.destination.path = destination.get("path", "")
        self.destination.identifier = destination.get("identifier", "")
        self.destination.parent_id = destination.get("parent_id", "")
        self.destination.size = destination.get("size", "")

    