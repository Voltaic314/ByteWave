import json

class PathHelper:

    def __init__(self, path_components=[], path_metadata_list=[]):
        # Initialize with a root path and metadata
        self.path_components = path_components
        self.path_metadata = path_metadata_list

    def get_metadata_item_by_name(self, name):
        """Get the metadata item by name."""
        for metadata in self.path_metadata:
            if metadata.get("name", "") == name:
                return metadata
        return {}

    def get_metadata_item_by_identifier(self, identifier):
        """Get the metadata item by identifier."""
        for metadata in self.path_metadata:
            if metadata.get("identifier", "") == identifier:
                return metadata
        return {}

    def add(self, metadata={}):
        """Add a component (directory or file) to the current path with optional metadata."""
        component = metadata.get("name", "")
        if component:  # Ensure there is a valid name before adding
            self.path_components.append(component)
            self.path_metadata.append(metadata)

    def remove(self):
        """Remove the last component from the path."""
        if self.path_components:
            self.path_metadata.pop()  # Remove the corresponding metadata
            return self.path_components.pop()
        return None

    def get_current_path(self, separator='/'):
        """Get the current path as a string with the specified separator."""
        return separator + separator.join(self.path_components)

    def get_current_metadata(self):
        """Get the current metadata for the last component in the path."""
        return self.path_metadata[-1] if self.path_metadata else {}
    
    def get_last_identifier(self):
        """Get the identifier of the last component in the path."""
        metadata = self.get_current_metadata()
        return metadata.get("identifier", "") if metadata else ""

    def clear(self):
        """Clear the path and metadata."""
        self.path_components = []
        self.path_metadata = []

    def __repr__(self):
        """For easier debugging and printing purposes."""
        return f"Path: {self.get_current_path()}, \nMetadata: {json.dumps(self.path_metadata, indent=4)}"
