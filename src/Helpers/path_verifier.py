import re


class PathVerifier:
    """
    Handles validation of file and folder paths based on extensions, keywords, and bad keywords.
    """

    def __init__(self, request: dict):
        """
        Initialize the PathVerifier with criteria from the request configuration.

        Args:
            request (dict): Configuration parameters, typically from request.json.
        """
        self.valid_extensions = request.get("valid_extensions", [])
        self.keywords = [keyword.lower() for keyword in request.get("keywords", [])]
        self.bad_keywords = [keyword.lower() for keyword in request.get("bad_keywords", [])]
        self.regex_patterns = [re.compile(pattern) for pattern in request.get("regex_patterns", [])]

    def _validate_path(self, file_path: str, is_file: bool) -> bool:
        """
        Centralized validation logic for file or folder paths.

        Args:
            file_path (str): The path to validate.
            is_file (bool): Whether the path belongs to a file.

        Returns:
            bool: True if the path is valid, False otherwise.
        """
        # Check bad keywords for all items
        if not self.path_does_not_match_bad_keywords(file_path):
            return False

        # Additional checks for files
        if is_file:
            return (
                self.is_valid_file_extension(file_path)
                and self.path_matches_keywords(file_path)
            )
        return True

    def is_valid_file_extension(self, file_path: str) -> bool:
        """Check if the file has a valid extension."""
        file_ext = file_path.split('.')[-1]
        return file_ext in self.valid_extensions if self.valid_extensions else True

    def path_matches_keywords(self, file_path: str) -> bool:
        """Check if the file path contains all required keywords."""
        return all(keyword in file_path.lower() for keyword in self.keywords) if self.keywords else True

    def path_does_not_match_bad_keywords(self, file_path: str) -> bool:
        """Check if the file path does not contain any bad keywords."""
        return not any(keyword in file_path.lower() for keyword in self.bad_keywords) if self.bad_keywords else True

    def is_valid_item(self, item) -> bool:
        """
        Validate an item based on its metadata.

        Args:
            item (FileSubItem | FolderSubItem): Item object to validate.

        Returns:
            bool: True if the item is valid, False otherwise.
        """
        # Determine if the item is a file or folder
        is_file = hasattr(item, "size")

        # Validate source path
        if item.path:
            return self._validate_path(item.path, is_file)

        return False
