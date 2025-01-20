from datetime import datetime
from typing import List


class Error:
    def __init__(self, error_type: str, message: str, code: int = None, metadata: dict = None):
        self.error_type = error_type  # e.g., 'PermissionError', 'ConnectionError'
        self.message = message  # Human-readable message
        self.metadata = metadata or {}  # Additional contextual details

    def to_dict(self) -> dict:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "metadata": self.metadata,
        }

    def __str__(self):
        metadata_str = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
        return f"{self.error_type}: {self.message} (Code: {self.code}, Metadata: {metadata_str})"


class Warning:
    def __init__(self, warning_type: str, message: str, metadata: dict = None):
        self.warning_type = warning_type  # e.g., 'RetryWarning', 'DeprecationWarning'
        self.message = message  # Human-readable message
        self.metadata = metadata or {}  # Additional contextual details

    def to_dict(self) -> dict:
        return {
            "warning_type": self.warning_type,
            "message": self.message,
            "metadata": self.metadata,
        }

    def __str__(self):
        metadata_str = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
        return f"{self.warning_type}: {self.message} (Metadata: {metadata_str})"


class Response:
    def __init__(self, success: bool, response=None):
        self.success = success
        self.response = response
        self.errors: List[Error] = []  # List of ErrorDetail objects
        self.warnings: List[Warning] = []  # List of WarningDetail objects
        self.timestamp = datetime.now()

    def add_error(self, error_type: str, message: str, code: int = None, metadata: dict = None):
        """
        Add an error to the response.

        Args:
            error_type (str): Type of the error (e.g., 'PermissionError').
            message (str): Human-readable error message.
            code (int): Optional error code (e.g., HTTP status).
            metadata (dict): Additional metadata for debugging.
        """
        error = Error(error_type, message, code, metadata)
        if error not in self.errors:
            self.errors.append(error)

    def add_warning(self, warning_type: str, message: str, metadata: dict = None):
        """
        Add a warning to the response.

        Args:
            warning_type (str): Type of the warning (e.g., 'RetryWarning').
            message (str): Human-readable warning message.
            metadata (dict): Additional metadata for debugging.
        """
        warning = Warning(warning_type, message, metadata)
        if warning not in self.warnings:
            self.warnings.append(warning)

    def to_dict(self) -> dict:
        """
        Convert the Response object to a dictionary for serialization or logging.
        """
        return {
            "success": self.success,
            "response": self.response,
            "errors": [error.to_dict() for error in self.errors],
            "warnings": [warning.to_dict() for warning in self.warnings],
            "timestamp": self.timestamp.isoformat(),
        }

    def __str__(self):
        """
        String representation of the Response object.
        """
        errors = [str(error) for error in self.errors]
        warnings = [str(warning) for warning in self.warnings]
        return (
            f"Response(success={self.success}, response={self.response}, "
            f"errors={errors}, warnings={warnings}, timestamp={self.timestamp})"
        )
