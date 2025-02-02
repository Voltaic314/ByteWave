from datetime import datetime
from typing import List, Optional, Dict, Any


class Error:

    def __init__(self, 
                error_type: str, 
                message: str, 
                details: Optional[str] = None, 
                metadata: Optional[Dict] = None
    ):
        self.error_type = error_type
        self.message = message
        self.details = details
        self.metadata = metadata or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }

    def __str__(self):
        metadata_str = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
        return f"{self.error_type}: {self.message} \nMetadata: {metadata_str}"


class Warning:

    def __init__(self,
                warning_type: str,
                message: str,
                details: Optional[str] = None,
                metadata: Optional[Dict] = None
    ):
        self.warning_type = warning_type
        self.message = message
        self.details = details
        self.metadata = metadata or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "warning_type": self.warning_type,
            "message": self.message,
            "details": self.details,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }

    def __str__(self):
        metadata_str = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
        return f"{self.warning_type}: {self.message} \nMetadata: {metadata_str}"


class Response:

    def __init__(
        self,
        success: bool,
        response: Optional[Any] = None,
        errors: Optional[List[Error]] = None,
        warnings: Optional[List[Warning]] = None,
    ):
        """
        Initializes a Response object.

        Args:
            success (bool): Whether the operation was successful.
            response (Any, optional): The actual response data.
            errors (List[Error], optional): List of `Error` objects.
            warnings (List[Warning], optional): List of `Warning` objects.
        """
        self.success = success
        self.response = response
        self.errors: List[Error] = errors if errors else []
        self.warnings: List[Warning] = warnings if warnings else []
        self.timestamp = datetime.now()

    def add_error(self, error: Error):
        """Add a single error to the response."""
        if error not in self.errors:
            self.errors.append(error)

    def add_warning(self, warning: Warning):
        """Add a single warning to the response."""
        if warning not in self.warnings:
            self.warnings.append(warning)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Response object to a dictionary for serialization or logging."""
        return {
            "success": self.success,
            "response": self.response,
            "errors": [error.to_dict() for error in self.errors],
            "warnings": [warning.to_dict() for warning in self.warnings],
            "timestamp": self.timestamp.isoformat(),
        }

    def __str__(self):
        """String representation of the Response object."""
        errors = [str(error) for error in self.errors]
        warnings = [str(warning) for warning in self.warnings]
        return (
            f"Response(success={self.success}, response={self.response}, "
            f"errors={errors}, warnings={warnings}, timestamp={self.timestamp})"
        )

    def __bool__(self):
        """
        Allows using Response as a truthy/falsy value.
        A Response object evaluates to True if success=True, and False otherwise.
        """
        return self.success


class API_Response(Response):

    def __init__(self, 
                success: bool,
                code: int,
                response=None,
                **kwargs
    ):

        super().__init__(success, response)
        self.code = code
        self.metadata = kwargs
    
    def to_dict(self) -> dict:
        """
        Convert the Response object to a dictionary for serialization or logging.
        """
        return {
            "success": self.success,
            "response": self.response,
            "errors": [error.to_dict() for error in self.errors],
            "warnings": [warning.to_dict() for warning in self.warnings],
            "code": self.code,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }
