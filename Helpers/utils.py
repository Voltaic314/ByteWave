import json
import os
import sys

class Utils:

    @staticmethod
    def load_request(request_file_path):
        """Load configuration from a JSON file."""
        output = {}

        # Handle path when running as a PyInstaller executable
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS  # Special folder created by PyInstaller
            request_file_path = os.path.join(base_path, request_file_path)

        try:
            with open(request_file_path, 'r') as f:
                output = json.load(f)
        except Exception as e:
            print(f"Error loading request from file: {e}")
        return output

    @staticmethod
    def save_request(request_file_path, request_data):
        """Save configuration to a JSON file."""
        try:
            with open(request_file_path, 'w') as f:
                json.dump(request_data, f, indent=4)
        except Exception as e:
            print(f"Error saving request to file: {e}")

    @staticmethod
    def modify_request(request_file_path, key, value):
        """Modify the request JSON with the given key and value."""
        request_data = Utils.load_request(request_file_path)
        if request_data:
            request_data[key] = value
            Utils.save_request(request_file_path, request_data)
        return request_data

    @staticmethod
    def get_service(service_name: str):
        # Conditional imports based on the service name
        if service_name.lower() == "sharepoint":
            from Services.sharepoint import SharePoint
            return SharePoint
        elif service_name.lower() == "box":
            from Services.box import Box
            return Box
        elif service_name.lower() == "windows":
            from Services.os_class import OS_Class
            return OS_Class
        else:
            raise ValueError(f"Invalid service name: {service_name}")
