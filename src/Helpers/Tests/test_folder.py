import pytest
from Helpers.folder import Folder, FolderSubItem


### FolderSubItem Tests ###
def test_folder_sub_item_initialization():
    # Create a FolderSubItem instance
    sub_item = FolderSubItem(
        name="example_folder",
        path="/path/to/example_folder",
        identifier="1234",
        parent_id="5678"
    )

    # Assertions
    assert sub_item.name == "example_folder"
    assert sub_item.path == "/path/to/example_folder"
    assert sub_item.identifier == "1234"
    assert sub_item.parent_id == "5678"


def test_folder_sub_item_repr():
    # Create a FolderSubItem instance
    sub_item = FolderSubItem(
        name="example_folder",
        path="/path/to/example_folder",
        identifier="1234"
    )

    # Verify the string representation
    assert repr(sub_item) == "FolderSubItem(name=example_folder, path=/path/to/example_folder)"


### Folder Tests ###
def test_folder_initialization():
    # Create source and destination FolderSubItem instances
    source = FolderSubItem(
        name="source_folder",
        path="/path/to/source_folder",
        identifier="1234"
    )
    destination = FolderSubItem(
        name="destination_folder",
        path="/path/to/destination_folder",
        identifier="5678"
    )

    # Create a Folder instance
    folder = Folder(source=source, destination=destination)

    # Assertions
    assert folder.source == source
    assert folder.destination == destination


def test_folder_repr():
    # Create source and destination FolderSubItem instances
    source = FolderSubItem(
        name="source_folder",
        path="/path/to/source_folder",
        identifier="1234"
    )
    destination = FolderSubItem(
        name="destination_folder",
        path="/path/to/destination_folder",
        identifier="5678"
    )

    # Create a Folder instance
    folder = Folder(source=source, destination=destination)

    # Verify the string representation
    assert repr(folder) == (
        "Folder(Source=FolderSubItem(name=source_folder, path=/path/to/source_folder), "
        "Destination=FolderSubItem(name=destination_folder, path=/path/to/destination_folder))"
    )
