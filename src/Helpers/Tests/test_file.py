import pytest
from Helpers.file import File, FileSubItem


### FileSubItem Tests ###
def test_file_sub_item_initialization():
    # Create a FileSubItem instance
    sub_item = FileSubItem(
        name="example.txt",
        path="/path/to/example.txt",
        identifier="1234",
        parent_id="5678",
        size=1024
    )

    # Assertions
    assert sub_item.name == "example.txt"
    assert sub_item.path == "/path/to/example.txt"
    assert sub_item.identifier == "1234"
    assert sub_item.parent_id == "5678"
    assert sub_item.size == 1024


def test_file_sub_item_repr():
    # Create a FileSubItem instance
    sub_item = FileSubItem(
        name="example.txt",
        path="/path/to/example.txt",
        identifier="1234",
        size=1024
    )

    # Verify the string representation
    assert repr(sub_item) == "FileSubItem(name=example.txt, path=/path/to/example.txt, size=1024)"


### File Tests ###
def test_file_initialization():
    # Create source and destination FileSubItem instances
    source = FileSubItem(
        name="source.txt",
        path="/path/to/source.txt",
        identifier="1234",
        size=2048
    )
    destination = FileSubItem(
        name="destination.txt",
        path="/path/to/destination.txt",
        identifier="5678",
        size=2048
    )

    # Create a File instance
    file = File(source=source, destination=destination)

    # Assertions
    assert file.source == source
    assert file.destination == destination


def test_file_repr():
    # Create source and destination FileSubItem instances
    source = FileSubItem(
        name="source.txt",
        path="/path/to/source.txt",
        identifier="1234",
        size=2048
    )
    destination = FileSubItem(
        name="destination.txt",
        path="/path/to/destination.txt",
        identifier="5678",
        size=2048
    )

    # Create a File instance
    file = File(source=source, destination=destination)

    # Verify the string representation
    assert repr(file) == (
        "File(Source=FileSubItem(name=source.txt, path=/path/to/source.txt, size=2048), "
        "Destination=FileSubItem(name=destination.txt, path=/path/to/destination.txt, size=2048))"
    )
