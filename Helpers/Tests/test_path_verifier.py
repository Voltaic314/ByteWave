import pytest
from Helpers.path_verifier import PathVerifier
from Helpers.file import FileSubItem
from Helpers.folder import FolderSubItem


### PathVerifier Tests ###
@pytest.fixture
def path_verifier():
    """Fixture to create a PathVerifier instance with test criteria."""
    request = {
        "valid_extensions": ["txt", "log", "csv"],
        "keywords": ["important", "project"],
        "bad_keywords": ["temp", "backup"],
        "regex_patterns": [r"^/valid_path/.*"]
    }
    return PathVerifier(request)


def test_is_valid_file_extension(path_verifier):
    assert path_verifier.is_valid_file_extension("/path/to/file.txt") is True
    assert path_verifier.is_valid_file_extension("/path/to/file.exe") is False
    assert path_verifier.is_valid_file_extension("/path/to/file") is False


def test_path_matches_keywords(path_verifier):
    assert path_verifier.path_matches_keywords("/path/to/important_project.txt") is True
    assert path_verifier.path_matches_keywords("/path/to/unrelated_file.txt") is False


def test_path_does_not_match_bad_keywords(path_verifier):
    assert path_verifier.path_does_not_match_bad_keywords("/path/to/file.txt") is True
    assert path_verifier.path_does_not_match_bad_keywords("/path/to/temp_file.txt") is False
    assert path_verifier.path_does_not_match_bad_keywords("/path/to/backup_data.log") is False


def test_is_valid_item_file(path_verifier):
    file_item = FileSubItem(
        name="important_project.txt",
        path="/valid_path/to/important_project.txt",
        identifier="1234",
        size=1024
    )
    assert path_verifier.is_valid_item(file_item) is True

    invalid_file = FileSubItem(
        name="temp_file.txt",
        path="/valid_path/to/temp_file.txt",
        identifier="5678",
        size=1024
    )
    assert path_verifier.is_valid_item(invalid_file) is False


def test_is_valid_item_folder(path_verifier):
    folder_item = FolderSubItem(
        name="important_project",
        path="/valid_path/to/important_project",
        identifier="1234"
    )
    assert path_verifier.is_valid_item(folder_item) is True

    invalid_folder = FolderSubItem(
        name="backup_folder",
        path="/valid_path/to/backup_folder",
        identifier="5678"
    )
    assert path_verifier.is_valid_item(invalid_folder) is False


def test_validate_path_combined(path_verifier):
    # Valid file
    assert path_verifier._validate_path("/valid_path/to/important_project.txt", is_file=True) is True

    # Invalid file (bad keyword)
    assert path_verifier._validate_path("/valid_path/to/temp_project.txt", is_file=True) is False

    # Valid folder
    assert path_verifier._validate_path("/valid_path/to/project_folder", is_file=False) is True

    # Invalid folder (bad keyword)
    assert path_verifier._validate_path("/valid_path/to/backup_folder", is_file=False) is False
