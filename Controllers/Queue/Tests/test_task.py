import pytest
from Controllers.Queue.task import Task


### Tests ###
def test_task_initialization():
    """Test Task initialization with correct attributes."""
    payload = {
        "node_id": "12345",
        "trie": "mock_trie",
        "service": "mock_service",
    }
    task = Task(id="task1", type="traverse", payload=payload, retries=5)

    assert task.id == "task1"
    assert task.type == "traverse"
    assert task.payload == payload
    assert task.retries == 5
    assert task.status == "pending"


def test_task_mark_failed():
    """Test marking a task as failed and decrementing retries."""
    task = Task(id="task1", type="upload", payload={}, retries=3)

    # First failure
    task.mark_failed()
    assert task.retries == 2
    assert task.status == "pending"

    # Second failure
    task.mark_failed()
    assert task.retries == 1
    assert task.status == "pending"

    # Final failure
    task.mark_failed()
    assert task.retries == 0
    assert task.status == "failed"


def test_task_mark_completed():
    """Test marking a task as completed."""
    task = Task(id="task1", type="traverse", payload={}, retries=3)

    task.mark_completed()
    assert task.status == "completed"


def test_task_to_dict():
    """Test serialization of a Task to a dictionary."""
    payload = {
        "node_id": "12345",
        "trie": "mock_trie",
        "service": "mock_service",
    }
    task = Task(id="task1", type="traverse", payload=payload, retries=5)
    task_dict = task.to_dict()

    assert task_dict == {
        "id": "task1",
        "type": "traverse",
        "payload": payload,
        "retries": 5,
        "status": "pending",
    }


def test_task_lifecycle():
    """Test the full lifecycle of a Task: pending -> failed -> completed."""
    task = Task(id="task1", type="upload", payload={}, retries=2)

    # Task starts in "pending" status
    assert task.status == "pending"

    # Fail the task once
    task.mark_failed()
    assert task.status == "pending"
    assert task.retries == 1

    # Fail the task again
    task.mark_failed()
    assert task.status == "failed"
    assert task.retries == 0

    # Marking a failed task as completed (should still transition to completed)
    task.mark_completed()
    assert task.status == "completed"
