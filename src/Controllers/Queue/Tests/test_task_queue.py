import pytest
import asyncio
from Controllers.Queue.task_queue import TaskQueue


@pytest.fixture
def task_queue():
    """Fixture to initialize a TaskQueue instance."""
    return TaskQueue()


@pytest.mark.asyncio
async def test_add_task(task_queue):
    """Test adding tasks to the queue."""
    await task_queue.add_task("task1", {"data": "Task 1 Data"})
    await task_queue.add_task("task2", {"data": "Task 2 Data"})

    assert len(task_queue.queue) == 2
    assert "task1" in task_queue.task_status
    assert task_queue.task_status["task1"]["status"] == "pending"


@pytest.mark.asyncio
async def test_get_next_task(task_queue):
    """Test retrieving the next task from the queue."""
    await task_queue.add_task("task1", {"data": "Task 1 Data"})
    await task_queue.add_task("task2", {"data": "Task 2 Data"})

    task_id, task_data = await task_queue.get_next_task()
    assert task_id == "task1"
    assert task_data == {"data": "Task 1 Data"}
    assert task_queue.task_status["task1"]["status"] == "in_progress"

    task_id, task_data = await task_queue.get_next_task()
    assert task_id == "task2"
    assert task_data == {"data": "Task 2 Data"}
    assert task_queue.task_status["task2"]["status"] == "in_progress"

    # No more tasks available
    task_id, task_data = await task_queue.get_next_task()
    assert task_id is None
    assert task_data is None


@pytest.mark.asyncio
async def test_mark_task_completed(task_queue):
    """Test marking a task as completed."""
    await task_queue.add_task("task1", {"data": "Task 1 Data"})

    # Move task to "in_progress"
    await task_queue.get_next_task()

    await task_queue.mark_task_completed("task1")
    assert "task1" not in task_queue.task_status


@pytest.mark.asyncio
async def test_mark_task_failed_requeue(task_queue):
    """Test marking a task as failed and requeuing it."""
    await task_queue.add_task("task1", {"data": "Task 1 Data"})

    # Move task to "in_progress"
    await task_queue.get_next_task()

    await task_queue.mark_task_failed("task1", retry_limit=3)
    assert task_queue.task_status["task1"]["status"] == "pending"
    assert task_queue.task_status["task1"]["attempts"] == 1
    assert len(task_queue.queue) == 1  # Task requeued


@pytest.mark.asyncio
async def test_mark_task_failed_remove(task_queue):
    """Test marking a task as failed and removing it after exceeding retries."""
    await task_queue.add_task("task1", {"data": "Task 1 Data"})

    # Move task to "in_progress"
    await task_queue.get_next_task()

    # Fail the task multiple times
    for _ in range(3):
        await task_queue.mark_task_failed("task1", retry_limit=3)

    assert "task1" not in task_queue.task_status
    assert len(task_queue.queue) == 0  # Task not requeued


@pytest.mark.asyncio
async def test_get_status_summary(task_queue):
    """Test retrieving a summary of task statuses."""
    await task_queue.add_task("task1", {"data": "Task 1 Data"})
    await task_queue.add_task("task2", {"data": "Task 2 Data"})

    # Move task1 to "in_progress"
    await task_queue.get_next_task()

    # Mark task2 as failed
    await task_queue.mark_task_failed("task2", retry_limit=1)

    summary = await task_queue.get_status_summary()
    assert summary == {
        "pending": 0,
        "in_progress": 1,
        "completed": 0,
        "failed": 1,
    }
