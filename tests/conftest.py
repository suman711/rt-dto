import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4
from datetime import datetime

from app.models.job import Job, Task, JobStatus, TaskStatus, TaskType


# ---------------------------------------------------------------------------
# Event Loop
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def event_loop():
    """Single event loop for the entire test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# ---------------------------------------------------------------------------
# Mock Database Session
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    """
    Mock AsyncSession that captures adds and commits without a real DB.
    """
    db = AsyncMock()
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.get = AsyncMock()
    db.execute = AsyncMock()
    return db


# ---------------------------------------------------------------------------
# Sample Data Factories
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_payload():
    return {
        "account_id": "ACC-TEST-001",
        "amount": 500.00,
        "currency": "USD",
        "transaction_type": "DEBIT",
    }


@pytest.fixture
def sample_job(sample_payload):
    job = MagicMock(spec=Job)
    job.id = uuid4()
    job.status = JobStatus.PENDING
    job.payload = sample_payload
    job.created_at = datetime.utcnow()
    job.updated_at = datetime.utcnow()
    return job


@pytest.fixture
def sample_task(sample_job):
    task = MagicMock(spec=Task)
    task.id = uuid4()
    task.job_id = sample_job.id
    task.job = sample_job
    task.task_type = TaskType.VALIDATION
    task.sequence_order = 0
    task.status = TaskStatus.SCHEDULED
    task.retry_count = 0
    task.scheduled_at = datetime.utcnow()
    task.started_at = None
    task.completed_at = None
    task.error_message = None
    task.duration_seconds = None
    return task


@pytest.fixture
def sample_ledger_task(sample_job):
    task = MagicMock(spec=Task)
    task.id = uuid4()
    task.job_id = sample_job.id
    task.job = sample_job
    task.task_type = TaskType.LEDGER_UPDATE
    task.sequence_order = 1
    task.status = TaskStatus.SCHEDULED
    task.retry_count = 0
    task.scheduled_at = datetime.utcnow()
    task.started_at = None
    task.completed_at = None
    task.error_message = None
    task.duration_seconds = None
    return task


@pytest.fixture
def sample_notification_task(sample_job):
    task = MagicMock(spec=Task)
    task.id = uuid4()
    task.job_id = sample_job.id
    task.job = sample_job
    task.task_type = TaskType.NOTIFICATION
    task.sequence_order = 2
    task.status = TaskStatus.SCHEDULED
    task.retry_count = 0
    task.scheduled_at = datetime.utcnow()
    task.started_at = None
    task.completed_at = None
    task.error_message = None
    task.duration_seconds = None
    return task