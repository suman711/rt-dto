import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from app.models.job import TaskStatus, TaskType, JobStatus


class TestWorker:
    """Tests for worker task processing logic."""

    @pytest.mark.asyncio
    async def test_task_transitions_to_running(self, mock_db):
        """Worker must transition task to RUNNING before executing."""
        from app.core.worker import _process_task
        from app.models.job import TaskType, TaskStatus
        from uuid import uuid4

        class SimpleTask:
            id = uuid4()
            job_id = uuid4()
            task_type = TaskType.VALIDATION
            sequence_order = 0
            status = TaskStatus.SCHEDULED
            retry_count = 0
            started_at = None
            completed_at = None
            error_message = None
            duration_seconds = 1.0

        task = SimpleTask()

        class SimpleJob:
            id = task.job_id
            payload = {"account_id": "ACC-001", "amount": 500.0}

        task.job = SimpleJob()

        # Capture status at the moment execute_task is called
        status_during_execution = []

        async def capture_status(*args, **kwargs):
            status_during_execution.append(task.status)

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = task
        mock_db.execute = AsyncMock(return_value=mock_result)
        mock_db.get = AsyncMock(return_value=task.job)

        with patch("app.core.worker.AsyncSessionFactory") as mock_factory, \
            patch("app.core.worker.execute_task", side_effect=capture_status), \
            patch("app.core.worker._ack", new_callable=AsyncMock), \
            patch("app.core.worker._chain_next_task", new_callable=AsyncMock):

            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            await _process_task("entry-123", str(task.id))

            # Status must have been RUNNING when execute_task was called
            assert len(status_during_execution) == 1
            assert status_during_execution[0] == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_completed_task_is_skipped(self, sample_task, mock_db):
        """Idempotency: already-completed tasks must not be re-executed."""
        from app.core.worker import _process_task

        sample_task.status = TaskStatus.COMPLETED

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_task
        mock_db.execute = AsyncMock(return_value=mock_result)

        with patch("app.core.worker.AsyncSessionFactory") as mock_factory, \
             patch("app.core.worker.execute_task", new_callable=AsyncMock) as mock_execute, \
             patch("app.core.worker._ack", new_callable=AsyncMock):

            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            await _process_task("entry-123", str(sample_task.id))

            # execute_task must NOT be called for already-completed tasks
            mock_execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_task_is_skipped(self, mock_db):
        """Worker must handle gracefully when task_id doesn't exist in DB."""
        from app.core.worker import _process_task

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None  # Not found
        mock_db.execute = AsyncMock(return_value=mock_result)

        with patch("app.core.worker.AsyncSessionFactory") as mock_factory, \
             patch("app.core.worker.execute_task", new_callable=AsyncMock) as mock_execute, \
             patch("app.core.worker._ack", new_callable=AsyncMock):

            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            # Should not raise — just skip and ack
            await _process_task("entry-123", str(uuid4()))
            mock_execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_next_task_chained_after_completion(self, sample_task, mock_db):
        """After task completes, next task in sequence must be enqueued."""
        from app.core.worker import _chain_next_task
        from app.models.job import Task

        next_task = MagicMock(spec=Task)
        next_task.id = uuid4()
        next_task.sequence_order = 1

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = next_task
        mock_db.execute = AsyncMock(return_value=mock_result)

        with patch("app.core.worker.enqueue_task", new_callable=AsyncMock) as mock_enqueue:
            await _chain_next_task(sample_task, mock_db)
            mock_enqueue.assert_called_once_with(str(next_task.id))

    @pytest.mark.asyncio
    async def test_job_completed_when_no_next_task(self, sample_task, mock_db):
        """When no next task exists, job status must be set to COMPLETED."""
        from app.core.worker import _chain_next_task

        # No next task
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with patch("app.core.worker._update_job_status", new_callable=AsyncMock) as mock_update:
            await _chain_next_task(sample_task, mock_db)
            mock_update.assert_called_once_with(
                sample_task.job_id, JobStatus.COMPLETED, mock_db
            )