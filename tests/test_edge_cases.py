import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from app.models.job import TaskStatus, JobStatus


class TestEdgeCases:
    """
    Tests for timeout enforcement, DLQ promotion, and retry logic.
    These are the critical edge cases called out in the assignment.
    """

    @pytest.mark.asyncio
    async def test_task_timeout_triggers_failure(self, sample_task, mock_db):
        """
        If task execution exceeds 30s, it must be killed and marked FAILED.
        This is the core timeout requirement from the assignment.
        """
        from app.core.worker import _process_task

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_task
        mock_db.execute = AsyncMock(return_value=mock_result)
        mock_db.get = AsyncMock(return_value=sample_task.job)

        async def slow_task(*args, **kwargs):
            await asyncio.sleep(999)  # Simulates task exceeding timeout

        with patch("app.core.worker.AsyncSessionFactory") as mock_factory, \
             patch("app.core.worker.execute_task", side_effect=slow_task), \
             patch("app.core.worker.settings") as mock_settings, \
             patch("app.core.worker._handle_failure", new_callable=AsyncMock) as mock_failure, \
             patch("app.core.worker._ack", new_callable=AsyncMock):

            mock_settings.TASK_TIMEOUT_SECONDS = 0.01  # 10ms timeout for test speed
            mock_settings.MAX_RETRIES = 3
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            await _process_task("entry-123", str(sample_task.id))

            # _handle_failure must be called with timeout message
            mock_failure.assert_called_once()
            call_args = mock_failure.call_args[0]
            assert "timed out" in call_args[3].lower()

    @pytest.mark.asyncio
    async def test_task_promoted_to_dlq_after_max_retries(self, sample_task, mock_db):
        """
        A task that fails MAX_RETRIES times must be moved to DLQ
        and its status set to DEAD. This is explicitly required by the assignment.
        """
        from app.core.worker import _handle_failure

        sample_task.retry_count = 2  # One more failure = MAX_RETRIES (3)
        sample_task.status = TaskStatus.RUNNING

        with patch("app.core.worker.send_to_dlq", new_callable=AsyncMock) as mock_dlq, \
             patch("app.core.worker._update_job_status", new_callable=AsyncMock) as mock_update, \
             patch("app.core.worker.settings") as mock_settings, \
             patch("app.core.worker._ack", new_callable=AsyncMock):

            mock_settings.MAX_RETRIES = 3

            await _handle_failure(
                sample_task, mock_db, "entry-123", "Simulated failure"
            )

            # Task must be marked DEAD
            assert sample_task.status == TaskStatus.DEAD

            # Must be sent to DLQ
            mock_dlq.assert_called_once_with(
                str(sample_task.id), "Simulated failure"
            )

            # Parent job must be marked FAILED
            mock_update.assert_called_once_with(
                sample_task.job_id, JobStatus.FAILED, mock_db
            )

    @pytest.mark.asyncio
    async def test_task_retried_before_dlq(self, sample_task, mock_db):
        """
        A task that fails but hasn't hit MAX_RETRIES must be re-enqueued,
        not sent to DLQ. Retry count must increment.
        """
        from app.core.worker import _handle_failure

        sample_task.retry_count = 0  # First failure
        sample_task.status = TaskStatus.RUNNING

        with patch("app.core.worker.send_to_dlq", new_callable=AsyncMock) as mock_dlq, \
             patch("app.core.worker.enqueue_task", new_callable=AsyncMock) as mock_enqueue, \
             patch("app.core.worker.settings") as mock_settings, \
             patch("app.core.worker._ack", new_callable=AsyncMock):

            mock_settings.MAX_RETRIES = 3

            await _handle_failure(
                sample_task, mock_db, "entry-123", "Transient error"
            )

            # Must NOT go to DLQ yet
            mock_dlq.assert_not_called()

            # Must be re-enqueued for retry
            mock_enqueue.assert_called_once_with(str(sample_task.id))

            # Retry count must have incremented
            assert sample_task.retry_count == 1

    @pytest.mark.asyncio
    async def test_dlq_replay_resets_retry_count(self, mock_db):
        """
        Replaying a DLQ task must reset retry_count to 0
        so it gets a fresh set of attempts.
        """
        from app.core.dlq import replay_dlq_task

        dead_task = MagicMock()
        dead_task.id = uuid4()
        dead_task.status = TaskStatus.DEAD
        dead_task.retry_count = 3

        mock_db.get = AsyncMock(return_value=dead_task)

        with patch("app.core.dlq.AsyncSessionFactory") as mock_factory, \
            patch("app.core.orchestrator.enqueue_task", new_callable=AsyncMock), \
            patch("app.core.worker.enqueue_task", new_callable=AsyncMock):

            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await replay_dlq_task(str(dead_task.id))

            assert result is True
            assert dead_task.retry_count == 0
            assert dead_task.status == TaskStatus.SCHEDULED

    @pytest.mark.asyncio
    async def test_validation_rejects_negative_amount(self):
        """
        Validation handler must reject transactions with amount <= 0.
        This enforces the business rule in task_handlers.py.
        """
        from app.core.task_handlers import _cpu_bound_validation

        with pytest.raises(ValueError, match="invalid amount"):
            _cpu_bound_validation({"account_id": "ACC-001", "amount": -100})

    @pytest.mark.asyncio
    async def test_validation_rejects_missing_account_id(self):
        """Validation must reject payloads without account_id."""
        from app.core.task_handlers import _cpu_bound_validation

        with pytest.raises(ValueError, match="missing account_id"):
            _cpu_bound_validation({"amount": 500})

    @pytest.mark.asyncio
    async def test_validation_rejects_excessive_amount(self):
        """Validation must reject amounts over the single-transaction limit."""
        from app.core.task_handlers import _cpu_bound_validation

        with pytest.raises(ValueError, match="exceeds single-transaction limit"):
            _cpu_bound_validation({"account_id": "ACC-001", "amount": 2_000_000})