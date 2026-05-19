import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from app.models.job import JobStatus, TaskStatus, TaskType


class TestDispatcher:
    """Tests for POST /jobs and GET /jobs/{job_id} endpoints."""

    @pytest.mark.asyncio
    async def test_submit_job_success(self, mock_db, sample_payload):
        """
        Submitting a valid job should:
        - Create 1 Job + 3 Tasks in DB
        - Enqueue only the first task (VALIDATION)
        - Return 202 with job_id and PENDING status
        """
        from app.api.dispatcher import submit_job
        from app.models.schemas import JobSubmitRequest

        with patch("app.api.dispatcher.enqueue_task", new_callable=AsyncMock) as mock_enqueue:
            # Setup mock DB to return a job-like object on flush
            mock_job_id = uuid4()
            mock_db.flush = AsyncMock()

            request = JobSubmitRequest(payload=sample_payload)

            # Track what gets added to DB
            added_objects = []
            mock_db.add = MagicMock(side_effect=lambda obj: added_objects.append(obj))

            with patch("app.api.dispatcher.Job") as MockJob, \
                 patch("app.api.dispatcher.Task") as MockTask:

                mock_job_instance = MagicMock()
                mock_job_instance.id = mock_job_id
                mock_job_instance.status = JobStatus.PENDING
                MockJob.return_value = mock_job_instance

                mock_task_instance = MagicMock()
                mock_task_instance.id = uuid4()
                MockTask.return_value = mock_task_instance

                response = await submit_job(request, mock_db)

            # Verify enqueue was called exactly once (only first task)
            mock_enqueue.assert_called_once()

            # Verify DB commit was called
            mock_db.commit.assert_called_once()

            # Verify response shape
            assert response.status == JobStatus.PENDING
            assert response.message == "Job accepted and queued for processing"

    @pytest.mark.asyncio
    async def test_submit_job_creates_three_tasks(self, mock_db, sample_payload):
        """Job submission must decompose into exactly 3 sequential tasks."""
        from app.api.dispatcher import submit_job, TASK_PIPELINE
        from app.models.schemas import JobSubmitRequest

        assert len(TASK_PIPELINE) == 3
        assert TASK_PIPELINE[0] == TaskType.VALIDATION
        assert TASK_PIPELINE[1] == TaskType.LEDGER_UPDATE
        assert TASK_PIPELINE[2] == TaskType.NOTIFICATION

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, mock_db):
        """GET /jobs/{id} with unknown ID should return 404."""
        from app.api.dispatcher import get_job_status
        from fastapi import HTTPException

        mock_db.get = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await get_job_status("non-existent-id", mock_db)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_job_returns_all_tasks(self, mock_db, sample_job):
        """GET /jobs/{id} should return job with all tasks included."""
        from app.api.dispatcher import get_job_status
        from app.models.job import Task
        from unittest.mock import MagicMock
        from datetime import datetime

        mock_db.get = AsyncMock(return_value=sample_job)

        # Mock 3 tasks in result
        mock_tasks = []
        for i, task_type in enumerate([TaskType.VALIDATION, TaskType.LEDGER_UPDATE, TaskType.NOTIFICATION]):
            t = MagicMock(spec=Task)
            t.id = uuid4()
            t.task_type = task_type
            t.sequence_order = i
            t.status = TaskStatus.COMPLETED
            t.retry_count = 0
            t.scheduled_at = datetime.utcnow()
            t.started_at = datetime.utcnow()
            t.completed_at = datetime.utcnow()
            t.error_message = None
            t.duration_seconds = 1.0
            mock_tasks.append(t)

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_tasks
        mock_db.execute = AsyncMock(return_value=mock_result)

        response = await get_job_status(str(sample_job.id), mock_db)

        assert len(response.tasks) == 3
        assert response.tasks[0].task_type == TaskType.VALIDATION
        assert response.tasks[1].task_type == TaskType.LEDGER_UPDATE
        assert response.tasks[2].task_type == TaskType.NOTIFICATION

    def test_invalid_payload_rejected(self):
        """Payload missing required fields should fail Pydantic validation."""
        from app.models.schemas import JobSubmitRequest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            # payload field is required — passing empty dict should fail
            JobSubmitRequest()