from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.job import JobStatus, TaskStatus, TaskType


# ---------------------------------------------------------------------------
# Request Schemas (Client → API)
# ---------------------------------------------------------------------------

class JobSubmitRequest(BaseModel):
    """
    Payload submitted by the client to create a new job.
    The payload field carries the raw transaction data.
    """
    payload: dict[str, Any] = Field(
        ...,
        description="Transaction data to process (amount, account_id, etc.)",
        examples=[{
            "account_id": "ACC-001",
            "amount": 1500.00,
            "currency": "USD",
            "transaction_type": "DEBIT",
        }],
    )


# ---------------------------------------------------------------------------
# Response Schemas (API → Client)
# ---------------------------------------------------------------------------

class TaskResponse(BaseModel):
    """Represents a single task's state — embedded inside JobStatusResponse."""
    id: UUID
    task_type: TaskType
    sequence_order: int
    status: TaskStatus
    retry_count: int
    scheduled_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
    duration_seconds: float | None

    class Config:
        from_attributes = True  # Allows building from SQLAlchemy ORM objects


class JobSubmitResponse(BaseModel):
    """
    Returned immediately after job submission (HTTP 202 Accepted).
    Client uses job_id to poll for status via GET /jobs/{job_id}.
    """
    job_id: UUID
    status: JobStatus
    message: str = "Job accepted and queued for processing"


class JobStatusResponse(BaseModel):
    """
    Full job status including all task states.
    Returned by GET /jobs/{job_id}.
    """
    job_id: UUID
    status: JobStatus
    payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    tasks: list[TaskResponse]

    class Config:
        from_attributes = True


class ErrorResponse(BaseModel):
    """Standard error envelope for all 4xx/5xx responses."""
    detail: str
    error_code: str | None = None