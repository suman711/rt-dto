import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Column, DateTime, Enum, ForeignKey,
    Integer, String, JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class JobStatus(str, enum.Enum):
    """Top-level job lifecycle states."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskStatus(str, enum.Enum):
    """
    Individual task lifecycle states.
    Transitions: SCHEDULED → RUNNING → COMPLETED | FAILED | DEAD
    DEAD = permanently failed, moved to Dead Letter Queue.
    """
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD = "DEAD"


class TaskType(str, enum.Enum):
    """
    The three sequential task types for every fintech job.
    Order is enforced via sequence_order field (0 → 1 → 2).
    """
    VALIDATION = "VALIDATION"        # 0 — CPU intensive
    LEDGER_UPDATE = "LEDGER_UPDATE"  # 1 — I/O intensive
    NOTIFICATION = "NOTIFICATION"   # 2 — External API dependent


class Job(Base):
    """
    Represents a complete unit of work submitted by a client.
    Each job is decomposed into exactly 3 sequential tasks on creation.
    """
    __tablename__ = "jobs"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )
    status = Column(
        Enum(JobStatus),
        default=JobStatus.PENDING,
        nullable=False,
        index=True,  # Frequently queried for status polling
    )
    payload = Column(
        JSON,
        nullable=False,
        comment="Original client-submitted transaction data",
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    # One job → many tasks (always 3, in sequence)
    tasks = relationship(
        "Task",
        back_populates="job",
        order_by="Task.sequence_order",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} status={self.status}>"


class Task(Base):
    """
    Represents a single executable step within a Job.
    Tasks are chained: each starts only after the previous completes.

    Observability fields (started_at, completed_at) allow per-task
    latency tracking — critical for SLA monitoring in production.
    """
    __tablename__ = "tasks"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )
    job_id = Column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    task_type = Column(Enum(TaskType), nullable=False)
    sequence_order = Column(
        Integer,
        nullable=False,
        comment="Execution order within the job: 0=VALIDATION, 1=LEDGER_UPDATE, 2=NOTIFICATION",
    )
    status = Column(
        Enum(TaskStatus),
        default=TaskStatus.SCHEDULED,
        nullable=False,
        index=True,
    )
    retry_count = Column(
        Integer,
        default=0,
        nullable=False,
        comment="Incremented on each failure; task moves to DLQ at MAX_RETRIES",
    )

    # --- Observability: per-task latency tracking ---
    scheduled_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(
        String,
        nullable=True,
        comment="Last error message; overwritten on each retry",
    )

    job = relationship("Job", back_populates="tasks")

    @property
    def duration_seconds(self) -> float | None:
        """Returns task execution time in seconds, or None if not completed."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def __repr__(self) -> str:
        return (
            f"<Task id={self.id} type={self.task_type} "
            f"status={self.status} order={self.sequence_order}>"
        )