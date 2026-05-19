from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.orchestrator import enqueue_task
from app.db.session import get_session
from app.models.job import Job, JobStatus, Task, TaskStatus, TaskType
from app.models.schemas import (
    JobSubmitRequest,
    JobSubmitResponse,
    JobStatusResponse,
    TaskResponse,
)
from app.observability.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/jobs", tags=["Jobs"])

# Sequential task pipeline — order is enforced via sequence_order
TASK_PIPELINE = [
    TaskType.VALIDATION,      # 0 — must complete before ledger update
    TaskType.LEDGER_UPDATE,   # 1 — must complete before notification
    TaskType.NOTIFICATION,    # 2 — final step
]


# ---------------------------------------------------------------------------
# POST /jobs — Submit a new job
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=JobSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a new job",
    description=(
        "Accepts a transaction payload and decomposes it into 3 sequential tasks: "
        "VALIDATION → LEDGER_UPDATE → NOTIFICATION. "
        "Returns immediately with a job_id for status polling."
    ),
)
async def submit_job(
    request: JobSubmitRequest,
    db: AsyncSession = Depends(get_session),
):
    """
    Dispatcher endpoint — the entry point for all job submissions.

    Design:
    - Creates Job + all 3 Tasks atomically in a single transaction
    - Only enqueues the FIRST task; workers chain the rest on completion
    - Returns 202 Accepted immediately (non-blocking)
    """
    # --- Create Job ---
    job = Job(
        payload=request.payload,
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.flush()  # Get job.id without committing yet

    # --- Decompose into sequential tasks ---
    tasks = []
    for order, task_type in enumerate(TASK_PIPELINE):
        task = Task(
            job_id=job.id,
            task_type=task_type,
            sequence_order=order,
            status=TaskStatus.SCHEDULED,
        )
        db.add(task)
        tasks.append(task)

    # Commit job + all tasks atomically
    # If this fails, nothing is enqueued — no orphaned Redis messages
    await db.commit()

    logger.info(
        f"Job {job.id} created with {len(tasks)} tasks — "
        f"payload keys: {list(request.payload.keys())}"
    )

    # --- Enqueue only the first task ---
    # Workers chain tasks 1 and 2 after each preceding task completes
    await enqueue_task(str(tasks[0].id))

    logger.info(
        f"Job {job.id} → first task {tasks[0].id} "
        f"({TaskType.VALIDATION}) enqueued"
    )

    return JobSubmitResponse(
        job_id=job.id,
        status=job.status,
    )


# ---------------------------------------------------------------------------
# GET /jobs/{job_id} — Poll job status
# ---------------------------------------------------------------------------

@router.get(
    "/{job_id}",
    response_model=JobStatusResponse,
    summary="Get job status",
    description=(
        "Returns the current status of a job and all its tasks. "
        "Poll this endpoint to track progress after submission."
    ),
)
async def get_job_status(
    job_id: str,
    db: AsyncSession = Depends(get_session),
):
    """
    Status polling endpoint.
    Returns full job detail including per-task states and latencies.
    """
    job = await db.get(Job, job_id)

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    # Load tasks ordered by sequence
    result = await db.execute(
        select(Task)
        .where(Task.job_id == job_id)
        .order_by(Task.sequence_order)
    )
    tasks = result.scalars().all()

    task_responses = [
        TaskResponse(
            id=task.id,
            task_type=task.task_type,
            sequence_order=task.sequence_order,
            status=task.status,
            retry_count=task.retry_count,
            scheduled_at=task.scheduled_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error_message=task.error_message,
            duration_seconds=task.duration_seconds,
        )
        for task in tasks
    ]

    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        payload=job.payload,
        created_at=job.created_at,
        updated_at=job.updated_at,
        tasks=task_responses,
    )


# ---------------------------------------------------------------------------
# GET /jobs/{job_id}/tasks — List all tasks for a job
# ---------------------------------------------------------------------------

@router.get(
    "/{job_id}/tasks",
    response_model=list[TaskResponse],
    summary="List tasks for a job",
)
async def list_job_tasks(
    job_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Returns all tasks for a job ordered by sequence."""
    result = await db.execute(
        select(Task)
        .where(Task.job_id == job_id)
        .order_by(Task.sequence_order)
    )
    tasks = result.scalars().all()

    if not tasks:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No tasks found for job {job_id}",
        )

    return [
        TaskResponse(
            id=task.id,
            task_type=task.task_type,
            sequence_order=task.sequence_order,
            status=task.status,
            retry_count=task.retry_count,
            scheduled_at=task.scheduled_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error_message=task.error_message,
            duration_seconds=task.duration_seconds,
        )
        for task in tasks
    ]