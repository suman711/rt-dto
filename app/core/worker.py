import asyncio
import uuid
from datetime import datetime

from sqlalchemy import select

from app.config import settings
from app.core.orchestrator import (
    redis_client,
    enqueue_task,
    send_to_dlq,
    reclaim_abandoned_tasks,
)
from app.core.task_handlers import execute_task
from app.db.session import AsyncSessionFactory
from app.models.job import Job, JobStatus, Task, TaskStatus
from app.observability.logger import get_logger

logger = get_logger(__name__)

# Unique ID per worker instance — used for Redis consumer group membership
WORKER_ID = f"worker-{str(uuid.uuid4())[:8]}"


# ---------------------------------------------------------------------------
# Main Worker Loop
# ---------------------------------------------------------------------------

async def run_worker():
    """
    Main worker loop. Pulls tasks from Redis Stream using consumer groups
    for distributed, at-least-once delivery with idempotency guards.

    Design decisions:
    - XREADGROUP ensures only one worker receives any given message (atomic)
    - Messages stay in the Pending Entries List (PEL) until XACK is called
    - If a worker dies, XAUTOCLAIM reclaims its pending messages after 60s
    - XACK is called only AFTER the DB commit — guarantees no silent data loss
    """
    logger.info(f"Worker {WORKER_ID} starting up")

    while True:
        try:
            # Block up to 5s waiting for new tasks
            # 5s timeout allows periodic reclaim checks and graceful shutdown
            messages = await redis_client.xreadgroup(
                groupname=settings.CONSUMER_GROUP,
                consumername=WORKER_ID,
                streams={settings.STREAM_NAME: ">"},  # ">" = only new messages
                count=settings.WORKER_CONCURRENCY,
                block=5000,
            )

            if messages:
                tasks_coroutines = []
                for stream, entries in messages:
                    for entry_id, data in entries:
                        tasks_coroutines.append(
                            _process_task(entry_id, data["task_id"])
                        )

                # Process all fetched tasks concurrently
                await asyncio.gather(*tasks_coroutines, return_exceptions=True)

            # Periodically reclaim tasks from dead workers
            await reclaim_abandoned_tasks(WORKER_ID)

        except asyncio.CancelledError:
            logger.info(f"Worker {WORKER_ID} shutting down gracefully")
            break
        except Exception as e:
            logger.error(f"Worker loop error: {e}", exc_info=True)
            await asyncio.sleep(1)  # Brief pause before retrying


# ---------------------------------------------------------------------------
# Single Task Processor
# ---------------------------------------------------------------------------

async def _process_task(stream_entry_id: str, task_id: str):
    """
    Executes a single task with:
    - Idempotency guard (skip if already completed)
    - State machine transitions (SCHEDULED → RUNNING → COMPLETED/FAILED)
    - Hard 30-second timeout enforcement
    - Retry logic with exponential backoff
    - DLQ promotion after MAX_RETRIES failures
    """
    async with AsyncSessionFactory() as db:
        # Load task with its parent job (needed for payload in handlers)
        result = await db.execute(
            select(Task).where(Task.id == task_id)
        )
        task = result.scalar_one_or_none()

        if not task:
            logger.warning(f"Task {task_id} not found in DB — skipping")
            await _ack(stream_entry_id)
            return

        # --- Idempotency guard ---
        # If task was already completed (e.g. duplicate delivery), skip safely
        if task.status in (TaskStatus.COMPLETED, TaskStatus.DEAD):
            logger.info(
                f"Task {task_id} already in terminal state "
                f"({task.status}) — skipping"
            )
            await _ack(stream_entry_id)
            return

        # Load parent job for payload access in handlers
        job = await db.get(Job, task.job_id)
        task.job = job  # Manually attach since we're in async context

        # --- Transition → RUNNING ---
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        await db.commit()

        logger.info(
            f"[Job:{task.job_id}] Task {task.task_type} "
            f"(order={task.sequence_order}) → RUNNING"
        )

        try:
            # --- Execute with hard timeout ---
            await asyncio.wait_for(
                execute_task(task, db),
                timeout=settings.TASK_TIMEOUT_SECONDS,
            )

            # --- Transition → COMPLETED ---
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            await db.commit()

            logger.info(
                f"[Job:{task.job_id}] Task {task.task_type} → COMPLETED "
                f"({task.duration_seconds:.2f}s)"
            )

            # XACK only after DB commit — ensures at-least-once with no data loss
            await _ack(stream_entry_id)

            # Chain the next task in sequence
            await _chain_next_task(task, db)

        except asyncio.TimeoutError:
            await _handle_failure(
                task, db, stream_entry_id,
                f"Task timed out after {settings.TASK_TIMEOUT_SECONDS}s"
            )
        except Exception as e:
            await _handle_failure(task, db, stream_entry_id, str(e))


# ---------------------------------------------------------------------------
# Failure Handler
# ---------------------------------------------------------------------------

async def _handle_failure(
    task: Task,
    db,
    stream_entry_id: str,
    reason: str,
):
    """
    Handles task failure with retry logic:
    - Increments retry_count
    - Re-enqueues with exponential backoff if under MAX_RETRIES
    - Promotes to DLQ and marks job FAILED if MAX_RETRIES exceeded
    """
    task.retry_count += 1
    task.error_message = reason
    task.status = TaskStatus.FAILED

    logger.warning(
        f"[Job:{task.job_id}] Task {task.task_type} failed "
        f"(attempt {task.retry_count}/{settings.MAX_RETRIES}): {reason}"
    )

    if task.retry_count >= settings.MAX_RETRIES:
        # --- Promote to Dead Letter Queue ---
        task.status = TaskStatus.DEAD
        await db.commit()

        await send_to_dlq(str(task.id), reason)
        await _update_job_status(task.job_id, JobStatus.FAILED, db)

        logger.error(
            f"[Job:{task.job_id}] Task {task.task_type} → DLQ "
            f"after {settings.MAX_RETRIES} attempts"
        )
    else:
        # --- Re-enqueue with exponential backoff ---
        task.status = TaskStatus.SCHEDULED
        await db.commit()

        backoff_seconds = 2 ** task.retry_count  # 2s, 4s, 8s
        logger.info(
            f"[Job:{task.job_id}] Retrying {task.task_type} "
            f"in {backoff_seconds}s (attempt {task.retry_count})"
        )
        await asyncio.sleep(backoff_seconds)
        await enqueue_task(str(task.id))

    # ACK the original message regardless — prevents infinite redelivery
    # The retry is handled by a fresh enqueue above
    await _ack(stream_entry_id)


# ---------------------------------------------------------------------------
# Task Chaining
# ---------------------------------------------------------------------------

async def _chain_next_task(completed_task: Task, db):
    """
    After a task completes, enqueues the next task in sequence.
    If no next task exists, the job is fully complete.

    This enforces the dependency: Task B only starts after Task A succeeds.
    """
    result = await db.execute(
        select(Task).where(
            Task.job_id == completed_task.job_id,
            Task.sequence_order == completed_task.sequence_order + 1,
        )
    )
    next_task = result.scalar_one_or_none()

    if next_task:
        await enqueue_task(str(next_task.id))
        logger.info(
            f"[Job:{completed_task.job_id}] Chained → {next_task.task_type}"
        )
    else:
        # All 3 tasks done — mark job COMPLETED
        await _update_job_status(completed_task.job_id, JobStatus.COMPLETED, db)
        logger.info(
            f"[Job:{completed_task.job_id}] All tasks complete → Job COMPLETED"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _ack(stream_entry_id: str):
    """Acknowledges a Redis Stream message, removing it from the PEL."""
    await redis_client.xack(
        settings.STREAM_NAME,
        settings.CONSUMER_GROUP,
        stream_entry_id,
    )


async def _update_job_status(job_id, status: JobStatus, db):
    """Updates the parent job's status in the database."""
    job = await db.get(Job, job_id)
    if job:
        job.status = status
        job.updated_at = datetime.utcnow()
        await db.commit()