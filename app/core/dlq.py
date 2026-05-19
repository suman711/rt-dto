import asyncio
from app.config import settings
from app.core.orchestrator import redis_client
from app.db.session import AsyncSessionFactory
from app.models.job import Task, TaskStatus
from app.observability.logger import get_logger

logger = get_logger(__name__)

DLQ_CONSUMER_GROUP = settings.CONSUMER_GROUP + "_dlq"
DLQ_CONSUMER_NAME = "dlq-inspector"


# ---------------------------------------------------------------------------
# DLQ Monitor Loop
# ---------------------------------------------------------------------------

async def run_dlq_monitor():
    """
    Continuously monitors the Dead Letter Queue stream.
    Logs all DLQ entries for manual inspection.

    In production this would:
    - Send alerts to PagerDuty / Slack
    - Write to a separate audit table
    - Expose metrics to Prometheus
    """
    logger.info("DLQ monitor starting up")

    while True:
        try:
            messages = await redis_client.xreadgroup(
                groupname=DLQ_CONSUMER_GROUP,
                consumername=DLQ_CONSUMER_NAME,
                streams={settings.DLQ_STREAM: ">"},
                count=10,
                block=5000,
            )

            if messages:
                for stream, entries in messages:
                    for entry_id, data in entries:
                        await _inspect_dlq_entry(entry_id, data)

        except asyncio.CancelledError:
            logger.info("DLQ monitor shutting down gracefully")
            break
        except Exception as e:
            logger.error(f"DLQ monitor error: {e}", exc_info=True)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# DLQ Entry Inspector
# ---------------------------------------------------------------------------

async def _inspect_dlq_entry(entry_id: str, data: dict):
    """
    Processes a single DLQ entry:
    - Logs full context for manual review
    - Acknowledges the message (it stays in the stream for replay if needed)
    """
    task_id = data.get("task_id")
    reason = data.get("reason", "Unknown")

    logger.error(
        f"DLQ entry detected — task_id={task_id} reason={reason}",
        extra={"dlq": True, "task_id": task_id, "failure_reason": reason},
    )

    # Enrich with DB context
    async with AsyncSessionFactory() as db:
        task = await db.get(Task, task_id)
        if task:
            logger.error(
                f"DLQ task detail — job_id={task.job_id} "
                f"type={task.task_type} retries={task.retry_count} "
                f"last_error={task.error_message}"
            )

    # ACK so the message doesn't keep reappearing in the monitor
    await redis_client.xack(settings.DLQ_STREAM, DLQ_CONSUMER_GROUP, entry_id)


# ---------------------------------------------------------------------------
# Manual Replay Utility
# ---------------------------------------------------------------------------

async def replay_dlq_task(task_id: str):
    """
    Manually re-enqueues a DLQ task for another attempt.
    Called by an operator after fixing the underlying issue.

    Resets retry_count so the task gets a fresh set of MAX_RETRIES attempts.
    """
    async with AsyncSessionFactory() as db:
        task = await db.get(Task, task_id)

        if not task:
            logger.error(f"Replay failed: task {task_id} not found")
            return False

        if task.status != TaskStatus.DEAD:
            logger.warning(
                f"Replay skipped: task {task_id} is not in DEAD state "
                f"(current: {task.status})"
            )
            return False

        # Reset for fresh attempt
        task.status = TaskStatus.SCHEDULED
        task.retry_count = 0
        task.error_message = None
        await db.commit()

        # Re-enqueue onto the main stream
        from app.core.orchestrator import enqueue_task
        await enqueue_task(task_id)

        logger.info(f"Task {task_id} replayed from DLQ → re-enqueued")
        return True