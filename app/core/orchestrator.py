import redis.asyncio as aioredis

from app.config import settings
from app.observability.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Redis client — single shared async connection pool
# ---------------------------------------------------------------------------

redis_client = aioredis.from_url(
    settings.REDIS_URL,
    encoding="utf-8",
    decode_responses=True,  # Always return str, not bytes
)


# ---------------------------------------------------------------------------
# Consumer Group Bootstrap
# ---------------------------------------------------------------------------

async def ensure_consumer_groups():
    """
    Idempotently creates Redis consumer groups for the task stream and DLQ.
    Called once at application startup before workers begin consuming.

    id="0" means the group will read from the beginning of the stream,
    ensuring no messages are missed if the app restarts.
    mkstream=True creates the stream if it doesn't exist yet.
    """
    for stream, group in [
        (settings.STREAM_NAME, settings.CONSUMER_GROUP),
        (settings.DLQ_STREAM, settings.CONSUMER_GROUP + "_dlq"),
    ]:
        try:
            await redis_client.xgroup_create(
                stream, group, id="0", mkstream=True
            )
            logger.info(f"Consumer group '{group}' created on stream '{stream}'")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                # Group already exists — this is expected on restart
                logger.info(f"Consumer group '{group}' already exists, skipping")
            else:
                logger.error(f"Failed to create consumer group '{group}': {e}")
                raise


# ---------------------------------------------------------------------------
# Task Enqueueing
# ---------------------------------------------------------------------------

async def enqueue_task(task_id: str):
    """
    Pushes a task onto the Redis Stream for worker pickup.
    Used for: first task of a new job, retries, and chaining next tasks.

    XADD appends a new message to the stream with an auto-generated
    message ID (timestamp-sequence). The worker reads via XREADGROUP.
    """
    message_id = await redis_client.xadd(
        settings.STREAM_NAME,
        {"task_id": task_id},
    )
    logger.info(f"Task {task_id} enqueued → stream message {message_id}")
    return message_id


async def send_to_dlq(task_id: str, reason: str):
    """
    Moves a permanently failed task to the Dead Letter Queue stream.
    DLQ entries are for manual inspection — they are never auto-retried.
    """
    message_id = await redis_client.xadd(
        settings.DLQ_STREAM,
        {
            "task_id": task_id,
            "reason": reason,
        },
    )
    logger.error(
        f"Task {task_id} sent to DLQ (reason: {reason}) → message {message_id}"
    )
    return message_id


# ---------------------------------------------------------------------------
# Dead Worker Recovery
# ---------------------------------------------------------------------------

async def reclaim_abandoned_tasks(worker_id: str):
    """
    Reclaims messages that were delivered to a worker but never acknowledged
    (i.e., the worker died mid-task).

    XAUTOCLAIM transfers ownership of messages idle for more than
    60 seconds to the calling worker, so they get re-processed.

    This is the core of our fault-tolerance strategy — no task is ever
    permanently lost due to a worker crash.
    """
    try:
        result = await redis_client.xautoclaim(
            settings.STREAM_NAME,
            settings.CONSUMER_GROUP,
            worker_id,
            min_idle_time=60000,  # 60 seconds in milliseconds
            start_id="0-0",
            count=10,
        )
        # result is (next_start_id, messages, deleted_ids)
        messages = result[1] if result else []
        if messages:
            logger.info(
                f"Worker {worker_id} reclaimed {len(messages)} abandoned task(s)"
            )
        return messages
    except Exception as e:
        logger.error(f"Failed to reclaim abandoned tasks: {e}")
        return []