import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Task, TaskType
from app.observability.logger import get_logger

logger = get_logger(__name__)

# Shared thread pool for CPU-bound work
# Keeps CPU tasks off the event loop without blocking async coroutines
_thread_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="cpu-worker")


# ---------------------------------------------------------------------------
# Task Router
# ---------------------------------------------------------------------------

async def execute_task(task: Task, db: AsyncSession):
    """
    Routes a task to its specific handler based on task_type.
    Raises an exception on failure — the worker catches and handles retries.
    """
    handlers = {
        TaskType.VALIDATION: _handle_validation,
        TaskType.LEDGER_UPDATE: _handle_ledger_update,
        TaskType.NOTIFICATION: _handle_notification,
    }

    handler = handlers.get(task.task_type)
    if not handler:
        raise ValueError(f"Unknown task type: {task.task_type}")

    logger.info(
        f"[Job:{task.job_id}] Executing {task.task_type} "
        f"(attempt {task.retry_count + 1})"
    )

    await handler(task, db)


# ---------------------------------------------------------------------------
# Handler 1: Validation (CPU-intensive)
# ---------------------------------------------------------------------------

async def _handle_validation(task: Task, db: AsyncSession):
    """
    Simulates CPU-intensive validation work:
    - Schema validation
    - Fraud scoring
    - Compliance/AML checks

    Runs in a ThreadPoolExecutor so it doesn't block the event loop
    while other tasks continue processing concurrently.
    """
    loop = asyncio.get_event_loop()

    # Offload CPU work to thread pool
    await loop.run_in_executor(
        _thread_pool,
        _cpu_bound_validation,
        task.job.payload,
    )

    logger.info(f"[Job:{task.job_id}] Validation passed")


def _cpu_bound_validation(payload: dict):
    """
    Runs synchronously in a thread — safe for CPU-intensive work.
    In production: integrate with fraud scoring models, schema validators.
    """
    import time

    # Simulate variable CPU processing time (0.5s – 2.0s)
    processing_time = random.uniform(0.5, 2.0)
    time.sleep(processing_time)

    # --- Business validation rules ---
    if not payload.get("account_id"):
        raise ValueError("Validation failed: missing account_id")

    amount = payload.get("amount", 0)
    if amount <= 0:
        raise ValueError(f"Validation failed: invalid amount {amount}")

    if amount > 1_000_000:
        raise ValueError(
            f"Validation failed: amount {amount} exceeds single-transaction limit"
        )

    # Simulate occasional transient validation failures (3% rate)
    # Tests retry logic without making failures too frequent
    if random.random() < 0.03:
        raise RuntimeError("Validation service intermittent error — retry eligible")


# ---------------------------------------------------------------------------
# Handler 2: Ledger Update (I/O-intensive)
# ---------------------------------------------------------------------------

async def _handle_ledger_update(task: Task, db: AsyncSession):
    """
    Simulates I/O-intensive ledger write operations:
    - Debit/credit balance updates
    - Transaction record insertion
    - Double-entry bookkeeping

    Uses asyncio natively — no thread pool needed for I/O.
    In production: execute raw SQL or ORM writes against the ledger DB.
    """
    payload = task.job.payload
    amount = payload.get("amount", 0)
    account_id = payload.get("account_id", "UNKNOWN")

    # Simulate async DB write latency (0.2s – 1.5s)
    await asyncio.sleep(random.uniform(0.2, 1.5))

    # Simulate occasional DB connection issues (5% rate)
    if random.random() < 0.05:
        raise ConnectionError(
            f"Ledger DB connection timeout while updating account {account_id}"
        )

    logger.info(
        f"[Job:{task.job_id}] Ledger updated — "
        f"account={account_id} amount={amount}"
    )


# ---------------------------------------------------------------------------
# Handler 3: Notification (External API-dependent)
# ---------------------------------------------------------------------------

async def _handle_notification(task: Task, db: AsyncSession):
    """
    Simulates an external API call to send notifications:
    - Email confirmation
    - SMS alert
    - Webhook to downstream systems

    High latency variability (0.1s – 3.0s) reflects real external API behavior.
    In production: use httpx to call notification service with circuit breaker.
    """
    payload = task.job.payload
    account_id = payload.get("account_id", "UNKNOWN")

    # Simulate external API call latency
    await asyncio.sleep(random.uniform(0.1, 3.0))

    # Simulate API unavailability (3% rate)
    if random.random() < 0.03:
        raise TimeoutError(
            f"Notification API did not respond for account {account_id}"
        )

    logger.info(
        f"[Job:{task.job_id}] Notification sent — account={account_id}"
    )