import asyncio
from app.core.orchestrator import ensure_consumer_groups
from app.core.worker import run_worker
from app.core.dlq import run_dlq_monitor
from app.observability.logger import get_logger

logger = get_logger(__name__)


async def main():
    logger.info("Standalone worker process starting")
    await ensure_consumer_groups()
    await asyncio.gather(
        run_worker(),
        run_dlq_monitor(),
    )


if __name__ == "__main__":
    asyncio.run(main())