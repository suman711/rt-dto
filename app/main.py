import asyncio
import contextlib

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.dispatcher import router as jobs_router
from app.config import settings
from app.core.dlq import run_dlq_monitor
from app.core.orchestrator import ensure_consumer_groups
from app.core.worker import run_worker
from app.db.session import init_db
from app.observability.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Lifespan — Startup & Shutdown
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages application startup and shutdown sequence.
    Using asynccontextmanager lifespan (FastAPI recommended pattern).

    Startup order matters:
    1. DB tables first — workers need tables to exist
    2. Redis consumer groups — workers need groups before consuming
    3. Workers — start consuming tasks
    4. DLQ monitor — watch for permanently failed tasks
    """
    logger.info(f"RT-DTO starting up (env={settings.APP_ENV})")

    # 1. Initialize database tables
    await init_db()

    # 2. Bootstrap Redis consumer groups
    await ensure_consumer_groups()

    # 3. Launch worker coroutines (WORKER_CONCURRENCY workers per process)
    worker_tasks = [
        asyncio.create_task(run_worker(), name=f"worker-{i}")
        for i in range(settings.WORKER_CONCURRENCY)
    ]

    # 4. Launch DLQ monitor
    dlq_task = asyncio.create_task(run_dlq_monitor(), name="dlq-monitor")

    logger.info(
        f"Started {settings.WORKER_CONCURRENCY} workers + 1 DLQ monitor"
    )

    yield  # Application is running

    # --- Graceful shutdown ---
    logger.info("RT-DTO shutting down — cancelling background tasks")

    for task in worker_tasks:
        task.cancel()
    dlq_task.cancel()

    # Wait for all tasks to finish cancellation
    await asyncio.gather(
        *worker_tasks, dlq_task, return_exceptions=True
    )

    logger.info("All background tasks stopped — shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="RT-DTO — Real-Time Distributed Task Orchestrator",
    description=(
        "A high-performance task orchestration engine for processing "
        "financial transactions through sequential validation, ledger update, "
        "and notification pipelines."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",       # Swagger UI
    redoc_url="/redoc",     # ReDoc UI
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(jobs_router)


# ---------------------------------------------------------------------------
# Health & Root Endpoints
# ---------------------------------------------------------------------------

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint — confirms service is running."""
    return {
        "service": "RT-DTO",
        "version": "1.0.0",
        "status": "running",
        "environment": settings.APP_ENV,
        "docs": "/docs",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint for Docker and load balancer probes.
    Verifies Redis connectivity in addition to app health.
    """
    from app.core.orchestrator import redis_client

    try:
        await redis_client.ping()
        redis_status = "ok"
    except Exception as e:
        redis_status = f"error: {str(e)}"

    return {
        "status": "ok" if redis_status == "ok" else "degraded",
        "redis": redis_status,
        "environment": settings.APP_ENV,
    }


# ---------------------------------------------------------------------------
# Entrypoint (for running directly without Docker)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.APP_ENV == "development",
        log_level=settings.LOG_LEVEL.lower(),
    )