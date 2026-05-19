import asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from app.config import settings
from app.observability.logger import get_logger

logger = get_logger(__name__)


class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy ORM models.
    All models in models/job.py inherit from this.
    """
    pass


# --- Engine ---
# pool_pre_ping=True: tests connections before use, handles stale connections
# pool_size / max_overflow: tuned for 2500 concurrent jobs across worker pool
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=40,
    echo=False,  # Set True temporarily to see raw SQL during debugging
)

# --- Session Factory ---
# expire_on_commit=False: keeps ORM objects usable after commit (important in async)
AsyncSessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncSession:
    """
    FastAPI dependency that provides a DB session per request.
    Automatically commits on success, rolls back on exception.

    Usage in endpoints:
        async def my_endpoint(db: AsyncSession = Depends(get_session)):
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db():
    """
    Creates all tables on startup if they don't exist.
    In production this would be replaced by Alembic migrations.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables initialized")