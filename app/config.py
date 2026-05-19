from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """
    Central configuration loaded from environment variables or .env file.
    All other modules import `get_settings()` rather than reading env vars directly.
    """

    # --- Database ---
    DATABASE_URL: str = Field(
        default="postgresql+psycopg://user:pass@localhost:5432/rtdto",
        description="Async-compatible PostgreSQL connection string (psycopg3 driver)",
    )

    # --- Redis ---
    REDIS_URL: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL for stream broker",
    )

    # --- Worker ---
    WORKER_CONCURRENCY: int = Field(
        default=10,
        description="Number of concurrent task coroutines per worker process",
    )
    TASK_TIMEOUT_SECONDS: int = Field(
        default=30,
        description="Hard timeout per task before it is killed and marked FAILED",
    )
    MAX_RETRIES: int = Field(
        default=3,
        description="Max attempts before a task is moved to the Dead Letter Queue",
    )

    # --- Redis Stream ---
    STREAM_NAME: str = Field(default="tasks_stream")
    DLQ_STREAM: str = Field(default="tasks_dlq")
    CONSUMER_GROUP: str = Field(default="workers")

    # --- App ---
    APP_ENV: str = Field(default="development")
    LOG_LEVEL: str = Field(default="INFO")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Single shared instance — import this everywhere
settings = Settings()