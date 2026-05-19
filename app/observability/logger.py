import logging
import json
import sys
from datetime import datetime, timezone
from app.config import settings


class JSONFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.
    Makes logs easily parseable by tools like Datadog, CloudWatch, or Grafana Loki.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Attach exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Attach any extra fields passed via logger.info("msg", extra={...})
        for key, value in record.__dict__.items():
            if key not in (
                "message", "asctime", "name", "msg", "args", "levelname",
                "levelno", "pathname", "filename", "module", "funcName",
                "lineno", "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "exc_info",
                "exc_text", "stack_info",
            ):
                log_entry[key] = value

        return json.dumps(log_entry)


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger with JSON formatting.
    Usage: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(settings.LOG_LEVEL.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate output)
    logger.propagate = False

    return logger