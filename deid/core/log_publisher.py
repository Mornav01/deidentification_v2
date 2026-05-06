"""Publish structured log records to Redis for the LogCollector."""
from __future__ import annotations

import os
import resource
import sys
from datetime import datetime, timezone

import redis as redis_lib

from deid.config.task_models import LogLevel, LogRecord


def publish_log(redis_url: str, record: LogRecord):
    """Publish a LogRecord to the deid:logs Redis channel."""
    r = redis_lib.from_url(redis_url)
    r.publish("deid:logs", record.model_dump_json())


def maybe_log(run_config: dict, record: LogRecord):
    """Publish log record only if the current verbosity level allows it."""
    verbosity = run_config.get("log_verbosity", "standard")
    redis_url = run_config.get("redis_url", "")
    if not redis_url:
        return

    # Errors are never filtered.
    if record.level == LogLevel.ERROR:
        publish_log(redis_url, record)
        return

    if verbosity == "minimal" and record.batch is not None:
        return  # skip batch/row level in minimal mode
    if verbosity == "standard" and record.level == LogLevel.DEBUG:
        return  # skip row-success in standard mode

    publish_log(redis_url, record)


def make_log_record(
    level: LogLevel,
    table: str,
    phase: str,
    message: str,
    **kwargs,
) -> LogRecord:
    """Convenience factory for LogRecord with auto-timestamp."""
    return LogRecord(
        timestamp=datetime.now(timezone.utc).isoformat(),
        level=level,
        table=table,
        phase=phase,
        message=message,
        worker_pid=os.getpid(),
        **kwargs,
    )


def get_peak_memory_mb() -> int:
    """Return peak RSS of current process in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    ru_maxrss = usage.ru_maxrss
    # macOS reports bytes, Linux reports KB
    if sys.platform == "darwin":
        return ru_maxrss // (1024 * 1024)
    return ru_maxrss // 1024
