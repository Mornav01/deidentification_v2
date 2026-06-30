"""Publish structured log records to Redis for the LogCollector."""
from __future__ import annotations

import logging
import os
import resource
import sys
from datetime import datetime, timezone
from functools import lru_cache

import redis as redis_lib

from deid.config.task_models import LogLevel, LogRecord

_log = logging.getLogger("deid.log_publisher")


@lru_cache(maxsize=8)
def _get_pool(redis_url: str) -> redis_lib.ConnectionPool:
    """Return a shared connection pool for *redis_url* (one pool per URL)."""
    return redis_lib.ConnectionPool.from_url(
        redis_url,
        socket_timeout=10,
        socket_connect_timeout=10,
        socket_keepalive=True,
        health_check_interval=30,
    )


def publish_log(redis_url: str, record: LogRecord):
    """Publish a LogRecord to the deid:logs Redis channel.

    Errors are logged locally and swallowed so that a transient Redis blip
    never propagates up and kills a running task.
    """
    try:
        r = redis_lib.Redis(connection_pool=_get_pool(redis_url))
        r.publish("deid:logs", record.model_dump_json())
    except Exception as exc:
        _log.warning("Redis publish failed (log dropped): %s", exc)


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
