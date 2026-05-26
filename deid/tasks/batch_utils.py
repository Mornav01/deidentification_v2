"""Shared batch-state helpers used by fetch, process, and write tasks."""
from __future__ import annotations

import time

from sqlalchemy.orm import Session

_LOCK_WAIT_ERRORS = (
    "lock wait timeout", "deadlock found", "1205", "1213",
    "database is locked",   # SQLite
)

_CONNECTION_ERRORS = (
    "unexpected eof",                          # pymssql 20017 — SQL Server dropped connection
    "20017",                                   # pymssql error code
    "20009",                                   # pymssql: unable to connect
    "20002",                                   # pymssql: connection failed
    "connection reset",                        # OS-level TCP reset
    "connection broken",                       # generic broken pipe
    "server closed the connection unexpectedly",
    "connection refused",
)


def _is_lock_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(s in msg for s in _LOCK_WAIT_ERRORS)


def _is_connection_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(s in msg for s in _CONNECTION_ERRORS)


def reset_or_fail_batch(
    engine,
    table_name: str,
    start_id: int,
    end_id: int,
    config_key: str,
    max_retries: int,
    reason: str = "",
) -> str:
    """Increment retry_count; set status to 'pending' or 'failed'.

    Returns the new status ('pending' or 'failed').
    If the batch row does not exist, returns 'pending' (no-op).
    Retries up to 5 times on lock errors with exponential back-off.
    """
    from deid.models.state import BatchState

    for attempt in range(5):
        try:
            with Session(engine) as session:
                batch = session.query(BatchState).filter_by(
                    table_name=table_name,
                    start_id=start_id,
                    end_id=end_id,
                    config_key=config_key,
                ).first()
                if batch is None:
                    return "pending"
                batch.retry_count = (batch.retry_count or 0) + 1
                batch.last_failed_reason = reason[:500] if reason else ""
                batch.status = "failed" if batch.retry_count >= max_retries else "pending"
                new_status = batch.status
                session.commit()
            return new_status
        except Exception as exc:
            if _is_lock_error(exc) and attempt < 4:
                time.sleep(0.5 * (2 ** attempt))
            else:
                raise
    return "pending"
