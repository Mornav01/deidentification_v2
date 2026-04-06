"""Fetch stage — reads batches from source DB, writes Arrow IPC files."""
from __future__ import annotations

import json
import logging
import os

import polars as pl
import pyarrow.ipc as ipc
from celery import shared_task
from sqlalchemy.orm import Session

from deid.config.task_models import FetchTaskConfig, LogLevel
from deid.core.dbPkg.dbhandler import NDDBHandler, stream_table_keyset
from deid.core.log_publisher import get_peak_memory_mb, make_log_record, publish_log
from deid.models.base import get_cached_state_engine
from deid.models.state import BatchState
from deid.staging import batch_fetched_path

logger = logging.getLogger("deid.tasks.fetch")

# Module-level handler cache for connection reuse across tasks
_handler_cache: dict[str, NDDBHandler] = {}


def _publish(config: FetchTaskConfig, level: LogLevel, phase: str, message: str, **kwargs):
    if config.redis_url:
        publish_log(config.redis_url, make_log_record(level, config.table_name, phase, message, **kwargs))


def _get_cached_handler(conn_str: str, read_only: bool = False) -> NDDBHandler:
    """Return a cached NDDBHandler, creating one if needed."""
    key = f"{conn_str}::{read_only}"
    if key not in _handler_cache:
        _handler_cache[key] = NDDBHandler(conn_str, read_only=read_only)
    return _handler_cache[key]


@shared_task(bind=True, name="deid.tasks.fetch.fetch_batch")
def fetch_batch(self, raw_config: dict):
    """Fetch a batch of rows from source DB and write to Arrow IPC file."""
    import time
    config = FetchTaskConfig(**raw_config)
    batch_tag = f"{config.start_id}-{config.end_id}"
    root = _staging_root(config)
    t0 = time.monotonic()
    try:
        result = _fetch_batch_inner(config, raw_config, batch_tag, root)
        duration_ms = int((time.monotonic() - t0) * 1000)
        _publish(config, LogLevel.INFO, "fetch",
                 f"batch {batch_tag} fetched",
                 batch=config.start_id, rows_in_batch=result.get("rows", 0),
                 rows_succeeded=result.get("rows", 0),
                 start_id=config.start_id, end_id=config.end_id,
                 duration_ms=duration_ms, peak_memory_mb=get_peak_memory_mb())
        return result
    except Exception as exc:
        # Mark batch as failed so the watchdog doesn't treat it as in-flight
        try:
            _update_batch_status(config, "pending")
        except Exception:
            logger.warning("Could not reset batch %s to pending after failure", batch_tag)
        _publish(config, LogLevel.ERROR, "fetch",
                 f"batch {batch_tag} failed: {exc}",
                 start_id=config.start_id, end_id=config.end_id,
                 error=f"{type(exc).__name__}: {exc}")
        raise


def _fetch_batch_inner(config: FetchTaskConfig, raw_config: dict, batch_tag: str, root):
    # Idempotency guard: handle re-delivery from task_acks_late after worker kill.
    engine = get_cached_state_engine(config.state_db_path)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
            config_key=config.config_key,
        ).first()
        if batch and batch.status in ("processed", "done"):
            logger.info("Skipping already-%s batch %s (idempotency guard)", batch.status, batch_tag)
            return {"table": config.table_name, "start_id": config.start_id,
                    "end_id": config.end_id, "status": batch.status, "rows": 0}
        if batch and batch.status == "fetched":
            # Arrow file is already on disk. Re-dispatch process_batch in case the
            # original dispatch was lost when the worker died, then return.
            logger.info(
                "Re-dispatching process_batch for already-fetched batch %s (idempotency recovery)",
                batch_tag,
            )
            from deid.tasks.process import process_batch
            process_config = {
                "table_name": config.table_name,
                "start_id": config.start_id,
                "end_id": config.end_id,
                "staging_root": str(root),
                "state_db_path": config.state_db_path,
                "config_key": config.config_key,
                "redis_url": config.redis_url,
                "run_config": config.run_config,
                **{k: raw_config[k] for k in (
                    "mapping_db_config", "table_details", "source_conn_str",
                    "offset_days", "pii_config", "pii_db_conn_str",
                    "secondary_pii_configs", "dest_conn_str", "failed_rows_db_path",
                ) if k in raw_config},
            }
            process_batch.apply_async(args=[process_config], queue="deid-process")
            return {"table": config.table_name, "start_id": config.start_id,
                    "end_id": config.end_id, "status": "fetched", "rows": 0}

    # 1. Fetch rows from source using keyset pagination (O(1) per batch)
    source = _get_cached_handler(config.source_conn_str, read_only=True)
    col_info = source.get_columns(config.table_name)
    col_schema = {}
    for c in col_info:
        length = getattr(c.get("type"), "length", None)
        col_schema[c["name"]] = {
            "type": str(c.get("type", "")),
            "length": int(length) if length else None,
        }

    id_col = config.id_column
    batch_size = config.end_id - config.start_id + 1
    frames = list(stream_table_keyset(
        source, config.table_name,
        batch_size=batch_size,
        last_id=config.last_fetched_id,
        id_column=id_col,
    ))

    if not frames:
        _update_batch_status(config, "done")
        return {"table": config.table_name, "start_id": config.start_id,
                "end_id": config.end_id, "status": "done", "rows": 0}

    df = pl.concat(frames)

    # Extract actual ID range for idempotent write DELETE
    if id_col in df.columns:
        actual_start_id = int(df[id_col].min())
        actual_end_id = int(df[id_col].max())
    else:
        actual_start_id = config.start_id
        actual_end_id = config.end_id

    # 2. Write Arrow IPC with embedded column schema metadata
    target = batch_fetched_path(root, config.table_name, config.start_id, config.end_id, config.config_key)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".tmp")

    arrow_table = df.to_arrow()
    existing_meta = arrow_table.schema.metadata or {}
    existing_meta[b"deid_column_schema"] = json.dumps(col_schema).encode()
    existing_meta[b"deid_actual_start_id"] = str(actual_start_id).encode()
    existing_meta[b"deid_actual_end_id"] = str(actual_end_id).encode()
    arrow_table = arrow_table.replace_schema_metadata(existing_meta)

    with ipc.new_file(str(tmp_path), arrow_table.schema) as writer:
        writer.write_table(arrow_table)
    os.rename(tmp_path, target)

    # 3. Update BatchState: pending -> fetched (record actual_end_id for watchdog recovery)
    _update_batch_status(config, "fetched", actual_end_id=actual_end_id)

    # 4. Self-chain: dispatch next pending batch for this table
    _dispatch_next_fetch(config, raw_config, actual_end_id)

    # 5. Dispatch process_batch for this batch
    from deid.tasks.process import process_batch
    process_config = {
        "table_name": config.table_name,
        "start_id": config.start_id,
        "end_id": config.end_id,
        "staging_root": str(root),
        "state_db_path": config.state_db_path,
        "config_key": config.config_key,
        "redis_url": config.redis_url,
        "run_config": config.run_config,
        **{k: raw_config[k] for k in (
            "mapping_db_config", "table_details", "source_conn_str",
            "offset_days", "pii_config", "pii_db_conn_str",
            "secondary_pii_configs", "dest_conn_str", "failed_rows_db_path",
        ) if k in raw_config},
    }
    process_batch.apply_async(args=[process_config], queue="deid-process")

    logger.info("Fetched %s batch %s (%d rows)", config.table_name, batch_tag, df.height)
    return {"table": config.table_name, "start_id": config.start_id,
            "end_id": config.end_id, "status": "fetched", "rows": df.height}


def _claim_next_pending_batch(session: Session, table_name: str, config_key: str = "default") -> "BatchState | None":
    """Atomically claim the next pending batch by setting status='dispatched'.

    Uses a guarded UPDATE (WHERE status='pending') so two concurrent workers
    racing on the same row get rowcount=1 and rowcount=0 respectively — only
    the winner proceeds to dispatch.
    """
    from sqlalchemy import text
    candidate = (
        session.query(BatchState)
        .filter_by(table_name=table_name, status="pending", config_key=config_key)
        .order_by(BatchState.start_id)
        .first()
    )
    if candidate is None:
        return None
    result = session.execute(
        text("UPDATE batch_states SET status = 'dispatched' WHERE id = :id AND status = 'pending'"),
        {"id": candidate.id},
    )
    session.commit()
    if result.rowcount == 0:
        return None  # another worker claimed it first
    candidate.status = "dispatched"
    return candidate


def _dispatch_next_fetch(config: FetchTaskConfig, raw_config: dict, last_fetched_id: int):
    """Atomically claim and dispatch the next pending batch for this table."""
    engine = get_cached_state_engine(config.state_db_path)
    with Session(engine) as session:
        next_batch = _claim_next_pending_batch(session, config.table_name, config.config_key)
        if next_batch:
            next_cfg = {**raw_config}
            next_cfg["start_id"] = next_batch.start_id
            next_cfg["end_id"] = next_batch.end_id
            next_cfg["last_fetched_id"] = last_fetched_id
            fetch_batch.apply_async(args=[next_cfg], queue="deid-fetch")


def _staging_root(config: FetchTaskConfig):
    from pathlib import Path
    return Path(config.staging_root)


def _update_batch_status(config: FetchTaskConfig, status: str, actual_end_id: int | None = None):
    engine = get_cached_state_engine(config.state_db_path)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
            config_key=config.config_key,
        ).first()
        if batch:
            batch.status = status
            if actual_end_id is not None:
                batch.actual_end_id = actual_end_id
            session.commit()
