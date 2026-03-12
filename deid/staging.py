"""Staging directory helpers for the 3-stage pipeline.

Arrow IPC files in .deid_staging/ are the inter-stage data transfer.
This module provides path computation, atomic writes, cleanup, and
startup reconciliation.
"""
from __future__ import annotations

import os
from pathlib import Path

import polars as pl
from sqlalchemy.orm import Session


def get_staging_root(state_db_path: str) -> Path:
    """Derive staging root from state_db_path."""
    return Path(state_db_path).resolve().parent / ".deid_staging"


def batch_fetched_path(root: Path, table: str, start_id: int, end_id: int) -> Path:
    return root / table / f"batch_{start_id}_{end_id}.arrow"


def batch_processed_path(root: Path, table: str, start_id: int, end_id: int) -> Path:
    return root / table / f"batch_{start_id}_{end_id}.proc.arrow"


def atomic_write_arrow(df: pl.DataFrame, target_path: Path) -> None:
    """Write DataFrame to Arrow IPC via tmp file + atomic rename."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    df.write_ipc(tmp_path)
    os.rename(tmp_path, target_path)


def cleanup_tmp_files(root: Path) -> None:
    """Delete all .tmp files recursively under root."""
    if not root.exists():
        return
    for tmp in root.rglob("*.tmp"):
        tmp.unlink(missing_ok=True)


def reconcile(state_engine, root: Path) -> None:
    """Fix BatchState vs. files on disk after a crash.

    Rules:
    - fetched + .arrow + .proc.arrow -> delete .arrow, advance to processed
    - fetched + no .arrow + .proc.arrow -> advance to processed
    - fetched + no .arrow + no .proc.arrow -> reset to pending
    - processed + no .proc.arrow -> reset to pending
    - done / pending -> no action
    """
    from deid.models.state import BatchState

    with Session(state_engine) as session:
        # "dispatched" means a task was queued but not yet running — safe to reset
        # on startup since the Celery queue is gone after a restart.
        session.query(BatchState).filter_by(status="dispatched").update({"status": "pending"})
        session.commit()

        batches = session.query(BatchState).filter(
            BatchState.status.in_(["fetched", "processed"])
        ).all()

        for batch in batches:
            fetched = batch_fetched_path(root, batch.table_name, batch.start_id, batch.end_id)
            processed = batch_processed_path(root, batch.table_name, batch.start_id, batch.end_id)

            if batch.status == "fetched":
                if fetched.exists() and processed.exists():
                    fetched.unlink()
                    batch.status = "processed"
                elif not fetched.exists() and processed.exists():
                    batch.status = "processed"
                elif not fetched.exists() and not processed.exists():
                    batch.status = "pending"
                # fetched.exists() and not processed.exists() -> normal, no change

            elif batch.status == "processed":
                if not processed.exists():
                    batch.status = "pending"
                # processed.exists() -> normal, no change

        session.commit()
