"""CLI command: deid retry — re-run failed batches from a failures file."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

import typer
from pydantic import validate_call

from deid.config.task_models import BatchFailure

logger = logging.getLogger("deid.cli")


@validate_call(config=dict(arbitrary_types_allowed=True))
def _load_failures(failures_path: str) -> list[BatchFailure]:
    """Read a failures JSONL file and return BatchFailure objects."""
    failures = []
    with open(failures_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                failures.append(BatchFailure.model_validate_json(line))
    return failures


@validate_call(config=dict(arbitrary_types_allowed=True))
def retry_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    failures: str = typer.Option(..., "--failures", "-f", help="Path to failures JSONL file"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Retry failed batches from a previous run."""
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    failures_path = Path(failures)
    if not failures_path.exists():
        typer.echo(f"Error: Failures file not found: {failures}", err=True)
        raise typer.Exit(code=1)

    batch_failures = _load_failures(str(failures_path))
    if not batch_failures:
        typer.echo("No failures found in file. Nothing to retry.")
        raise typer.Exit(code=0)

    typer.echo(f"Retrying {len(batch_failures)} failed batch(es)...")

    from deid.config.loader import load_config
    cfg = load_config(config_path)

    from deid.tasks.celery_app import create_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)

    from deid.cli.run import _start_workers, _stop_workers
    worker_procs = _start_workers(cfg)

    try:
        asyncio.run(_retry_run(cfg, batch_failures))
        typer.echo("Retry completed.")
    except KeyboardInterrupt:
        typer.echo("\nInterrupted — shutting down...")
    finally:
        _stop_workers(worker_procs)


async def _retry_run(config, batch_failures: list[BatchFailure]):
    """Dispatch retry tasks for failed batches using the 3-stage pipeline."""
    from deid.models.base import create_state_engine
    from deid.models.state import BatchState
    from deid.staging import get_staging_root
    from deid.tasks.fetch import fetch_batch
    from sqlalchemy.orm import Session

    state_engine = create_state_engine(config.state_db_path)
    staging_root = get_staging_root(config.state_db_path)
    mappings_conn_str = config.mappings_connection_string

    # Reset failed batches to pending and dispatch fetch tasks.
    dispatched = 0
    for failure in batch_failures:
        table_cfg = next(
            (t for t in config.tables if t.name == failure.table), None
        )
        if table_cfg is None:
            logger.warning(f"Table '{failure.table}' not in config — skipping.")
            continue

        if failure.start_id is None or failure.end_id is None:
            logger.warning(f"Batch for '{failure.table}' missing ID range — skipping.")
            continue

        # Reset BatchState to pending.
        with Session(state_engine) as session:
            batch = session.query(BatchState).filter_by(
                table_name=failure.table,
                start_id=failure.start_id,
                end_id=failure.end_id,
                config_key=config.config_key,
            ).first()
            if batch:
                batch.status = "pending"
                session.commit()

        from deid.orchestrator.async_runner import _build_fetch_config, _get_table_details

        # Build a minimal batch-like object for _build_fetch_config.
        class _Batch:
            def __init__(self, table_name, start_id, end_id):
                self.table_name = table_name
                self.start_id = start_id
                self.end_id = end_id

        batch_obj = _Batch(failure.table, failure.start_id, failure.end_id)
        cfg = _build_fetch_config(config, batch_obj, staging_root, mappings_conn_str)
        fetch_batch.apply_async(args=[cfg], queue=f"deid-fetch-{config.config_key}")
        dispatched += 1

    if dispatched == 0:
        return

    logger.info("Dispatched %d retry batches.", dispatched)

    # Poll for completion.
    import time
    while True:
        await asyncio.sleep(2)
        with Session(state_engine) as session:
            remaining = 0
            for failure in batch_failures:
                batch = session.query(BatchState).filter_by(
                    table_name=failure.table,
                    start_id=failure.start_id,
                    end_id=failure.end_id,
                    config_key=config.config_key,
                ).first()
                if batch and batch.status != "done":
                    remaining += 1
            if remaining == 0:
                break
