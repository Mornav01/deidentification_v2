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

    from deid.cli.run import _start_worker, _stop_worker
    worker_proc = _start_worker(cfg)

    try:
        asyncio.run(_retry_run(cfg, batch_failures))
        typer.echo("Retry completed.")
    except KeyboardInterrupt:
        typer.echo("\nInterrupted — shutting down...")
    finally:
        _stop_worker(worker_proc)


async def _retry_run(config, batch_failures: list[BatchFailure]):
    """Dispatch retry tasks and monitor progress."""
    from celery import group as celery_group
    from deid.tasks.deidentify import deidentify_table, deidentify_table_range
    from deid.orchestrator.task_graph import _build_table_config

    tasks = []
    for failure in batch_failures:
        # Find matching table config.
        table_cfg = next(
            (t for t in config.tables if t.name == failure.table), None
        )
        if table_cfg is None:
            logger.warning(f"Table '{failure.table}' not in config — skipping.")
            continue

        task_config = _build_table_config(config, failure.table, table_cfg.rules)

        if failure.task_type == "range" and failure.start_id is not None:
            tasks.append(
                deidentify_table_range.s(task_config, failure.start_id, failure.end_id)
            )
        else:
            tasks.append(deidentify_table.s(task_config))

    if not tasks:
        return

    result = celery_group(tasks).apply_async()

    from deid.orchestrator.progress import listen_progress
    from deid.config.task_models import ProgressEvent

    async for event in listen_progress(config.redis_url):
        progress = ProgressEvent(**event)
        logger.info("Retry progress: %s — %s", progress.table, progress.status)
        if result.ready():
            break
