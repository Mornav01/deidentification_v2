"""CLI command: deid qc — run quality-control scanning as a standalone step."""
from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("deid.cli.qc")


def qc_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Run QC for a single table"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Run QC scanning on de-identified tables (after 'deid run')."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    from deid.config.loader import load_config

    cfg = load_config(config_path)

    from deid.tasks.celery_app import create_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)

    worker_proc = _start_qc_worker(cfg)

    try:
        asyncio.run(_run_qc(cfg, table_filter=table))
        typer.echo("QC completed successfully.")
    except KeyboardInterrupt:
        typer.echo("\nInterrupted — shutting down...")
    finally:
        _stop_worker(worker_proc)


def _start_qc_worker(cfg) -> subprocess.Popen:
    """Spawn a single Celery worker for QC tasks on the deid-process queue."""
    import os
    cmd = [
        sys.executable, "-m", "celery",
        "-A", "deid.tasks.celery_app",
        "worker",
        "--queues=deid-process",
        f"--concurrency={cfg.workers.processors}",
        "--hostname=qc@%n",
        "--pool=prefork",
        f"--max-tasks-per-child={cfg.workers.max_tasks_per_child}",
        "--loglevel=info",
        "--without-heartbeat",
        "--without-mingle",
        "--without-gossip",
    ]
    env = {**os.environ, "DEID_WORKER_QUEUE": "deid-process"}
    proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=env)
    logger.info("Started QC worker (pid=%d, concurrency=%d)", proc.pid, cfg.workers.processors)
    time.sleep(3)
    if proc.poll() is not None:
        raise RuntimeError(f"QC worker exited immediately with code {proc.returncode}")
    return proc


def _stop_worker(proc: subprocess.Popen):
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logger.warning("QC worker pid=%d did not exit, sending SIGKILL", proc.pid)
        proc.kill()
        proc.wait(timeout=5)


async def _run_qc(config, table_filter: str | None = None):
    """Dispatch QC tasks and wait for results."""
    from sqlalchemy.orm import Session

    from deid.config.task_models import QCTaskConfig
    from deid.models.base import create_state_engine
    from deid.models.state import TableState
    from deid.orchestrator.async_runner import _get_table_details
    from deid.tasks.qc import run_qc

    state_engine = create_state_engine(config.state_db_path)

    configured_tables = {t.name for t in config.tables}
    with Session(state_engine) as session:
        query = session.query(TableState).filter_by(status="completed", config_key=config.config_key)
        if table_filter:
            query = query.filter_by(table_name=table_filter)
        completed = query.all()
        table_names = [t.table_name for t in completed if t.table_name in configured_tables]

    if not table_names:
        msg = f"No completed tables found" + (f" matching '{table_filter}'" if table_filter else "")
        msg += f" (configured: {configured_tables})"
        logger.warning(msg)
        return

    results = []
    for tname in table_names:
        qc_config = QCTaskConfig(
            table_name=tname,
            source_conn_str=config.source_db.connection_string(),
            dest_conn_str=config.destination_db.connection_string(),
            offset_days=config.deidentification.date_offset_days,
            sample_size=config.qc.sample_size,
            table_config=_get_table_details(config, tname),
            qc_results_db_path=config.qc_results_db_path,
        )
        r = run_qc.apply_async(args=[qc_config.model_dump()], queue="deid-process")
        logger.info("Dispatched QC task for table '%s'", tname)
        results.append((tname, r))

    qc_timeout = config.qc.task_timeout
    for tname, r in results:
        r.get(timeout=qc_timeout)
        logger.info("QC completed for table '%s'", tname)

    state_engine.dispose()
