"""CLI command: deid run — execute de-identification pipeline."""
from __future__ import annotations

import asyncio
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import typer

logger = logging.getLogger("deid.cli")


def run_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    phase: Optional[str] = typer.Option(None, "--phase", "-p", help="Override: run only this phase"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Run the de-identification pipeline (setup -> deidentify -> QC)."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    from deid.config.loader import load_config

    cfg = load_config(config_path)

    if phase:
        cfg.phases = [phase]

    from deid.tasks.celery_app import create_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)

    worker_proc = _start_worker(cfg)

    try:
        from deid.orchestrator.async_runner import run

        asyncio.run(run(cfg, str(config_path)))
        typer.echo("De-identification completed successfully.")
    except KeyboardInterrupt:
        typer.echo("\nInterrupted — shutting down...")
    finally:
        _stop_worker(worker_proc)


def _start_worker(cfg) -> subprocess.Popen:
    """Spawn a Celery worker as a child process."""
    cmd = [
        sys.executable, "-m", "celery",
        "-A", "deid.tasks.celery_app:get_celery_app()",
        "worker",
        "--pool=prefork",
        f"--concurrency={cfg.workers.concurrency}",
        "--loglevel=info",
        "--without-heartbeat",
        "--without-mingle",
        "--without-gossip",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(2)
    return proc


def _stop_worker(proc: subprocess.Popen | None):
    """Gracefully terminate the Celery worker subprocess."""
    if proc and proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
