"""CLI command: deid run — execute de-identification pipeline."""
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
from pydantic import validate_call
load_dotenv()

logger = logging.getLogger("deid.cli")


@validate_call(config=dict(arbitrary_types_allowed=True))
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
    Path(cfg.logging.log_dir).mkdir(parents=True, exist_ok=True)

    if phase:
        cfg.phases = [phase]

    from deid.tasks.celery_app import create_celery_app, get_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)
    get_celery_app().conf.deid_config_path = str(config_path)

    worker_procs = _start_workers(cfg, str(config_path))

    try:
        from deid.orchestrator.async_runner import run

        asyncio.run(run(cfg, str(config_path)))
        typer.echo("De-identification completed successfully.")
    except KeyboardInterrupt:
        typer.echo("\nInterrupted — shutting down...")
    finally:
        _stop_workers(worker_procs)


def _start_workers(cfg, config_path: str = "") -> list[subprocess.Popen]:
    """Spawn Celery worker subprocesses: fetch, process, and one write worker per table."""
    import os
    global_mtpc = cfg.workers.max_tasks_per_child
    shared_configs = [
        ("deid-fetch", cfg.workers.fetchers, "fetch",
         cfg.workers.max_tasks_per_child_fetch or global_mtpc),
        ("deid-process", cfg.workers.processors, "process",
         cfg.workers.max_tasks_per_child_process or global_mtpc),
    ]

    processes = []
    for queue, concurrency, name, mtpc in shared_configs:
        cmd = _worker_cmd(queue, concurrency, name, mtpc)
        env = {**os.environ, "DEID_WORKER_QUEUE": queue, "DEID_CONFIG_PATH": config_path}
        proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=env)
        logger.info("Started %s worker (pid=%d, concurrency=%d, mtpc=%d)", name, proc.pid, concurrency, mtpc)
        processes.append(proc)

    # One dedicated write worker per table (concurrency=1 serialises writes,
    # preventing MySQL lock-wait timeouts from concurrent INSERTs on the same table).
    for table in cfg.tables:
        queue = f"deid-write-{table.name}"
        name = f"write-{table.name}"
        cmd = _worker_cmd(queue, 1, name, global_mtpc)
        env = {**os.environ, "DEID_WORKER_QUEUE": queue, "DEID_CONFIG_PATH": config_path}
        proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=env)
        logger.info("Started write worker for table '%s' (pid=%d)", table.name, proc.pid)
        processes.append(proc)

    time.sleep(3)
    for proc in processes:
        if proc.poll() is not None:
            raise RuntimeError(
                f"Celery worker exited immediately with code {proc.returncode}"
            )

    return processes


def _worker_cmd(queue: str, concurrency: int, name: str, mtpc: int) -> list[str]:
    return [
        sys.executable, "-m", "celery",
        "-A", "deid.tasks.celery_app",
        "worker",
        f"--queues={queue}",
        f"--concurrency={concurrency}",
        f"--hostname={name}@%n",
        "--pool=prefork",
        f"--max-tasks-per-child={mtpc}",
        "--loglevel=info",
        "--without-heartbeat",
        "--without-mingle",
        "--without-gossip",
    ]


def _stop_workers(processes: list[subprocess.Popen]):
    """Terminate all worker subprocesses with SIGKILL fallback."""
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()

    for proc in processes:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("Worker pid=%d did not exit, sending SIGKILL", proc.pid)
            proc.kill()
            proc.wait(timeout=5)
