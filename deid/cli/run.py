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
    config: str = typer.Option(..., "--config", "-c", help="Path to base config.yaml"),
    overlay: Optional[str] = typer.Option(None, "--overlay", "-o", help="Task-specific config overlay (overrides base keys, appends new ones)"),
    phase: Optional[str] = typer.Option(None, "--phase", "-p", help="Override: run only this phase"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
    rerun: bool = typer.Option(False, "--rerun", help="Clean slate: drop dest tables, remove state/staging, start fresh"),
    tables_csv: Optional[str] = typer.Option(None, "--tables-csv", help="CSV file listing table names to run (one per line)"),
):
    """Run the de-identification pipeline (setup -> deidentify)."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    overlay_path = None
    if overlay:
        overlay_path = Path(overlay)
        if not overlay_path.exists():
            typer.echo(f"Error: Overlay config not found: {overlay}", err=True)
            raise typer.Exit(code=1)

    from deid.config.loader import load_config

    cfg = load_config(config_path, overlay_path=overlay_path)
    Path(cfg.logging.log_dir).mkdir(parents=True, exist_ok=True)

    if tables_csv:
        cfg.tables_to_run_csv = tables_csv
        # Re-run the filter validator manually since the model is already constructed.
        cfg.filter_tables_to_run()

    if phase:
        cfg.phases = [phase]

    if rerun:
        _rerun_cleanup(cfg)

    if cfg.unmatched_tables:
        _record_unmatched_tables(cfg)

    if not cfg.tables:
        typer.echo(
            f"No tables to process — all tables_to_run were unmatched "
            f"({cfg.unmatched_tables}). Recorded as failed in state.db.",
            err=True,
        )
        raise typer.Exit(code=1)

    from deid.tasks.celery_app import create_celery_app, get_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)
    get_celery_app().conf.deid_config_path = str(config_path)

    from deid.orchestrator.async_runner import run

    all_tables = cfg.tables[:]
    batch_size = cfg.workers.table_batch_size
    batches = (
        [all_tables[i:i + batch_size] for i in range(0, len(all_tables), batch_size)]
        if batch_size > 0 else [all_tables]
    )
    n = len(batches)
    if n > 1:
        logger.info(
            "Processing %d tables in %d batch(es) of up to %d.",
            len(all_tables), n, batch_size,
        )

    interrupted = False
    for idx, batch in enumerate(batches):
        if interrupted:
            break
        cfg.tables = batch
        if n > 1:
            logger.info("Table batch %d/%d: %s", idx + 1, n, [t.name for t in batch])
        # Queue hygiene: drop stale messages for this batch's queues.
        _purge_run_queues(cfg)
        worker_procs = _start_workers(cfg, str(config_path))
        try:
            asyncio.run(run(cfg, str(config_path)))
            if n > 1:
                typer.echo(f"Batch {idx + 1}/{n} complete.")
        except KeyboardInterrupt:
            typer.echo("\nInterrupted — shutting down...")
            interrupted = True
        finally:
            _stop_workers(worker_procs)

    if not interrupted:
        typer.echo("De-identification completed successfully.")


def _rerun_cleanup(cfg):
    """Remove state DB, staging files, and destination tables for configured tables."""
    import shutil
    from deid.staging import get_staging_root

    table_names = [t.name for t in cfg.tables] if cfg.tables else []
    logger.info("Rerun cleanup for %d tables: %s", len(table_names), table_names)

    # 1. Drop destination tables
    if table_names:
        from sqlalchemy import text
        from deid.core.dbPkg.dbhandler import NDDBHandler
        dest = NDDBHandler(cfg.destination_db.connection_string())
        qi = dest._qi
        for tname in table_names:
            try:
                with dest.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {qi(tname)}"))
                logger.info("Dropped destination table: %s", tname)
            except Exception as e:
                logger.warning("Could not drop table %s: %s", tname, e)
        dest.close()

    # 2. Remove state entries only for the tables being rerun
    #    Also delete any prior state for unmatched tables so _record_unmatched_tables
    #    writes a clean fresh row rather than updating a stale one.
    all_names_to_clear = table_names + list(cfg.unmatched_tables or [])
    _state_is_sqlite = cfg.resolved_state_db_url.startswith("sqlite:///")
    _state_db_ready = (not _state_is_sqlite) or Path(cfg.state_db_path).exists()
    if _state_db_ready and all_names_to_clear:
        from sqlalchemy import text as sa_text
        from deid.models.base import create_state_engine, create_all_state_tables
        st_engine = create_state_engine(cfg.resolved_state_db_url)
        create_all_state_tables(st_engine)
        try:
            from sqlalchemy import bindparam
            delete_batch = sa_text(
                "DELETE FROM batch_states WHERE table_name IN :names AND config_key = :ck"
            ).bindparams(bindparam("names", expanding=True))
            delete_table = sa_text(
                "DELETE FROM table_states WHERE table_name IN :names AND config_key = :ck"
            ).bindparams(bindparam("names", expanding=True))
            params = {"names": all_names_to_clear, "ck": cfg.config_key}
            with st_engine.begin() as conn:
                conn.execute(delete_batch, params)
                conn.execute(delete_table, params)
            logger.info("Cleared state for %d table(s) from state.db", len(all_names_to_clear))
        except Exception as e:
            logger.warning("Could not clear state entries: %s", e)
        finally:
            st_engine.dispose()

    # 3. Delete failed rows only for the tables being rerun
    _fr_is_sqlite = cfg.resolved_failed_rows_db_url.startswith("sqlite:///")
    _fr_db_ready = (not _fr_is_sqlite) or Path(cfg.failed_rows_db_path).exists()
    if _fr_db_ready and table_names:
        from sqlalchemy import text as sa_text, inspect as sa_inspect
        from deid.models.base import create_failed_rows_engine
        from deid.models.failed_rows import get_schema_table_name
        schema_name = cfg.source_db.database
        fr_table = get_schema_table_name(schema_name)
        fr_engine = create_failed_rows_engine(cfg.resolved_failed_rows_db_url)
        try:
            existing = set(sa_inspect(fr_engine).get_table_names())
            if fr_table in existing:
                from sqlalchemy import bindparam
                delete_fr = sa_text(
                    f"DELETE FROM {fr_table} WHERE table_name IN :names AND config_key = :ck"
                ).bindparams(bindparam("names", expanding=True))
                with fr_engine.begin() as conn:
                    conn.execute(delete_fr, {"names": table_names, "ck": cfg.config_key})
                logger.info(
                    "Deleted failed rows for %d table(s) from '%s'",
                    len(table_names), fr_table,
                )
        except Exception as e:
            logger.warning("Could not clean failed_rows: %s", e)
        finally:
            fr_engine.dispose()

    # 4. Remove staging files only for the tables being rerun (scoped by config_key)
    staging_root = get_staging_root(cfg.state_db_path)
    if staging_root.exists():
        for tname in table_names:
            table_dir = staging_root / cfg.config_key / tname
            if table_dir.exists():
                shutil.rmtree(table_dir)
                logger.info("Removed staging for table: %s", tname)

    # Queue purge is handled by _purge_run_queues at run startup — it runs
    # unconditionally and covers fetch, process, and write queues for this config_key.


def _record_unmatched_tables(cfg):
    """Write a failed TableState row for each table in tables_to_run that has no config rules."""
    from sqlalchemy.orm import Session
    from deid.models.base import create_state_engine, create_all_state_tables
    from deid.models.state import DbConfig as StateDbConfig, TableState

    engine = create_state_engine(cfg.resolved_state_db_url)
    create_all_state_tables(engine)
    try:
        with Session(engine) as session:
            db_cfg = session.query(StateDbConfig).filter_by(name=cfg.config_key).first()
            if not db_cfg:
                src = cfg.source_db
                dst = cfg.destination_db
                db_cfg = StateDbConfig(
                    name=cfg.config_key,
                    source_conn_str=f"{src.type}://{src.host}:{src.port}/{src.database}",
                    dest_conn_str=f"{dst.type}://{dst.host}:{dst.port}/{dst.database}",
                )
                session.add(db_cfg)
                session.commit()

            for tname in cfg.unmatched_tables:
                existing = session.query(TableState).filter_by(
                    table_name=tname, config_key=cfg.config_key
                ).first()
                if existing:
                    existing.status = "failed"
                    existing.failure_remarks = (
                        "Table listed in tables_to_run but has no de-identification rules in config"
                    )
                else:
                    session.add(TableState(
                        db_config_id=db_cfg.id,
                        table_name=tname,
                        config_key=cfg.config_key,
                        status="failed",
                        failure_remarks=(
                            "Table listed in tables_to_run but has no de-identification rules in config"
                        ),
                        rules_config={},
                    ))
            session.commit()
            logger.warning(
                "Recorded %d table(s) as failed (no config rules): %s",
                len(cfg.unmatched_tables), cfg.unmatched_tables,
            )
    finally:
        engine.dispose()


def _purge_run_queues(cfg) -> None:
    """Drop any stale messages in the queues this run will consume.

    Aborted prior runs can leave tasks in Redis that a naive restart would
    re-execute against changed state. Startup purge is unconditional so
    `deid run` (with or without --rerun) always begins with a clean queue
    for its config_key. State-DB idempotency + the orchestrator watchdog
    handle re-dispatch of anything that genuinely still needs to run.
    """
    queues = [
        f"deid-fetch-{cfg.config_key}",
        f"deid-process-{cfg.config_key}",
        *[f"deid-write-{cfg.config_key}-{t.name}" for t in cfg.tables],
    ]
    try:
        import redis
        r = redis.Redis.from_url(cfg.redis_url)
        for q in queues:
            r.delete(q)
        r.close()
        logger.info("Purged %d Redis queue(s) for config_key=%s", len(queues), cfg.config_key)
    except Exception as e:
        logger.warning("Could not purge Redis queues: %s", e)


def _start_workers(cfg, config_path: str = "") -> list[subprocess.Popen]:
    """Spawn Celery worker subprocesses: fetch, process, and one write worker per table."""
    import os
    global_mtpc = cfg.workers.max_tasks_per_child
    shared_configs = [
        (f"deid-fetch-{cfg.config_key}", cfg.workers.fetchers, "fetch",
         cfg.workers.max_tasks_per_child_fetch or global_mtpc),
        (f"deid-process-{cfg.config_key}", cfg.workers.processors, "process",
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
        queue = f"deid-write-{cfg.config_key}-{table.name}"
        name = f"write-{cfg.config_key}-{table.name}"
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
