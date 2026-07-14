"""CLI command: deid retry — re-run failed batches from a failures file or state.db."""
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
    failures: Optional[str] = typer.Option(None, "--failures", "-f", help="Path to failures JSONL file (optional)"),
    tables: Optional[str] = typer.Option(None, "--tables", "-t", help="Comma-separated table names to retry"),
    tables_csv: Optional[str] = typer.Option(None, "--tables-csv", help="File listing table names to retry (one per line)"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Retry failed batches from a previous run.

    Scoped to the config_key in config.yaml. Tables that are already fully
    done are skipped automatically.

    If neither --failures nor --tables/--tables-csv is provided, all
    permanently-failed batches in state.db for this config_key are retried.
    """
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    # Build optional table name filter
    tables_filter: set[str] | None = None
    if tables:
        tables_filter = {t.strip() for t in tables.split(",") if t.strip()}
    elif tables_csv:
        csv_path = Path(tables_csv)
        if not csv_path.exists():
            typer.echo(f"Error: --tables-csv file not found: {tables_csv}", err=True)
            raise typer.Exit(code=1)
        with open(csv_path) as _f:
            tables_filter = {
                line.strip() for line in _f
                if line.strip() and not line.strip().startswith("#")
            }

    batch_failures: list[BatchFailure] = []
    if failures is not None:
        failures_path = Path(failures)
        if not failures_path.exists():
            typer.echo(f"Error: Failures file not found: {failures}", err=True)
            raise typer.Exit(code=1)
        batch_failures = _load_failures(str(failures_path))

    typer.echo("Retrying failed batches (tables already done will be skipped)...")

    from deid.config.loader import load_config
    cfg = load_config(config_path)

    # Load pii_config / secondary_pii_configs from file — same as run() does.
    # Without this, cfg.pii_config is None and notes de-identification crashes.
    if cfg.pii_db and not cfg.pii_config and cfg.pii_config_path:
        import yaml as _yaml
        from deid.config.loader import _interpolate_env_vars
        from deid.config.schema import validate_replace_value
        with open(cfg.pii_config_path) as _f:
            cfg.pii_config = _interpolate_env_vars(_yaml.safe_load(_f))
        validate_replace_value(cfg.pii_config)
    if not cfg.secondary_pii_configs and cfg.secondary_pii_config_path:
        import yaml as _yaml
        from deid.config.loader import _interpolate_env_vars
        from deid.config.schema import validate_replace_value
        with open(cfg.secondary_pii_config_path) as _f:
            cfg.secondary_pii_configs = _interpolate_env_vars(_yaml.safe_load(_f))
        for _i, _cfg in enumerate(cfg.secondary_pii_configs or []):
            validate_replace_value(_cfg, source=f"secondary_pii_configs[{_i}]")

    if cfg.table_overrides_path and not cfg.table_overrides:
        import yaml as _yaml
        with open(cfg.table_overrides_path) as _f:
            cfg.table_overrides = _yaml.safe_load(_f) or {}

    # Scope cfg.tables to only the retry tables so _start_workers spawns
    # write workers only for those tables, not all tables in the rules_csv.
    # Without this, a 2,000-table config would spawn 2,000 write workers even
    # when retrying 3 tables — instantly exhausting system memory.
    if tables_filter:
        cfg.tables = [t for t in cfg.tables if t.name in tables_filter]
        if not cfg.tables:
            typer.echo(
                "Error: None of the specified tables exist in the config rules.",
                err=True,
            )
            raise typer.Exit(code=1)

    from deid.tasks.celery_app import create_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)

    from deid.cli.run import _start_workers, _stop_workers

    all_tables = cfg.tables[:]
    batch_size = cfg.workers.table_batch_size
    batches = (
        [all_tables[i:i + batch_size] for i in range(0, len(all_tables), batch_size)]
        if batch_size > 0 else [all_tables]
    )
    n = len(batches)
    if n > 1:
        logger.info(
            "Retrying %d tables in %d batch(es) of up to %d.",
            len(all_tables), n, batch_size,
        )

    interrupted = False
    for idx, batch in enumerate(batches):
        if interrupted:
            break
        cfg.tables = batch
        batch_names = {t.name for t in batch}
        batch_failures_slice = [f for f in batch_failures if f.table in batch_names]
        if n > 1:
            logger.info("Table batch %d/%d: %s", idx + 1, n, [t.name for t in batch])
        worker_procs = _start_workers(cfg, str(config_path))
        try:
            asyncio.run(_retry_run(cfg, batch_failures_slice, tables_filter=batch_names))
            if n > 1:
                typer.echo(f"Batch {idx + 1}/{n} retry complete.")
        except KeyboardInterrupt:
            typer.echo("\nInterrupted — shutting down...")
            interrupted = True
        finally:
            _stop_workers(worker_procs)

    if not interrupted:
        typer.echo("Retry completed.")


async def _retry_run(
    config,
    batch_failures: list[BatchFailure],
    tables_filter: set[str] | None = None,
):
    """Dispatch retry tasks for failed batches using the 3-stage pipeline.

    tables_filter: when provided, only batches belonging to these table names
    are retried.  Batches whose status is already 'done' are skipped silently.
    """
    from datetime import datetime, timezone

    from deid.models.base import create_state_engine
    from deid.models.state import BatchState
    from deid.staging import get_staging_root
    from deid.tasks.fetch import fetch_batch
    from sqlalchemy.orm import Session

    state_engine = create_state_engine(config.resolved_state_db_url)
    staging_root = get_staging_root(config.state_db_path)
    mappings_conn_str = config.mappings_connection_string

    # Scope to configured tables; optionally narrow to tables_filter.
    table_scope = [t.name for t in config.tables]
    if tables_filter:
        table_scope = [t for t in table_scope if t in tables_filter]

    # Include all incomplete batches (failed/pending/dispatched) from state.db not already in the list.
    existing_keys = {(f.table, f.start_id, f.end_id) for f in batch_failures}
    with Session(state_engine) as session:
        db_retryable = session.query(BatchState).filter(
            BatchState.config_key == config.config_key,
            BatchState.table_name.in_(table_scope),
            BatchState.status.in_(["failed", "pending", "dispatched"]),
        ).all()
        for b in db_retryable:
            key = (b.table_name, b.start_id, b.end_id)
            if key not in existing_keys:
                batch_failures.append(BatchFailure(
                    table=b.table_name,
                    start_id=b.start_id,
                    end_id=b.end_id,
                    error=b.last_failed_reason or b.status,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    task_type="fetch",
                ))
                existing_keys.add(key)
    if db_retryable:
        logger.info(
            "Added %d retryable batch(es) from state.db (statuses: %s).",
            len(db_retryable),
            ", ".join(sorted({b.status for b in db_retryable})),
        )

    # Reset failed batches to pending and dispatch fetch tasks.
    dispatched = 0
    skipped_done = 0
    for failure in batch_failures:
        # Apply tables_filter to JSONL-sourced failures too
        if tables_filter and failure.table not in tables_filter:
            continue

        table_cfg = next(
            (t for t in config.tables if t.name == failure.table), None
        )
        if table_cfg is None:
            logger.warning(f"Table '{failure.table}' not in config — skipping.")
            continue

        if failure.start_id is None or failure.end_id is None:
            logger.warning(f"Batch for '{failure.table}' missing ID range — skipping.")
            continue

        # Reset BatchState to pending with cleared retry counter.
        # Skip batches that are already done — they succeeded on a previous attempt.
        with Session(state_engine) as session:
            batch = session.query(BatchState).filter_by(
                table_name=failure.table,
                start_id=failure.start_id,
                end_id=failure.end_id,
                config_key=config.config_key,
            ).first()
            if batch is None:
                logger.warning(
                    "Batch %s %s-%s not found in state.db — skipping.",
                    failure.table, failure.start_id, failure.end_id,
                )
                continue
            if batch.status == "done":
                logger.info(
                    "Batch %s %s-%s already done — skipping.",
                    failure.table, failure.start_id, failure.end_id,
                )
                skipped_done += 1
                continue
            batch.status = "pending"
            batch.retry_count = 0
            batch.last_failed_reason = None
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

    if skipped_done:
        logger.info("Skipped %d already-done batch(es).", skipped_done)

    if dispatched == 0:
        logger.info("No batches to retry.")
        return

    logger.info("Dispatched %d retry batches.", dispatched)

    # Poll for completion — with stuck timeout, mid-run reconciliation, and watchdog.
    import time
    from collections import defaultdict
    from deid.tasks.fetch import _claim_next_pending_batch
    from deid.staging import batch_fetched_path, batch_processed_path

    retry_keys = {(f.table, f.start_id, f.end_id) for f in batch_failures
                  if f.start_id is not None and f.end_id is not None
                  and (not tables_filter or f.table in tables_filter)}
    total_batches = len(retry_keys)
    stuck_timeout = getattr(getattr(config, "workers", None), "task_timeout", 3600) * 2
    last_done_count = 0
    last_progress_time = time.monotonic()

    status_map: dict = {}
    while True:
        await asyncio.sleep(2)

        # Single query per cycle — load all batch statuses for table_scope, filter in Python.
        try:
            with Session(state_engine) as session:
                status_map = {
                    (b.table_name, b.start_id, b.end_id): b.status
                    for b in session.query(
                        BatchState.table_name, BatchState.start_id,
                        BatchState.end_id, BatchState.status,
                    ).filter(
                        BatchState.config_key == config.config_key,
                        BatchState.table_name.in_(table_scope),
                    ).all()
                }
        except Exception as _db_exc:
            logger.warning("State DB query failed (transient?), retrying next cycle: %s", _db_exc)
            continue

        done_count = sum(1 for k in retry_keys if status_map.get(k) == "done")
        failed_count = sum(1 for k in retry_keys if status_map.get(k) == "failed")
        remaining = total_batches - done_count - failed_count

        if remaining <= 0:
            if failed_count:
                logger.warning(
                    "Retry complete: %d/%d done, %d permanently failed.",
                    done_count, total_batches, failed_count,
                )
            else:
                logger.info("Retry complete: all %d batches done.", total_batches)
            break

        if done_count > last_done_count:
            last_done_count = done_count
            last_progress_time = time.monotonic()
            logger.info(
                "Retry progress: %d/%d done, %d failed, %d remaining.",
                done_count, total_batches, failed_count, remaining,
            )
        elif time.monotonic() - last_progress_time > stuck_timeout:
            logger.error(
                "Retry stuck: no progress for %ds. %d/%d done, %d remaining.",
                stuck_timeout, done_count, total_batches, remaining,
            )
            stuck_batches = [
                b for b in session.query(BatchState).filter(
                    BatchState.config_key == config.config_key,
                    BatchState.table_name.in_(table_scope),
                    BatchState.status.not_in(["done", "failed"]),
                ).order_by(BatchState.table_name, BatchState.start_id).all()
                if (b.table_name, b.start_id, b.end_id) in retry_keys
            ]
            if stuck_batches:
                by_table: dict = defaultdict(lambda: defaultdict(list))
                for b in stuck_batches:
                    by_table[b.table_name][b.status].append(f"{b.start_id}-{b.end_id}")
                lines = ["Stuck batches at timeout:"]
                for tname, statuses in sorted(by_table.items()):
                    for status, ranges in sorted(statuses.items()):
                        lines.append(f"  {tname}: {len(ranges)} '{status}' — {ranges[:3]}")
                logger.error("\n".join(lines))
            break

        # Mid-run reconciliation: reset fetched/processed batches whose Arrow files
        # have disappeared (worker crash between write and state update).
        stalled: list = []
        try:
            with Session(state_engine) as session:
                stuck_mid = session.query(BatchState).filter(
                    BatchState.config_key == config.config_key,
                    BatchState.table_name.in_(table_scope),
                    BatchState.status.in_(["fetched", "processed"]),
                ).all()
                reconciled = 0
                for _b in stuck_mid:
                    if (_b.table_name, _b.start_id, _b.end_id) not in retry_keys:
                        continue
                    _fetched_p = batch_fetched_path(
                        staging_root, _b.table_name, _b.start_id, _b.end_id, config.config_key
                    )
                    _proc_p = batch_processed_path(
                        staging_root, _b.table_name, _b.start_id, _b.end_id, config.config_key
                    )
                    if _b.status == "fetched" and not _fetched_p.exists() and not _proc_p.exists():
                        _b.status = "pending"
                        reconciled += 1
                    elif _b.status == "processed" and not _proc_p.exists():
                        _b.status = "pending"
                        reconciled += 1
                if reconciled:
                    session.commit()
                    logger.warning(
                        "Mid-run reconciliation: reset %d stuck batch(es) to pending.", reconciled
                    )

                # Watchdog: re-dispatch stalled table chains (pending with no in-flight work).
                table_names_with_pending = [
                    r[0] for r in session.query(BatchState.table_name)
                    .filter(
                        BatchState.status == "pending",
                        BatchState.config_key == config.config_key,
                        BatchState.table_name.in_(table_scope),
                    ).distinct().all()
                ]
                stalled = [
                    tname for tname in table_names_with_pending
                    if session.query(BatchState).filter(
                        BatchState.table_name == tname,
                        BatchState.config_key == config.config_key,
                        BatchState.status.in_(["dispatched", "fetched", "processed"]),
                    ).count() == 0
                ]
        except Exception as _db_exc:
            logger.warning("State DB reconciliation failed (transient?), skipping: %s", _db_exc)

        for tname in stalled:
            with Session(state_engine) as session:
                last_done = (
                    session.query(BatchState)
                    .filter_by(table_name=tname, status="done", config_key=config.config_key)
                    .order_by(BatchState.end_id.desc())
                    .first()
                )
                last_fetched_id = (
                    last_done.actual_end_id
                    if last_done and last_done.actual_end_id is not None
                    else None
                )
                batch = _claim_next_pending_batch(session, tname, config.config_key)
                if batch:
                    cfg = _build_fetch_config(config, batch, staging_root, mappings_conn_str)
                    if last_fetched_id is not None:
                        cfg["last_fetched_id"] = last_fetched_id
                    fetch_batch.apply_async(args=[cfg], queue=f"deid-fetch-{config.config_key}")
                    logger.warning(
                        "Re-dispatched stalled fetch chain for table %s (last_fetched_id=%s).",
                        tname, last_fetched_id,
                    )
