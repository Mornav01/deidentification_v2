"""Async orchestrator — drives the full de-identification pipeline."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from deid.config.schema import DeidConfig
from deid.config.task_models import QCTaskConfig
from pydantic import validate_call
from deid.models.base import (
    create_all_failed_rows_tables,
    create_all_state_tables,
    create_failed_rows_engine,
    create_read_only_mappings_engine,
    create_state_engine,
)
from deid.models.state import DbConfig, RunLog, TableState

logger = logging.getLogger("deid.orchestrator")


@validate_call(config=dict(arbitrary_types_allowed=True))
async def run(config: DeidConfig, config_path: str):
    """Main async entry point — runs phases from config."""
    assert config.phases, "config.phases must not be empty"
    from deid.orchestrator.log_collector import LogCollector

    run_start = datetime.now(timezone.utc)
    run_timestamp = run_start.strftime("%Y-%m-%d_%H-%M-%S")

    if config.state_db_url:
        logger.info("State DB: using MySQL at %s", config.state_db_url)
    else:
        logger.info("State DB: using SQLite at %s", config.state_db_path)

    if config.failed_rows_db_url:
        logger.info("Failed-rows DB: using MySQL at %s", config.failed_rows_db_url)
    else:
        logger.info("Failed-rows DB: using SQLite at %s", config.failed_rows_db_path)

    state_engine = create_state_engine(config.resolved_state_db_url)
    create_all_state_tables(state_engine)
    mappings_engine = create_read_only_mappings_engine(config.mappings_connection_string)
    failed_rows_engine = create_failed_rows_engine(config.resolved_failed_rows_db_url)
    create_all_failed_rows_tables(failed_rows_engine)
    failed_rows_engine.dispose()

    # ── Load pii_config from file if needed ───────────────────────────────
    if config.pii_db and not config.pii_config and config.pii_config_path:
        import yaml as _yaml
        with open(config.pii_config_path) as f:
            config.pii_config = _yaml.safe_load(f)

    if not config.secondary_pii_configs and config.secondary_pii_config_path:
        import yaml as _yaml
        with open(config.secondary_pii_config_path) as f:
            config.secondary_pii_configs = _yaml.safe_load(f)

    if config.table_overrides_path and not config.table_overrides:
        import yaml as _yaml
        with open(config.table_overrides_path) as f:
            config.table_overrides = _yaml.safe_load(f) or {}

    # ── Validate prerequisites ────────────────────────────────────────────
    from deid.models.mappings import PatientMapping

    if not config.mappings_db and not Path(config.mappings_db_path).exists():
        raise SystemExit(
            f"Mappings DB not found at '{config.mappings_db_path}'. "
            "Run `deid mapping --config <config.yaml>` first."
        )

    with Session(mappings_engine) as session:
        if session.query(PatientMapping).count() == 0:
            raise SystemExit(
                "No patient mappings found in mappings DB. "
                "Run `deid mapping --config <config.yaml>` first."
            )

    if config.pii_db:
        if not config.pii_config:
            raise SystemExit(
                "pii_db is configured but pii_config is not available. "
                "Run `deid pii-table --config <config.yaml>` first."
            )
        from sqlalchemy import create_engine as _ce, inspect as _insp
        _pii_engine = _ce(config.pii_db["master_connection_str"])
        _pii_tables = set(_insp(_pii_engine).get_table_names())
        _pii_engine.dispose()
        if config.pii_tables_config:
            missing = [t for t in config.pii_tables_config if t not in _pii_tables]
            if missing:
                raise SystemExit(
                    f"PII tables missing in destination: {missing}. "
                    "Run `deid pii-table --config <config.yaml>` first."
                )

    config_hash = hashlib.sha256(Path(config_path).read_bytes()).hexdigest()
    with Session(state_engine) as session:
        run_log = RunLog(config_hash=config_hash, phases=config.phases)
        session.add(run_log)
        session.commit()
        run_log_id = run_log.id

    # Start log collector.
    collector = LogCollector(
        log_dir=config.logging.log_dir,
        run_timestamp=run_timestamp,
    )
    collector_task = asyncio.create_task(collector.listen(config.redis_url))

    table_row_counts = {}
    table_id_ranges = {}

    _run_exc = None
    try:
        if "setup" in config.phases:
            logger.info("Phase: setup")
            table_row_counts, table_id_ranges = await _setup_phase(
                config, state_engine
            )

        if "qc" in config.phases:
            raise SystemExit(
                "'qc' phase has been removed from 'deid run'. "
                "Use 'deid qc --config <config.yaml>' instead."
            )

        if "deidentify" in config.phases:
            logger.info("Phase: deidentify")
            await _deidentify_phase(config, state_engine)

    except Exception as exc:
        _run_exc = exc
        raise
    finally:
        # Stop collector and write summary.
        collector.stop()
        # Give collector a moment to drain remaining messages.
        await asyncio.sleep(0.5)
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass

        summary = collector.write_summary()
        text_summary = collector.format_text_summary()
        logger.info(text_summary)
        print(text_summary)
        collector.close()

        # Update RunLog with stats and completion time.
        with Session(state_engine) as session:
            log = session.get(RunLog, run_log_id)
            log.status = "failed" if _run_exc else "completed"
            log.completed_at = datetime.now(timezone.utc)
            log.stats = summary
            session.commit()


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _setup_phase(config: DeidConfig, state_engine):
    """Discover tables, create destination schemas, persist state."""
    assert config.tables, "config.tables must not be empty for setup phase"

    from deid.core.dbPkg.dbhandler import NDDBHandler

    # Use read_only=False for setup so we get pool_size=5+max_overflow=5 (10 total).
    # read_only=True gives only pool_size=1+max_overflow=2 (3 total) which is too
    # small when many tables fire COUNT(*) queries concurrently during setup.
    source = NDDBHandler(config.source_db.connection_string(), read_only=False)

    loop = asyncio.get_event_loop()

    # ── 1. Gather exact row counts for all tables in parallel ─────────────
    # Semaphore matches pool_size (5) — prevents more concurrent queries than
    # the pool can serve, which previously caused pool_timeout and all tables
    # being marked failed when a large tables list was used.
    _sem = asyncio.Semaphore(5)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    async def _get_count(table_name: str) -> tuple[str, int]:
        async with _sem:
            count = await loop.run_in_executor(None, source.get_exact_row_count, table_name)
        logger.info("  %s: %s rows", table_name, f"{count:,}")
        return table_name, count

    table_names = [t.name for t in config.tables]
    logger.info("Setup: fetching row counts for %d tables...", len(table_names))
    count_results = await asyncio.gather(*[_get_count(n) for n in table_names], return_exceptions=True)

    table_row_counts = {}
    failed_tables = []
    for tname, result in zip(table_names, count_results):
        if isinstance(result, Exception):
            logger.error("  %s: failed to get row count — %s", tname, result)
            failed_tables.append(tname)
        else:
            table_row_counts[tname] = result[1]

    # Remove tables that failed row-count fetch from the run — mark them failed in state.db
    if failed_tables:
        with Session(state_engine) as session:
            db_cfg = session.query(DbConfig).filter_by(name=config.config_key).first()
            for tname in failed_tables:
                existing = session.query(TableState).filter_by(
                    table_name=tname, config_key=config.config_key
                ).first()
                if existing:
                    existing.status = "failed"
                    existing.failure_remarks = "Table not found or inaccessible in source DB during setup"
                elif db_cfg:
                    session.add(TableState(
                        db_config_id=db_cfg.id,
                        table_name=tname,
                        config_key=config.config_key,
                        status="failed",
                        failure_remarks="Table not found or inaccessible in source DB during setup",
                        rules_config={},
                    ))
            session.commit()
            logger.warning("Marked %d table(s) as failed (not found in source DB): %s", len(failed_tables), failed_tables)
        # Remove from config.tables so they are excluded from batch splitting and deidentify
        config.tables = [t for t in config.tables if t.name not in failed_tables]
        if not config.tables:
            logger.error("All tables failed row-count fetch — nothing to process.")
            return {}, {}

    table_id_ranges = {}  # kept for return value compatibility

    # ── 2. Persist state (sequential — SQLite writes) ────────────────────
    with Session(state_engine) as session:
        db_cfg = session.query(DbConfig).filter_by(name=config.config_key).first()
        if not db_cfg:
            # Store only non-secret connection info (host:port/database) in
            # state.db — never persist passwords to the SQLite file.
            src = config.source_db
            dst = config.destination_db
            db_cfg = DbConfig(
                name=config.config_key,
                source_conn_str=f"{src.type}://{src.host}:{src.port}/{src.database}",
                dest_conn_str=f"{dst.type}://{dst.host}:{dst.port}/{dst.database}",
            )
            session.add(db_cfg)
            session.commit()

        for table_cfg in config.tables:
            existing = session.query(TableState).filter_by(
                table_name=table_cfg.name, config_key=config.config_key
            ).first()
            if not existing:
                ts = TableState(
                    db_config_id=db_cfg.id,
                    table_name=table_cfg.name,
                    config_key=config.config_key,
                    status="pending",
                    row_count=table_row_counts.get(table_cfg.name, 0),
                    rules_config=table_cfg.rules,
                )
                session.add(ts)
        session.commit()

    logger.info("Setup: complete — %d tables registered.", len(config.tables))

    # ── 3. Pre-split tables into BatchState rows (OFFSET-based) ─────────
    from deid.models.state import BatchState
    from deid.staging import get_staging_root, cleanup_tmp_files

    staging_root = get_staging_root(config.state_db_path)

    with Session(state_engine) as session:
        for table_cfg in config.tables:
            tname = table_cfg.name
            # Per-table batch_size override from table_overrides YAML; falls back to global.
            overrides = (config.table_overrides or {}).get(tname, {})
            batch_size = overrides.get("batch_size") or config.deidentification.batch_size

            row_count = table_row_counts.get(tname, 0)
            if row_count == 0:
                # Mark 0-row tables as completed immediately — nothing to process.
                ts = session.query(TableState).filter_by(
                    table_name=tname, config_key=config.config_key
                ).first()
                if ts:
                    ts.status = "completed"
                    ts.row_count = 0
                logger.info("  %s: 0 rows — marked as completed (skipped).", tname)
                continue

            offset = 0
            while offset < row_count:
                end = offset + batch_size - 1
                existing = session.query(BatchState).filter_by(
                    table_name=tname, start_id=offset, end_id=end, config_key=config.config_key
                ).first()
                if existing:
                    # Always reset to pending during setup — stale "done" rows from
                    # a previously interrupted run must not count toward this run's total.
                    existing.status = "pending"
                    existing.retry_count = 0
                    existing.last_failed_reason = None
                else:
                    session.add(BatchState(
                        table_name=tname, start_id=offset, end_id=end,
                        config_key=config.config_key, status="pending"
                    ))
                offset += batch_size
        session.commit()

    # ── 4. Cleanup stale .tmp files ───────────────────────────────────────
    cleanup_tmp_files(staging_root)

    logger.info("Setup: pre-split %d tables into BatchState rows.", len(config.tables))
    return table_row_counts, table_id_ranges


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _deidentify_phase(config, state_engine):
    """Dispatch pipeline tasks based on BatchState, poll for completion."""
    import time
    from deid.models.state import BatchState
    from deid.staging import get_staging_root, reconcile
    from deid.tasks.fetch import fetch_batch
    from deid.tasks.process import process_batch
    from deid.tasks.write import write_batch

    staging_root = get_staging_root(config.state_db_path)
    reconcile(state_engine, staging_root, config_key=config.config_key)

    mappings_conn_str = config.mappings_connection_string

    # Count total batches — scoped to the tables configured for THIS run only,
    # so that completed rows from previous runs on other tables don't inflate counts.
    configured_table_names = [t.name for t in config.tables]
    with Session(state_engine) as session:
        total = (
            session.query(BatchState)
            .filter(
                BatchState.config_key == config.config_key,
                BatchState.table_name.in_(configured_table_names),
            )
            .count()
        )
        if total == 0:
            # All configured tables had 0 rows — already marked completed in setup.
            logger.info("All configured tables had 0 rows — nothing to process.")
            return

    # Initial dispatch: atomically claim and dispatch the FIRST pending batch per table.
    # Scoped to configured_table_names so that pending batches left over from a previous
    # run (tables not in the current run) are not accidentally re-dispatched.
    from deid.tasks.fetch import _claim_next_pending_batch
    with Session(state_engine) as session:
        table_names_pending = [
            r[0] for r in session.query(BatchState.table_name)
            .filter(
                BatchState.status == "pending",
                BatchState.config_key == config.config_key,
                BatchState.table_name.in_(configured_table_names),
            ).distinct().all()
        ]
    for tname in table_names_pending:
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

    # Resume in-progress batches (fetched -> process, processed -> write)
    with Session(state_engine) as session:
        for batch in session.query(BatchState).filter_by(status="fetched", config_key=config.config_key).all():
            cfg = _build_process_config(config, batch, staging_root, mappings_conn_str)
            process_batch.apply_async(args=[cfg], queue=f"deid-process-{config.config_key}")
        for batch in session.query(BatchState).filter_by(status="processed", config_key=config.config_key).all():
            cfg = _build_write_config(config, batch, staging_root)
            write_batch.apply_async(args=[cfg], queue=f"deid-write-{config.config_key}-{batch.table_name}")

    # Poll for completion
    last_done_count = 0
    last_progress_time = time.monotonic()
    stuck_timeout = config.workers.task_timeout * 2

    while True:
        await asyncio.sleep(2)

        with Session(state_engine) as session:
            done_count = (
                session.query(BatchState)
                .filter(
                    BatchState.config_key == config.config_key,
                    BatchState.status == "done",
                    BatchState.table_name.in_(configured_table_names),
                )
                .count()
            )
            failed_count = (
                session.query(BatchState)
                .filter(
                    BatchState.config_key == config.config_key,
                    BatchState.status == "failed",
                    BatchState.table_name.in_(configured_table_names),
                )
                .count()
            )

        if done_count + failed_count == total:
            if failed_count:
                logger.warning(
                    "Pipeline complete: %d/%d batches done, %d permanently failed.",
                    done_count, total, failed_count,
                )
            else:
                logger.info("All %d batches complete.", total)
            break

        if done_count > last_done_count:
            last_done_count = done_count
            last_progress_time = time.monotonic()
            suffix = f", {failed_count} permanently failed" if failed_count else ""
            logger.info("Progress: %d/%d batches done%s.", done_count, total, suffix)
        elif time.monotonic() - last_progress_time > stuck_timeout:
            logger.error(
                "Pipeline stuck: no progress for %ds. %d/%d batches done.",
                stuck_timeout, done_count, total,
            )
            with Session(state_engine) as session:
                stuck_batches = (
                    session.query(BatchState)
                    .filter(
                        BatchState.config_key == config.config_key,
                        BatchState.table_name.in_(configured_table_names),
                        BatchState.status != "done",
                    )
                    .order_by(BatchState.table_name, BatchState.start_id)
                    .all()
                )
            if stuck_batches:
                from collections import defaultdict
                by_table: dict = defaultdict(lambda: defaultdict(list))
                for b in stuck_batches:
                    by_table[b.table_name][b.status].append(f"{b.start_id}-{b.end_id}")
                lines = ["Stuck at timeout — non-done batches:"]
                for tname, statuses in sorted(by_table.items()):
                    for status, ranges in sorted(statuses.items()):
                        lines.append(f"  {tname}: {len(ranges)} '{status}' — {ranges[:3]}")
                logger.error("\n".join(lines))
            break

        # Watchdog: re-dispatch stalled table chains (pending with no in-flight work).
        # "dispatched", "fetched", and "processed" all count as in-flight.
        # Changed from `0 < done_count < total` to `done_count < total` so that a
        # failed batch (reset to 'pending') is re-dispatched even while other batches
        # for the same table are still in-flight — previously the guard blocked the
        # watchdog until ALL in-flight work finished, causing the pipeline to stall.
        if done_count + failed_count < total:
            from deid.tasks.fetch import _claim_next_pending_batch
            from deid.staging import batch_fetched_path, batch_processed_path
            with Session(state_engine) as session:
                # Mid-run reconciliation: reset stuck fetched/processed batches whose
                # Arrow files have disappeared (worker crash between file write and state
                # update, or disk issue), so the watchdog can re-dispatch them.
                stuck_mid = session.query(BatchState).filter(
                    BatchState.config_key == config.config_key,
                    BatchState.table_name.in_(configured_table_names),
                    BatchState.status.in_(["fetched", "processed"]),
                ).all()
                reconciled = 0
                for _b in stuck_mid:
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
                        "Mid-run reconciliation: reset %d stuck batch(es) to pending",
                        reconciled,
                    )

                table_names_with_pending = [
                    r[0] for r in session.query(BatchState.table_name)
                    .filter(
                        BatchState.status == "pending",
                        BatchState.config_key == config.config_key,
                        BatchState.table_name.in_(configured_table_names),
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
                        cfg = _build_fetch_config(
                            config, batch, staging_root, mappings_conn_str
                        )
                        if last_fetched_id is not None:
                            cfg["last_fetched_id"] = last_fetched_id
                        fetch_batch.apply_async(args=[cfg], queue=f"deid-fetch-{config.config_key}")
                        logger.warning(
                            "Re-dispatched stalled fetch chain for table %s (last_fetched_id=%s)",
                            tname, last_fetched_id,
                        )


def _rules_to_table_details(rules: dict[str, str], table_name: str = "") -> dict:
    """Convert a flat {column: rule} dict to the table_details format."""
    columns_details = []
    for col_name, rule in rules.items():
        columns_details.append({
            "column_name": col_name,
            "table_name": table_name,
            "is_phi": True,
            "de_identification_rule": rule,
            "mask_value": col_name.upper(),
        })
    return {
        "columns_details": columns_details,
        "ignore_rows": {},
        "batch_size": 0,
        "reference_patient_id_column": None,
        "reference_enc_id_column": None,
        "reference_mapping": "",
    }


def _get_table_details(config, table_name: str) -> dict:
    """Build table_details dict for a table from config."""
    for table_cfg in config.tables:
        if table_cfg.name == table_name:
            details = _rules_to_table_details(table_cfg.rules, table_name=table_name)
            if config.reference_mappings:
                details["reference_mapping"] = config.reference_mappings.get(table_name, "")
            return details
    return {"columns_details": []}


def _build_fetch_config(config, batch, staging_root, mappings_conn_str):
    from deid.config.task_models import FetchTaskConfig
    base = FetchTaskConfig(
        table_name=batch.table_name,
        start_id=batch.start_id,
        end_id=batch.end_id,
        source_conn_str=config.source_db.connection_string(),
        state_db_url=config.resolved_state_db_url,
        staging_root=str(staging_root),
        config_key=config.config_key,
        batch_size=config.deidentification.batch_size,
        redis_url=config.redis_url,
        run_config={"max_batch_retries": config.workers.max_batch_retries},
    ).model_dump()
    # Extra fields forwarded by fetch_batch to process_batch and write_batch
    base["mapping_db_config"] = {"connection_str": mappings_conn_str}
    base["table_details"] = _get_table_details(config, batch.table_name)
    base["offset_days"] = config.deidentification.date_offset_days
    base["dest_conn_str"] = config.destination_db.connection_string()
    # PII config for NOTES de-identification (patient name masking in free text)
    if config.pii_db:
        base["pii_config"] = config.pii_config
        base["pii_db_conn_str"] = config.pii_db
    if config.secondary_pii_configs:
        base["secondary_pii_configs"] = config.secondary_pii_configs
    base["failed_rows_db_url"] = config.resolved_failed_rows_db_url
    return base


def _build_process_config(config, batch, staging_root, mappings_conn_str):
    from deid.config.task_models import ProcessTaskConfig
    base = ProcessTaskConfig(
        table_name=batch.table_name,
        start_id=batch.start_id,
        end_id=batch.end_id,
        staging_root=str(staging_root),
        state_db_url=config.resolved_state_db_url,
        mapping_db_config={"connection_str": mappings_conn_str},
        table_details=_get_table_details(config, batch.table_name),
        source_conn_str=config.source_db.connection_string(),
        config_key=config.config_key,
        join_db_conn_str=config.join_db.connection_string() if config.join_db else None,
        offset_days=config.deidentification.date_offset_days,
        pii_config=config.pii_config,
        pii_db_conn_str=config.pii_db,
        secondary_pii_configs=config.secondary_pii_configs,
        failed_rows_db_url=config.resolved_failed_rows_db_url,
        redis_url=config.redis_url,
        run_config={"max_batch_retries": config.workers.max_batch_retries},
    ).model_dump()
    # Extra field forwarded by process_batch to write_batch
    base["dest_conn_str"] = config.destination_db.connection_string()
    return base


def _build_write_config(config, batch, staging_root):
    from deid.config.task_models import WriteTaskConfig
    return WriteTaskConfig(
        table_name=batch.table_name,
        start_id=batch.start_id,
        end_id=batch.end_id,
        staging_root=str(staging_root),
        state_db_url=config.resolved_state_db_url,
        dest_conn_str=config.destination_db.connection_string(),
        config_key=config.config_key,
        redis_url=config.redis_url,
        table_details=_get_table_details(config, batch.table_name),
        run_config={"max_batch_retries": config.workers.max_batch_retries},
    ).model_dump()


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _qc_phase(config, state_engine):
    """Dispatch QC tasks for completed tables."""
    from deid.tasks.qc import run_qc

    configured_tables = {t.name for t in config.tables}
    with Session(state_engine) as session:
        completed = session.query(TableState).filter_by(
            status="completed", config_key=config.config_key
        ).all()
        table_names = [t.table_name for t in completed if t.table_name in configured_tables]

    if not table_names:
        logger.info("No completed tables for QC (configured: %s)", configured_tables)
        return

    # Dispatch all QC tasks to this run's process workers, then wait for all results.
    results = []
    for tname in table_names:
        qc_config = QCTaskConfig(
            table_name=tname,
            source_conn_str=config.source_db.connection_string(),
            dest_conn_str=config.destination_db.connection_string(),
            offset_days=config.deidentification.date_offset_days,
            sample_size=config.qc.sample_size,
            table_config=_get_table_details(config, tname),
            qc_results_db_url=config.resolved_qc_results_db_url,
        )
        r = run_qc.apply_async(args=[qc_config.model_dump()], queue=f"deid-process-{config.config_key}")
        logger.info("Dispatched QC task for table '%s'", tname)
        results.append((tname, r))

    qc_timeout = config.qc.task_timeout
    for tname, r in results:
        r.get(timeout=qc_timeout)
        logger.info("QC completed for table '%s'", tname)
