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
    create_all_state_tables,
    create_mappings_engine,
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

    state_engine = create_state_engine(config.state_db_path)
    create_all_state_tables(state_engine)
    mappings_engine = create_mappings_engine(config.mappings_db_path)

    # ── Load pii_config from file if needed ───────────────────────────────
    if config.pii_db and not config.pii_config and config.pii_config_path:
        import yaml as _yaml
        with open(config.pii_config_path) as f:
            config.pii_config = _yaml.safe_load(f)

    # ── Validate prerequisites ────────────────────────────────────────────
    from deid.models.mappings import PatientMapping

    if not Path(config.mappings_db_path).exists():
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

        if "deidentify" in config.phases:
            logger.info("Phase: deidentify")
            await _deidentify_phase(config, state_engine)

        if "qc" in config.phases:
            logger.info("Phase: qc")
            await _qc_phase(config, state_engine)

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

    source = NDDBHandler(config.source_db.connection_string(), read_only=True)

    loop = asyncio.get_event_loop()

    # ── 1. Gather exact row counts for all tables in parallel ─────────────
    @validate_call(config=dict(arbitrary_types_allowed=True))
    async def _get_count(table_name: str) -> tuple[str, int]:
        count = await loop.run_in_executor(None, source.get_rows_count, table_name)
        logger.info("  %s: ~%s rows (catalog estimate)", table_name, f"{count:,}")
        return table_name, count

    table_names = [t.name for t in config.tables]
    logger.info("Setup: fetching row counts for %d tables...", len(table_names))
    count_results = await asyncio.gather(*[_get_count(n) for n in table_names])
    table_row_counts = dict(count_results)

    table_id_ranges = {}  # kept for return value compatibility

    # ── 2. Persist state (sequential — SQLite writes) ────────────────────
    with Session(state_engine) as session:
        db_cfg = session.query(DbConfig).first()
        if not db_cfg:
            # Store only non-secret connection info (host:port/database) in
            # state.db — never persist passwords to the SQLite file.
            src = config.source_db
            dst = config.destination_db
            db_cfg = DbConfig(
                name="default",
                source_conn_str=f"{src.type}://{src.host}:{src.port}/{src.database}",
                dest_conn_str=f"{dst.type}://{dst.host}:{dst.port}/{dst.database}",
            )
            session.add(db_cfg)
            session.commit()

        for table_cfg in config.tables:
            existing = session.query(TableState).filter_by(table_name=table_cfg.name).first()
            if not existing:
                ts = TableState(
                    db_config_id=db_cfg.id,
                    table_name=table_cfg.name,
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

    batch_size = config.deidentification.batch_size
    staging_root = get_staging_root(config.state_db_path)

    with Session(state_engine) as session:
        for table_cfg in config.tables:
            tname = table_cfg.name
            row_count = table_row_counts.get(tname, 0)
            if row_count == 0:
                continue

            offset = 0
            while offset < row_count:
                end = offset + batch_size - 1
                existing = session.query(BatchState).filter_by(
                    table_name=tname, start_id=offset, end_id=end
                ).first()
                if not existing:
                    session.add(BatchState(
                        table_name=tname, start_id=offset, end_id=end, status="pending"
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
    reconcile(state_engine, staging_root)

    mappings_conn_str = f"sqlite:///{config.mappings_db_path}"

    # Count total batches
    with Session(state_engine) as session:
        total = session.query(BatchState).count()
        if total == 0:
            raise RuntimeError(
                "No BatchState rows found. Run the 'setup' phase first."
            )

    # Initial dispatch: atomically claim and dispatch the FIRST pending batch per table.
    from deid.tasks.fetch import _claim_next_pending_batch
    with Session(state_engine) as session:
        table_names_pending = [
            r[0] for r in session.query(BatchState.table_name)
            .filter_by(status="pending").distinct().all()
        ]
    for tname in table_names_pending:
        with Session(state_engine) as session:
            last_done = (
                session.query(BatchState)
                .filter_by(table_name=tname, status="done")
                .order_by(BatchState.end_id.desc())
                .first()
            )
            last_fetched_id = (
                last_done.actual_end_id
                if last_done and last_done.actual_end_id is not None
                else None
            )
            batch = _claim_next_pending_batch(session, tname)
            if batch:
                cfg = _build_fetch_config(config, batch, staging_root, mappings_conn_str)
                if last_fetched_id is not None:
                    cfg["last_fetched_id"] = last_fetched_id
                fetch_batch.apply_async(args=[cfg], queue="deid-fetch")

    # Resume in-progress batches (fetched -> process, processed -> write)
    with Session(state_engine) as session:
        for batch in session.query(BatchState).filter_by(status="fetched").all():
            cfg = _build_process_config(config, batch, staging_root, mappings_conn_str)
            process_batch.apply_async(args=[cfg], queue="deid-process")
        for batch in session.query(BatchState).filter_by(status="processed").all():
            cfg = _build_write_config(config, batch, staging_root)
            write_batch.apply_async(args=[cfg], queue=f"deid-write-{batch.table_name}")

    # Poll for completion
    last_done_count = 0
    last_progress_time = time.monotonic()
    stuck_timeout = config.workers.task_timeout * 2

    while True:
        await asyncio.sleep(2)

        with Session(state_engine) as session:
            done_count = session.query(BatchState).filter_by(status="done").count()

        if done_count == total:
            logger.info("All %d batches complete.", total)
            break

        if done_count > last_done_count:
            last_done_count = done_count
            last_progress_time = time.monotonic()
            logger.info("Progress: %d/%d batches done.", done_count, total)
        elif time.monotonic() - last_progress_time > stuck_timeout:
            logger.error(
                "Pipeline stuck: no progress for %ds. %d/%d batches done.",
                stuck_timeout, done_count, total,
            )
            break

        # Watchdog: re-dispatch stalled table chains (pending with no in-flight work).
        # "dispatched", "fetched", and "processed" all count as in-flight.
        if 0 < done_count < total:
            from deid.tasks.fetch import _claim_next_pending_batch
            with Session(state_engine) as session:
                table_names_with_pending = [
                    r[0] for r in session.query(BatchState.table_name)
                    .filter_by(status="pending")
                    .distinct()
                    .all()
                ]
                stalled = [
                    tname for tname in table_names_with_pending
                    if session.query(BatchState).filter(
                        BatchState.table_name == tname,
                        BatchState.status.in_(["dispatched", "fetched", "processed"]),
                    ).count() == 0
                ]

            for tname in stalled:
                with Session(state_engine) as session:
                    last_done = (
                        session.query(BatchState)
                        .filter_by(table_name=tname, status="done")
                        .order_by(BatchState.end_id.desc())
                        .first()
                    )
                    last_fetched_id = (
                        last_done.actual_end_id
                        if last_done and last_done.actual_end_id is not None
                        else None
                    )
                    batch = _claim_next_pending_batch(session, tname)
                    if batch:
                        cfg = _build_fetch_config(
                            config, batch, staging_root, mappings_conn_str
                        )
                        if last_fetched_id is not None:
                            cfg["last_fetched_id"] = last_fetched_id
                        fetch_batch.apply_async(args=[cfg], queue="deid-fetch")
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
            return _rules_to_table_details(table_cfg.rules, table_name=table_name)
    return {"columns_details": []}


def _build_fetch_config(config, batch, staging_root, mappings_conn_str):
    from deid.config.task_models import FetchTaskConfig
    base = FetchTaskConfig(
        table_name=batch.table_name,
        start_id=batch.start_id,
        end_id=batch.end_id,
        source_conn_str=config.source_db.connection_string(),
        state_db_path=config.state_db_path,
        staging_root=str(staging_root),
        batch_size=config.deidentification.batch_size,
        redis_url=config.redis_url,
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
    return base


def _build_process_config(config, batch, staging_root, mappings_conn_str):
    from deid.config.task_models import ProcessTaskConfig
    base = ProcessTaskConfig(
        table_name=batch.table_name,
        start_id=batch.start_id,
        end_id=batch.end_id,
        staging_root=str(staging_root),
        state_db_path=config.state_db_path,
        mapping_db_config={"connection_str": mappings_conn_str},
        table_details=_get_table_details(config, batch.table_name),
        source_conn_str=config.source_db.connection_string(),
        offset_days=config.deidentification.date_offset_days,
        pii_config=config.pii_config,
        pii_db_conn_str=config.pii_db,
        secondary_pii_configs=config.secondary_pii_configs,
        redis_url=config.redis_url,
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
        state_db_path=config.state_db_path,
        dest_conn_str=config.destination_db.connection_string(),
        redis_url=config.redis_url,
    ).model_dump()


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _qc_phase(config, state_engine):
    """Dispatch QC tasks for completed tables."""
    from celery import group as celery_group
    from deid.tasks.qc import run_qc

    with Session(state_engine) as session:
        completed = session.query(TableState).filter_by(status="completed").all()
        table_names = [t.table_name for t in completed]

    if not table_names:
        logger.info("No completed tables for QC")
        return

    qc_tasks = []
    for tname in table_names:
        qc_config = QCTaskConfig(
            table_name=tname,
            source_conn_str=config.source_db.connection_string(),
            dest_conn_str=config.destination_db.connection_string(),
            offset_days=config.deidentification.date_offset_days,
            sample_size=config.qc.sample_size,
            table_config=_get_table_details(config, tname),
        )
        qc_tasks.append(run_qc.s(qc_config.model_dump()))

    qc_group = celery_group(qc_tasks)
    result = qc_group.apply_async()
    result.get(timeout=config.workers.task_timeout)
