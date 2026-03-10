"""Async orchestrator — drives the full de-identification pipeline."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from deid.config.schema import DeidConfig
from deid.config.task_models import ProgressEvent, QCTaskConfig
from pydantic import validate_call
from deid.models.base import (
    create_all_mappings_tables,
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
    create_all_mappings_tables(mappings_engine)

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

    try:
        if "setup" in config.phases:
            logger.info("Phase: setup")
            table_row_counts, table_id_ranges = await _setup_phase(
                config, state_engine, mappings_engine
            )

        cache_paths: dict[str, str] = {}
        if "deidentify" in config.phases and table_id_ranges:
            try:
                cache_paths = await _cache_large_tables(config, table_id_ranges, table_row_counts)
            except Exception:
                logger.exception("Cache phase failed — workers will read from source DB directly")
                cache_paths = {}

        if "deidentify" in config.phases:
            logger.info("Phase: deidentify")
            await _deidentify_phase(config, state_engine, table_row_counts, table_id_ranges, cache_paths)

        if "qc" in config.phases:
            logger.info("Phase: qc")
            await _qc_phase(config, state_engine)

    finally:
        # Cleanup IPC cache.
        cache_root = Path(config.state_db_path).resolve().parent / ".deid_cache"
        if cache_root.exists():
            import shutil
            shutil.rmtree(cache_root, ignore_errors=True)
            logger.info("Cache: cleaned up %s", cache_root)

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
            log.status = "completed"
            log.completed_at = datetime.now(timezone.utc)
            log.stats = summary
            session.commit()


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _setup_phase(config: DeidConfig, state_engine, mappings_engine=None):
    """Discover tables, create destination schemas, persist state."""
    assert config.tables, "config.tables must not be empty for setup phase"

    from deid.core.dbPkg.dbhandler import NDDBHandler

    source = NDDBHandler(config.source_db.connection_string(), read_only=True)

    loop = asyncio.get_event_loop()

    # ── 1. Gather row counts for all tables in parallel ──────────────────
    @validate_call(config=dict(arbitrary_types_allowed=True))
    async def _get_count(table_name: str) -> tuple[str, int]:
        count = await loop.run_in_executor(None, source.get_rows_count, table_name)
        logger.info("  %s: %s rows", table_name, f"{count:,}")
        return table_name, count

    table_names = [t.name for t in config.tables]
    logger.info("Setup: fetching row counts for %d tables...", len(table_names))
    count_results = await asyncio.gather(*[_get_count(n) for n in table_names])
    table_row_counts = dict(count_results)

    # ── 2. Gather min/max IDs for large tables in parallel ───────────────
    threshold = config.deidentification.large_table_threshold
    large_tables = [n for n, c in table_row_counts.items() if c > threshold]
    table_id_ranges = {}

    if large_tables:
        logger.info("Setup: fetching ID ranges for %d large tables...", len(large_tables))

        @validate_call(config=dict(arbitrary_types_allowed=True))
        async def _get_range(table_name: str) -> tuple[str, tuple[int, int] | None]:
            mm = await loop.run_in_executor(None, source.get_min_max_id, table_name)
            return table_name, mm

        range_results = await asyncio.gather(*[_get_range(n) for n in large_tables])
        for name, mm in range_results:
            if mm:
                table_id_ranges[name] = mm

    # ── 3. Persist state (sequential — SQLite writes) ────────────────────
    with Session(state_engine) as session:
        db_cfg = session.query(DbConfig).first()
        if not db_cfg:
            db_cfg = DbConfig(
                name="default",
                source_conn_str=config.source_db.connection_string(),
                dest_conn_str=config.destination_db.connection_string(),
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

    # ── 4. Populate mapping tables ─────────────────────────────────────────
    from deid.core.mapping_populator import populate_mappings

    if mappings_engine is None:
        mappings_engine = create_mappings_engine(config.mappings_db_path)
        create_all_mappings_tables(mappings_engine)

    mapping_summary = populate_mappings(
        source=source,
        tables=config.tables,
        mappings_engine=mappings_engine,
        patient_id_prefix=config.deidentification.patient_id_prefix,
        max_offset=config.deidentification.date_offset_days,
        random_seed=config.deidentification.random_seed,
    )
    logger.info(
        "Setup: mappings populated — %d patients, %d encounters, %d appointments created.",
        mapping_summary["patients_created"],
        mapping_summary["encounters_created"],
        mapping_summary["appointments_created"],
    )
    return table_row_counts, table_id_ranges


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _cache_large_tables(
    config: DeidConfig,
    table_id_ranges: dict[str, tuple[int, int]],
    table_row_counts: dict[str, int] | None = None,
) -> dict[str, str]:
    """Dump large tables (those in table_id_ranges) to local Arrow IPC files.

    Returns a mapping of {table_name: cache_dir_path} for tables that were cached.
    Tables not in table_id_ranges are skipped (they won't be split).
    """
    if not table_id_ranges:
        return {}

    from functools import partial

    from deid.core.dbPkg.dbhandler import NDDBHandler, dump_table_to_ipc_cache, stream_table_paginated

    table_row_counts = table_row_counts or {}
    cache_root = Path(config.state_db_path).resolve().parent / ".deid_cache"
    loop = asyncio.get_event_loop()
    cache_paths: dict[str, str] = {}

    async def _dump_one(table_name: str) -> tuple[str, dict | None]:
        source = NDDBHandler(config.source_db.connection_string(), read_only=True)
        cache_dir = str(cache_root / table_name)
        min_id, max_id = table_id_ranges[table_name]
        try:
            stream = stream_table_paginated(
                source, table_name, min_id, max_id,
                config.deidentification.cache_batch_size,
            )
            dump_fn = partial(
                dump_table_to_ipc_cache,
                stream,
                cache_dir,
                table_name=table_name,
                estimated_rows=table_row_counts.get(table_name, 0),
            )
            result = await loop.run_in_executor(None, dump_fn)
            return table_name, result
        finally:
            source.close()

    concurrency = config.deidentification.cache_concurrency
    semaphore = asyncio.Semaphore(concurrency)
    total = len(table_id_ranges)

    table_list = ", ".join(
        f"{t} (~{table_row_counts.get(t, 0):,} rows)" for t in table_id_ranges
    )
    logger.info(
        "Cache: dumping %d large table(s) to Arrow IPC (concurrency=%d): %s",
        total, concurrency, table_list,
    )

    async def _dump_with_semaphore(idx: int, table_name: str):
        async with semaphore:
            est = table_row_counts.get(table_name, 0)
            logger.info(
                "Cache: [%d/%d] starting %s (~%s rows)...",
                idx, total, table_name, f"{est:,}",
            )
            try:
                name, summary = await _dump_one(table_name)
                if summary:
                    cache_paths[name] = summary["cache_dir"]
                    logger.info(
                        "Cache: [%d/%d] %s done — %s rows, %d batches, %.1fs",
                        idx, total, name,
                        f"{summary['rows']:,}", summary["batches"], summary["elapsed_s"],
                    )
            except Exception:
                logger.exception(
                    "Cache: [%d/%d] %s FAILED — workers will read from source DB",
                    idx, total, table_name,
                )

    await asyncio.gather(*[
        _dump_with_semaphore(idx, t)
        for idx, t in enumerate(table_id_ranges, 1)
    ])

    logger.info("Cache: complete — %d/%d tables cached", len(cache_paths), total)
    return cache_paths


@validate_call(config=dict(arbitrary_types_allowed=True))
async def _deidentify_phase(config, state_engine, table_row_counts, table_id_ranges, cache_paths=None):
    """Build task graph and dispatch to Celery, monitor progress."""
    from deid.orchestrator.task_graph import build_task_graph

    graph = build_task_graph(config, table_row_counts, table_id_ranges, cache_paths or {})
    result = graph.apply_async()

    from deid.orchestrator.progress import listen_progress

    async for event in listen_progress(config.redis_url):
        progress = ProgressEvent(**event)
        logger.info("Progress: %s — %s", progress.table, progress.status)

        with Session(state_engine) as session:
            ts = session.query(TableState).filter_by(table_name=progress.table).first()
            if ts:
                ts.status = progress.status
                if progress.status == "failed":
                    ts.failure_remarks = progress.detail
                session.commit()

        if result.ready():
            break


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
            table_config={},
        )
        qc_tasks.append(run_qc.s(qc_config.model_dump()))

    qc_group = celery_group(qc_tasks)
    result = qc_group.apply_async()
    result.get(timeout=config.workers.task_timeout)
