"""Async orchestrator — drives the full de-identification pipeline."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from deid.config.schema import DeidConfig
from deid.models.base import (
    create_all_mappings_tables,
    create_all_state_tables,
    create_mappings_engine,
    create_state_engine,
)
from deid.models.state import DbConfig, RunLog, TableState

logger = logging.getLogger("deid.orchestrator")


async def run(config: DeidConfig, config_path: str):
    """Main async entry point — runs phases from config."""
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

    try:
        table_row_counts = {}
        table_id_ranges = {}

        if "setup" in config.phases:
            logger.info("Phase: setup")
            table_row_counts, table_id_ranges = await _setup_phase(
                config, state_engine
            )

        if "deidentify" in config.phases:
            logger.info("Phase: deidentify")
            await _deidentify_phase(config, state_engine, table_row_counts, table_id_ranges)

        if "qc" in config.phases:
            logger.info("Phase: qc")
            await _qc_phase(config, state_engine)

        with Session(state_engine) as session:
            log = session.get(RunLog, run_log_id)
            log.status = "completed"
            log.completed_at = datetime.now(timezone.utc)
            session.commit()

    except Exception:
        with Session(state_engine) as session:
            log = session.get(RunLog, run_log_id)
            log.status = "failed"
            log.completed_at = datetime.now(timezone.utc)
            session.commit()
        raise


async def _setup_phase(config: DeidConfig, state_engine):
    """Discover tables, create destination schemas, persist state."""
    from deid.core.dbPkg.dbhandler import NDDBHandler

    source = NDDBHandler(config.source_db.connection_string())

    table_row_counts = {}
    table_id_ranges = {}

    loop = asyncio.get_event_loop()
    for table_cfg in config.tables or []:
        row_count = await loop.run_in_executor(None, source.get_rows_count, table_cfg.name)
        table_row_counts[table_cfg.name] = row_count

        if row_count > config.deidentification.large_table_threshold:
            min_max = await loop.run_in_executor(
                None, source.get_min_max_id, table_cfg.name
            )
            if min_max:
                table_id_ranges[table_cfg.name] = min_max

        with Session(state_engine) as session:
            existing = session.query(TableState).filter_by(table_name=table_cfg.name).first()
            if not existing:
                db_cfg = session.query(DbConfig).first()
                if not db_cfg:
                    db_cfg = DbConfig(
                        name="default",
                        source_conn_str=config.source_db.connection_string(),
                        dest_conn_str=config.destination_db.connection_string(),
                    )
                    session.add(db_cfg)
                    session.commit()
                ts = TableState(
                    db_config_id=db_cfg.id,
                    table_name=table_cfg.name,
                    status="pending",
                    row_count=row_count,
                    rules_config=table_cfg.rules,
                )
                session.add(ts)
                session.commit()

    return table_row_counts, table_id_ranges


async def _deidentify_phase(config, state_engine, table_row_counts, table_id_ranges):
    """Build task graph and dispatch to Celery, monitor progress."""
    from deid.orchestrator.task_graph import build_task_graph

    graph = build_task_graph(config, table_row_counts, table_id_ranges)
    result = graph.apply_async()

    try:
        from deid.orchestrator.progress import listen_progress

        async for event in listen_progress(config.redis_url):
            table_name = event.get("table", "")
            status = event.get("status", "")
            logger.info("Progress: %s — %s", table_name, status)

            with Session(state_engine) as session:
                ts = session.query(TableState).filter_by(table_name=table_name).first()
                if ts:
                    ts.status = status
                    if status == "failed":
                        ts.failure_remarks = event.get("detail", "")
                    session.commit()

            if result.ready():
                break
    except Exception as exc:
        logger.warning("Progress monitoring interrupted: %s", exc)
        result.get(timeout=config.workers.task_timeout)


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
        qc_tasks.append(run_qc.s({
            "table_name": tname,
            "source_conn_str": config.source_db.connection_string(),
            "dest_conn_str": config.destination_db.connection_string(),
            "offset_days": config.deidentification.date_offset_days,
            "sample_size": config.qc.sample_size,
        }))

    qc_group = celery_group(qc_tasks)
    result = qc_group.apply_async()
    result.get(timeout=config.workers.task_timeout)
