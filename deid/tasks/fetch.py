"""Fetch stage — reads batches from source DB, writes Arrow IPC files."""
from __future__ import annotations

import json
import logging
import os

import polars as pl
import pyarrow.ipc as ipc
from celery import shared_task
from sqlalchemy.orm import Session

from deid.config.task_models import FetchTaskConfig
from deid.core.dbPkg.dbhandler import NDDBHandler, stream_table_paginated
from deid.models.base import create_state_engine
from deid.models.state import BatchState
from deid.staging import batch_fetched_path

logger = logging.getLogger("deid.tasks.fetch")


@shared_task(bind=True, name="deid.tasks.fetch.fetch_batch")
def fetch_batch(self, raw_config: dict):
    """Fetch a batch of rows from source DB and write to Arrow IPC file."""
    config = FetchTaskConfig(**raw_config)
    batch_tag = f"{config.start_id}-{config.end_id}"
    root = _staging_root(config)

    # 1. Fetch rows from source
    source = NDDBHandler(config.source_conn_str, read_only=True)
    try:
        col_info = source.get_columns(config.table_name)
        col_schema = {}
        for c in col_info:
            length = getattr(c.get("type"), "length", None)
            col_schema[c["name"]] = {
                "type": str(c.get("type", "")),
                "length": int(length) if length else None,
            }

        frames = list(stream_table_paginated(
            source, config.table_name,
            config.start_id, config.end_id,
            config.batch_size, config.id_column,
        ))
    finally:
        source.close()

    if not frames:
        _update_batch_status(config, "done")
        return {"table": config.table_name, "start_id": config.start_id,
                "end_id": config.end_id, "status": "done", "rows": 0}

    df = pl.concat(frames)

    # 2. Write Arrow IPC with embedded column schema metadata
    target = batch_fetched_path(root, config.table_name, config.start_id, config.end_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".tmp")

    arrow_table = df.to_arrow()
    existing_meta = arrow_table.schema.metadata or {}
    existing_meta[b"deid_column_schema"] = json.dumps(col_schema).encode()
    arrow_table = arrow_table.replace_schema_metadata(existing_meta)

    with ipc.new_file(str(tmp_path), arrow_table.schema) as writer:
        writer.write_table(arrow_table)
    os.rename(tmp_path, target)

    # 3. Update BatchState: pending -> fetched
    _update_batch_status(config, "fetched")

    # 4. Dispatch process_batch
    from deid.tasks.process import process_batch
    process_config = {
        "table_name": config.table_name,
        "start_id": config.start_id,
        "end_id": config.end_id,
        "staging_root": str(root),
        "state_db_path": config.state_db_path,
        "redis_url": config.redis_url,
        "run_config": config.run_config,
        **{k: raw_config[k] for k in (
            "mapping_db_config", "table_details", "source_conn_str",
            "offset_days", "pii_config", "pii_db_conn_str",
            "secondary_pii_configs", "dest_conn_str",
        ) if k in raw_config},
    }
    process_batch.apply_async(args=[process_config], queue="deid-process")

    logger.info("Fetched %s batch %s (%d rows)", config.table_name, batch_tag, df.height)

    return {"table": config.table_name, "start_id": config.start_id,
            "end_id": config.end_id, "status": "fetched", "rows": df.height}


def _staging_root(config: FetchTaskConfig):
    from pathlib import Path
    return Path(config.staging_root)


def _update_batch_status(config: FetchTaskConfig, status: str):
    engine = create_state_engine(config.state_db_path)
    from deid.models.base import create_all_state_tables
    create_all_state_tables(engine)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
        ).first()
        if batch:
            batch.status = status
            session.commit()
    engine.dispose()
