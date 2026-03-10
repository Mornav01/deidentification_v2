"""Write stage — inserts processed batches to dest DB with idempotent transactions."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl
import pyarrow.ipc as ipc
from celery import shared_task
from sqlalchemy import text
from sqlalchemy.orm import Session

from deid.config.task_models import WriteTaskConfig
from deid.core.dbPkg.dbhandler import NDDBHandler
from deid.models.base import create_state_engine
from deid.models.state import BatchState, TableState
from deid.staging import batch_processed_path

logger = logging.getLogger("deid.tasks.write")


@shared_task(bind=True, name="deid.tasks.write.write_batch")
def write_batch(self, raw_config: dict):
    """Read processed Arrow file, insert to dest DB in a single transaction."""
    config = WriteTaskConfig(**raw_config)
    batch_tag = f"{config.start_id}-{config.end_id}"
    root = Path(config.staging_root)

    proc_path = batch_processed_path(root, config.table_name, config.start_id, config.end_id)

    # 1. Read processed Arrow file + metadata
    reader = ipc.open_file(str(proc_path))
    arrow_table = reader.read_all()
    file_metadata = arrow_table.schema.metadata or {}
    df = pl.from_arrow(arrow_table)

    if df.is_empty():
        proc_path.unlink(missing_ok=True)
        _update_batch_status_and_check_table(config)
        return {"table": config.table_name, "start_id": config.start_id,
                "end_id": config.end_id, "status": "done", "rows": 0}

    # 2. Open dest DB
    dest = NDDBHandler(config.dest_conn_str)
    try:
        # 3. Create dest table if needed (using embedded schema)
        col_schema_raw = file_metadata.get(b"deid_column_schema", b"{}")
        col_schema = json.loads(col_schema_raw)
        _create_dest_table(dest, config.table_name, col_schema)

        # 4. Strip extra columns added during processing (mapping joins etc.)
        #    Only write columns that exist in the original source schema.
        source_columns = [c for c in df.columns if c in col_schema]
        df = df.select(source_columns)

        # 5. Idempotent write: DELETE + INSERT in single transaction
        qi = dest._qi
        delete_sql = text(
            f"DELETE FROM {qi(config.table_name)} "
            f"WHERE {qi(config.id_column)} BETWEEN :start_id AND :end_id"
        )
        rows = df.to_dicts()

        with dest.engine.begin() as conn:
            conn.execute(delete_sql, {"start_id": config.start_id, "end_id": config.end_id})
            if rows:
                columns = list(rows[0].keys())
                col_str = ", ".join(qi(c) for c in columns)
                val_str = ", ".join(f":{c}" for c in columns)
                insert_sql = text(f"INSERT INTO {qi(config.table_name)} ({col_str}) VALUES ({val_str})")
                conn.execute(insert_sql, rows)
    finally:
        dest.close()

    # 5. Delete processed file
    proc_path.unlink(missing_ok=True)

    # 6. Update BatchState and check table completion
    _update_batch_status_and_check_table(config)

    logger.info("Wrote %s batch %s (%d rows)", config.table_name, batch_tag, df.height)

    return {"table": config.table_name, "start_id": config.start_id,
            "end_id": config.end_id, "status": "done", "rows": df.height}


def _update_batch_status_and_check_table(config: WriteTaskConfig):
    """Mark batch as done; if all batches for table are done, mark table completed."""
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
            batch.status = "done"
            session.commit()

        remaining = session.query(BatchState).filter(
            BatchState.table_name == config.table_name,
            BatchState.status != "done",
        ).count()
        if remaining == 0:
            table_state = session.query(TableState).filter_by(
                table_name=config.table_name
            ).first()
            if table_state:
                table_state.status = "completed"
                session.commit()
    engine.dispose()


def _clean_type_str(raw: str) -> str:
    """Normalize a SQLAlchemy type repr for use in DDL."""
    import re
    s = raw.strip()
    # Remove trailing () from types like "LONGTEXT()" → "LONGTEXT"
    if s.endswith("()"):
        s = s[:-2]
    # Strip COLLATE clauses — dest DB may not support the same collation
    s = re.sub(r"\s+COLLATE\s+\S+", "", s, flags=re.IGNORECASE)
    # Strip CHARACTER SET clauses
    s = re.sub(r"\s+CHARACTER\s+SET\s+\S+", "", s, flags=re.IGNORECASE)
    return s if s else "VARCHAR(255)"


def _quote_identifier(engine, name: str) -> str:
    """Quote an identifier using the dialect's own preparer."""
    return engine.dialect.identifier_preparer.quote_identifier(name)


def _create_dest_table(handler: NDDBHandler, table_name: str, col_schema: dict):
    """Create destination table if it doesn't exist using exact source types."""
    if not col_schema:
        return

    qi = lambda name: _quote_identifier(handler.engine, name)
    col_defs = []
    for col_name, info in col_schema.items():
        type_str = _clean_type_str(info.get("type", "VARCHAR(255)"))
        col_defs.append(f"{qi(col_name)} {type_str}")

    ddl_str = f"CREATE TABLE IF NOT EXISTS {qi(table_name)} ({', '.join(col_defs)})"
    logger.debug("DDL: %s", ddl_str)
    with handler.engine.begin() as conn:
        conn.exec_driver_sql(ddl_str)
