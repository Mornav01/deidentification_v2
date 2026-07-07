"""Write stage — inserts processed batches to dest DB with idempotent transactions."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already available

import polars as pl
import pyarrow.ipc as ipc
from celery import shared_task
from sqlalchemy import text
from sqlalchemy.orm import Session

from deid.config.task_models import LogLevel, WriteTaskConfig
from deid.core.dbPkg.dbhandler import NDDBHandler
from deid.core.log_publisher import get_peak_memory_mb, make_log_record, publish_log
from deid.models.base import get_cached_state_engine
from deid.models.state import BatchState, TableState
from deid.staging import batch_processed_path
from deid.tasks.batch_utils import _is_lock_error, reset_or_fail_batch

logger = logging.getLogger("deid.tasks.write")

# Module-level handler cache for connection reuse across tasks
_handler_cache: dict[str, NDDBHandler] = {}

# Module-level guard: dest tables already created in this process
_created_dest_tables: set[str] = set()


def _publish(config: WriteTaskConfig, level: LogLevel, phase: str, message: str, **kwargs):
    if config.redis_url:
        publish_log(config.redis_url, make_log_record(level, config.table_name, phase, message, **kwargs))


def _get_cached_handler(conn_str: str) -> NDDBHandler:
    """Return a cached NDDBHandler, creating one if needed."""
    if conn_str not in _handler_cache:
        _handler_cache[conn_str] = NDDBHandler(conn_str)
    return _handler_cache[conn_str]


@shared_task(bind=True, name="deid.tasks.write.write_batch", max_retries=5)
def write_batch(self, raw_config: dict):
    """Read processed Arrow file, insert to dest DB in a single transaction."""
    config = WriteTaskConfig(**raw_config)
    batch_tag = f"{config.start_id}-{config.end_id}"
    t0 = time.monotonic()
    try:
        result = _write_batch_inner(config, batch_tag)
        duration_ms = int((time.monotonic() - t0) * 1000)
        _publish(config, LogLevel.INFO, "write",
                 f"batch {batch_tag} written",
                 batch=config.start_id, rows_in_batch=result.get("rows", 0),
                 rows_succeeded=result.get("rows", 0),
                 start_id=config.start_id, end_id=config.end_id,
                 duration_ms=duration_ms, peak_memory_mb=get_peak_memory_mb())
        return result
    except Exception as exc:
        if _is_lock_error(exc) and self.request.retries < self.max_retries:
            countdown = 10 * (2 ** self.request.retries)  # 10s, 20s, 40s, 80s, 160s
            logger.warning(
                "Lock wait timeout on %s batch %s (retry %d/%d in %ds)",
                config.table_name, batch_tag, self.request.retries + 1,
                self.max_retries, countdown,
            )
            raise self.retry(exc=exc, countdown=countdown)
        # Reset batch to pending or mark permanently failed after max retries
        try:
            max_retries = (config.run_config or {}).get("max_batch_retries", 3)
            new_status = reset_or_fail_batch(
                get_cached_state_engine(config.state_db_url),
                config.table_name, config.start_id, config.end_id,
                config.config_key, max_retries, f"{type(exc).__name__}: {exc}",
            )
            if new_status == "failed":
                logger.error(
                    "Batch %s permanently failed after %d retries: %s",
                    batch_tag, max_retries, exc,
                )
        except Exception:
            logger.warning("Could not reset batch %s after failure", batch_tag)
        _publish(config, LogLevel.ERROR, "write",
                 f"batch {batch_tag} failed: {exc}",
                 start_id=config.start_id, end_id=config.end_id,
                 error=f"{type(exc).__name__}: {exc}")
        raise


def _write_batch_inner(config: WriteTaskConfig, batch_tag: str):
    # Idempotency guard: skip if already done
    engine = get_cached_state_engine(config.state_db_url)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
            config_key=config.config_key,
        ).first()
        if batch and batch.status == "done":
            logger.info("Skipping already-done batch %s (idempotency guard)", batch_tag)
            return {"table": config.table_name, "start_id": config.start_id,
                    "end_id": config.end_id, "status": "done", "rows": 0}

    root = Path(config.staging_root)

    proc_path = batch_processed_path(root, config.table_name, config.start_id, config.end_id, config.config_key)

    # 1. Read processed Arrow file + metadata
    reader = ipc.open_file(str(proc_path))
    arrow_table = reader.read_all()
    file_metadata = arrow_table.schema.metadata or {}
    df = pl.from_arrow(arrow_table)

    # Use actual ID range from fetch metadata (falls back to config for old files)
    actual_start = file_metadata.get(b"deid_actual_start_id")
    actual_end = file_metadata.get(b"deid_actual_end_id")
    delete_start = int(actual_start) if actual_start else config.start_id
    delete_end = int(actual_end) if actual_end else config.end_id

    if df.is_empty():
        _update_batch_status_and_check_table(config)
        proc_path.unlink(missing_ok=True)
        return {"table": config.table_name, "start_id": config.start_id,
                "end_id": config.end_id, "status": "done", "rows": 0}

    # 2. Open dest DB
    dest = _get_cached_handler(config.dest_conn_str)

    # 3. Create dest table if needed (using embedded schema + PHI type overrides)
    col_schema_raw = file_metadata.get(b"deid_column_schema", b"{}")
    col_schema = {k.lower(): v for k, v in json.loads(col_schema_raw).items()}
    _apply_phi_type_overrides(col_schema, config.table_details)
    _create_dest_table(dest, config.table_name, col_schema)

    # 4. Strip extra columns added during processing (mapping joins etc.)
    #    Only write columns that exist in the original source schema.
    source_columns = [c for c in df.columns if c in col_schema]
    df = df.select(source_columns)

    # 4a. Convert empty strings → NULL for numeric columns to avoid
    #     MySQL 1366 "Incorrect integer value ''" errors.
    _NUMERIC_TYPE_KEYWORDS = ("INT", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "REAL", "BIT")
    nullify_exprs = []
    for col_name, info in col_schema.items():
        if col_name not in df.columns or df[col_name].dtype != pl.Utf8:
            continue
        type_str = (info.get("type") or "").upper()
        if any(kw in type_str for kw in _NUMERIC_TYPE_KEYWORDS):
            nullify_exprs.append(
                pl.when(pl.col(col_name).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )
    if nullify_exprs:
        df = df.with_columns(nullify_exprs)

    # 5. Idempotent write: DELETE + INSERT in single transaction
    qi = dest._qi
    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    delete_sql = text(
        f"DELETE FROM {qi(config.table_name)} "
        f"WHERE {qi(config.id_column)} BETWEEN :start_id AND :end_id"
    )
    rows = df.to_dicts()

    with dest.engine.begin() as conn:
        conn.execute(delete_sql, {"start_id": delete_start, "end_id": delete_end})
        if rows:
            columns = list(rows[0].keys())  # lowercase — matches df and bind-param names
            col_str = ", ".join(qi(col_schema.get(c, {}).get("original_name", c)) for c in columns)
            val_str = ", ".join(f":{c}" for c in columns)
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            insert_sql = text(f"INSERT INTO {qi(config.table_name)} ({col_str}) VALUES ({val_str})")
            conn.execute(insert_sql, rows)

    # 5. Update BatchState and check table completion — must happen BEFORE
    #    deleting the Arrow file so that a failure here leaves the file intact
    #    and reconciliation can re-process the batch cleanly.
    _update_batch_status_and_check_table(config)

    # 6. Delete processed file (only after state is safely committed)
    proc_path.unlink(missing_ok=True)

    logger.info("Wrote %s batch %s (%d rows)", config.table_name, batch_tag, df.height)

    return {"table": config.table_name, "start_id": config.start_id,
            "end_id": config.end_id, "status": "done", "rows": df.height}


def _update_batch_status_and_check_table(config: WriteTaskConfig):
    """Mark batch as done; if all batches for table are done, mark table completed.

    Uses a single commit to avoid a partial-update window where the batch is
    marked done but the table is not.  Retries up to 5 times on SQLite BUSY so
    that high-concurrency runs (many tables simultaneously) don't orphan batches.
    """
    engine = get_cached_state_engine(config.state_db_url)
    for attempt in range(5):
        try:
            with Session(engine) as session:
                batch = session.query(BatchState).filter_by(
                    table_name=config.table_name,
                    start_id=config.start_id,
                    end_id=config.end_id,
                    config_key=config.config_key,
                ).first()
                if batch:
                    batch.status = "done"

                remaining = session.query(BatchState).filter(
                    BatchState.table_name == config.table_name,
                    BatchState.config_key == config.config_key,
                    BatchState.status != "done",
                ).count()
                if remaining == 0:
                    table_state = session.query(TableState).filter_by(
                        table_name=config.table_name,
                        config_key=config.config_key,
                    ).first()
                    if table_state:
                        table_state.status = "completed"

                session.commit()
            return
        except Exception as exc:
            if _is_lock_error(exc) and attempt < 4:
                wait = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s, 4s
                logger.warning(
                    "state.db locked updating batch %s-%s for %s (attempt %d/5, retrying in %.1fs)",
                    config.start_id, config.end_id, config.table_name, attempt + 1, wait,
                )
                time.sleep(wait)
            else:
                raise


def _clean_type_str(raw: str) -> str:
    """Normalize a SQLAlchemy type repr for use in DDL."""
    s = raw.strip()
    # Remove trailing () from types like "LONGTEXT()" → "LONGTEXT"
    if s.endswith("()"):
        s = s[:-2]
    # Strip COLLATE clauses — dest DB may not support the same collation
    s = re.sub(r"(?i)\s+COLLATE\s+\S+", "", s)
    # Strip CHARACTER SET clauses
    s = re.sub(r"(?i)\s+CHARACTER\s+SET\s+\S+", "", s)
    s = s.strip()
    # NullType() stringifies to "NULL" — not a valid column type
    if not s or s.upper() == "NULL" or s.upper() == "NULLTYPE":
        return "TEXT"
    # Bare ENUM without values is invalid MySQL DDL; treat as VARCHAR
    if s.upper() == "ENUM" or s.upper() == "ENUM()":
        return "VARCHAR(255)"
    # VARCHAR/NVARCHAR/TEXT/NTEXT without a length means unbounded source text — use LONGTEXT
    upper = s.upper()
    if upper in ("VARCHAR", "NVARCHAR", "TEXT", "NTEXT"):
        return "LONGTEXT"
    # TEXT(n) — MSSQL text is unbounded (2^31-1 bytes); MySQL silently converts TEXT(n≤255)
    # to TINYTEXT. Promote to LONGTEXT to preserve the source semantics.
    if re.match(r"^TEXT\s*\(\s*\d+\s*\)$", s, re.I):
        return "LONGTEXT"
    # NVARCHAR(n) → VARCHAR(n), or LONGTEXT for large/max lengths
    if upper.startswith("NVARCHAR("):
        m = re.search(r"\((\d+)\)", s)
        if m:
            length = int(m.group(1))
            return "LONGTEXT" if length >= 255 else f"VARCHAR({length})"
        return "LONGTEXT"
    # MSSQL XML type has no MySQL equivalent — store as LONGTEXT
    if upper == "XML":
        return "LONGTEXT"
    if upper in ("CHAR", "NCHAR"):
        return f"{s}(255)"
    # NCHAR(n) → CHAR(n)
    if upper.startswith("NCHAR("):
        return re.sub(r"(?i)^NCHAR", "CHAR", s)
    if upper in ("VARBINARY", "BINARY"):
        return "LONGBLOB"
    # MSSQL DATETIME2 / SMALLDATETIME / DATETIMEOFFSET → DATETIME
    if re.match(r"(?i)^DATETIME2", s) or upper in ("SMALLDATETIME", "DATETIMEOFFSET"):
        return "DATETIME"
    # MSSQL UNIQUEIDENTIFIER → CHAR(36)
    if upper == "UNIQUEIDENTIFIER":
        return "CHAR(36)"
    # MSSQL MONEY / SMALLMONEY → DECIMAL
    if upper == "MONEY":
        return "DECIMAL(19,4)"
    if upper == "SMALLMONEY":
        return "DECIMAL(10,4)"
    return s


def _quote_identifier(engine, name: str) -> str:
    """Quote an identifier using the dialect's own preparer."""
    return engine.dialect.identifier_preparer.quote_identifier(name)


_MYSQL_ROW_SIZE_LIMIT = 65535


def _estimate_mysql_inline_size(ddl_type: str) -> int:
    """Estimate bytes this column contributes to MySQL row size (utf8mb4)."""
    upper = ddl_type.upper()
    m = re.search(r"VARCHAR\s*\(\s*(\d+)\s*\)", ddl_type, re.I)
    if m:
        return int(m.group(1)) * 4 + 2
    m = re.search(r"CHAR\s*\(\s*(\d+)\s*\)", ddl_type, re.I)
    if m and "VAR" not in upper:
        return int(m.group(1)) * 4
    if "BIGINT" in upper:
        return 8
    if "TINYINT" in upper:
        return 1
    if "INT" in upper:
        return 4
    if "DATETIME" in upper or "TIMESTAMP" in upper:
        return 8
    if "DATE" in upper:
        return 4
    if "TIME" in upper:
        return 3
    if "DOUBLE" in upper or "FLOAT" in upper:
        return 8
    m = re.search(r"DECIMAL\s*\(\s*(\d+)", ddl_type, re.I)
    if m:
        return max(8, (int(m.group(1)) + 2) // 2)
    if "TEXT" in upper or "BLOB" in upper or "BINARY" in upper:
        return 20  # off-row pointer
    return 255 * 4 + 2  # assume VARCHAR-like


def _adjust_ddl_for_mysql_row_limit(col_parts: list[str], col_types: list[str]) -> list[str]:
    """Convert VARCHAR columns to LONGTEXT if row size would exceed MySQL limit."""
    total = sum(_estimate_mysql_inline_size(t) for t in col_types)
    if total <= _MYSQL_ROW_SIZE_LIMIT:
        return col_parts

    # Sort VARCHAR columns by inline size descending, convert largest first
    var_indices = [
        (i, _estimate_mysql_inline_size(col_types[i]))
        for i in range(len(col_types))
        if "VARCHAR" in col_types[i].upper() or
           ("CHAR" in col_types[i].upper() and "VAR" not in col_types[i].upper())
    ]
    var_indices.sort(key=lambda x: x[1], reverse=True)

    result = list(col_parts)
    converted = []
    for idx, size in var_indices:
        if total <= _MYSQL_ROW_SIZE_LIMIT:
            break
        # Replace the type portion in the DDL fragment
        old_part = result[idx]
        # Extract column name (everything up to first space after the quoted identifier)
        try:
            m = re.match(r'^(`[^`]+`)\s', old_part)
            if not m:
                continue
            col_name_part = m.group(1)
        except Exception:
            continue
        result[idx] = f"{col_name_part} LONGTEXT"
        total = total - size + 20
        converted.append(col_name_part)

    if converted:
        logger.info(
            "[create_table] Row size exceeds MySQL limit; converted %d columns to LONGTEXT",
            len(converted),
        )
    return result


# PHI rule → destination DDL type (must match ColumnsTypeDetector)
_PHI_RULE_TO_DDL = {
    "PATIENT_ID": "BIGINT",
    "ENCOUNTER_ID": "BIGINT",
    "REFERENCE_PID": "BIGINT",
    "APPOINTMENT_ID": "BIGINT",
    "CHART_ID": "BIGINT",
    "DOB": "INTEGER",
    "PATIENT_DOB": "INTEGER",
    "DATE_OFFSET": "DATETIME",
    "STATIC_OFFSET": "DATETIME",
    "ZIP_CODE": "VARCHAR(50)",
    "NOTES": "LONGTEXT",
    "GENERIC_NOTES": "LONGTEXT",
}


def _apply_phi_type_overrides(col_schema: dict, table_details: dict | None) -> None:
    """Override source column types for PHI columns based on de-identification rules.

    After de-identification, ID columns hold nd_* BIGINT values, dates are shifted
    DATETIMEs, etc.  The source schema (INTEGER, VARCHAR) no longer matches what is
    actually written.  This mutates col_schema in-place.
    """
    if not table_details:
        return
    for col_conf in table_details.get("columns_details", []):
        if not col_conf.get("is_phi"):
            continue
        col_name = (col_conf.get("column_name") or "").lower()
        rule = col_conf.get("de_identification_rule")
        if col_name and rule and col_name in col_schema:
            ddl = _PHI_RULE_TO_DDL.get(rule)
            if ddl is None and rule.startswith("PATIENT_"):
                ddl = "BIGINT"
            if ddl:
                col_schema[col_name]["type"] = ddl
            elif rule == "MASK":
                mask_val = col_conf.get("mask_value", "")
                placeholder_len = len(f"<<{mask_val}>>")
                source_len = col_schema[col_name].get("length") or 0
                dest_len = max(source_len, placeholder_len, 50) + 10
                col_schema[col_name]["type"] = f"VARCHAR({dest_len})"


def _create_dest_table(handler: NDDBHandler, table_name: str, col_schema: dict):
    """Create destination table if it doesn't exist using exact source types."""
    if not col_schema:
        return
    if table_name in _created_dest_tables:
        return

    qi = lambda name: _quote_identifier(handler.engine, name)
    is_mysql = handler.engine.dialect.name == "mysql"
    col_defs = []
    col_types = []
    for col_name, info in col_schema.items():
        ddl_name = info.get("original_name", col_name)
        type_str = _clean_type_str(info.get("type", "VARCHAR(255)"))
        col_defs.append(f"{qi(ddl_name)} {type_str}")
        col_types.append(type_str)

    if is_mysql:
        col_defs = _adjust_ddl_for_mysql_row_limit(col_defs, col_types)

    ddl_str = f"CREATE TABLE IF NOT EXISTS {qi(table_name)} ({', '.join(col_defs)})"
    logger.debug("DDL: %s", ddl_str)
    with handler.engine.begin() as conn:
        if is_mysql:
            conn.exec_driver_sql("SET sql_mode = ''")
            conn.exec_driver_sql("SET innodb_strict_mode = 0")
        conn.exec_driver_sql(ddl_str)
        if is_mysql:
            conn.exec_driver_sql("SET innodb_strict_mode = 1")
            conn.exec_driver_sql("SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
    _created_dest_tables.add(table_name)
