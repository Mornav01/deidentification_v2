import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import pandas as pd
from datetime import datetime
from collections import defaultdict
import json
import re
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# MySQL error codes that indicate a dropped/broken connection and warrant a retry
_RETRYABLE_ERRNO = frozenset({2006, 2013})  # "server has gone away", "lost connection"
_MAX_TABLE_RETRIES = 3


def _is_connection_drop(exc: Exception) -> bool:
    """Return True if exc is a retryable MySQL connection-drop error."""
    orig = getattr(exc, "orig", None)
    return bool(
        orig and hasattr(orig, "args") and orig.args
        and orig.args[0] in _RETRYABLE_ERRNO
    )


# ============================
# Logging
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Compiled once — avoids redundant recompilation for every CDC row
_RE_VALUES      = re.compile(r"\)\s*VALUES\s*\(", re.IGNORECASE)
_RE_BINLOG_INT  = re.compile(r"^-?\d+$")
_RE_BINLOG_FLOAT = re.compile(r"^-?\d+\.\d+$")

# All audit columns added/ensured on every CDC-touched staging table
_ALTER_COLS = [
    ("nd_auto_increment_id", "BIGINT DEFAULT NULL"),
    ("nd_extracted_date",        "DATETIME DEFAULT NULL"),
    ("nd_updated_at",        "DATETIME DEFAULT NULL"),
    ("nd_operation",         "VARCHAR(6)"),
    ("nd_ActiveFlag",         "VARCHAR(1)"),
]


# ============================
# CLI
# ============================
def parse_args():
    """
    Parse command-line arguments for CDC restore.

    Example:
        python cdc_restore.py --run_date "2025-10-12" --table_name "change_log" \
            --staging_schema "mobiledoc_staging" --prod_schema "mobiledoc"
    """
    parser = argparse.ArgumentParser(description="CDC restore: apply CDC change log into staging schema")
    parser.add_argument("--run_date",       required=True, help="Run date in YYYY-MM-DD format")
    parser.add_argument("--table_name",     required=True, help='CDC change-log table name (e.g. "change_log")')
    parser.add_argument("--staging_schema", required=True, help='Target staging schema (e.g. "mobiledoc_staging")')
    parser.add_argument("--prod_schema",    required=True, help='Source prod schema (e.g. "mobiledoc")')
    parser.add_argument(
        "--output_dir",
        default=os.path.dirname(os.path.abspath(__file__)),
        help="Directory for the failed_cases CSV (default: script directory)",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=10,
        help="Number of tables to process in parallel (default: 10)",
    )
    return parser.parse_args()


# ============================
# DB helpers
# ============================
def _db_url(schema: str) -> str:
    """Build a MySQL connection URL. Set DB_USER / DB_PASS / DB_HOST / DB_PORT env vars.

    Built via URL.create (same as deid/config/schema.py DbConfig.connection_string) so
    special characters in DB_PASS (e.g. @) are percent-encoded correctly.
    """
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASS", "")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=schema,
    ).render_as_string(hide_password=False)


def stream_cdc_data(engine, table_name, batch_size=10000):
    """
    Yield rows from the CDC table in ID-ordered chunks.
    Prevents memory exhaustion and long-running transaction timeouts.
    """
    with engine.connect() as conn:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    logger.info("Starting stream: %s rows from CDC table %s", f"{total_rows:,}", table_name)

    last_bf: str = ""
    last_bp: int = 0
    last_id: int = 0
    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    query = text(f"""
        SELECT * FROM {table_name}
        WHERE binlog_file IS NOT NULL
        AND (
                binlog_file > :last_bf
            OR (binlog_file = :last_bf AND binlog_pos > :last_bp)
            OR (binlog_file = :last_bf AND binlog_pos = :last_bp AND id > :last_id)
        )
        ORDER BY binlog_file ASC, binlog_pos ASC, id ASC
        LIMIT :limit
    """)

    while True:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION sort_buffer_size = 268435456"))
            result = conn.execute(query, {"last_bf": last_bf, "last_bp": last_bp, "last_id": last_id, "limit": batch_size}).fetchall()

        if not result:
            break

        for row in result:
            yield row
            last_bf = row.binlog_file
            last_bp = row.binlog_pos
            last_id = row.id

        logger.info("Progress: %s rows streamed", f"{last_id:,}")


def stream_cdc_data_for_table(
    engine,
    cdc_table,
    target_table,
    batch_size=10000,
    dump_binlog_file=None,
    dump_binlog_pos=None,
):
    """
    Yield CDC rows for a single target_table in ID-ordered chunks.
    Used by parallel workers so each thread only pulls its own table's events.

    dump_binlog_file / dump_binlog_pos (optional):
        When provided, only events whose binlog position is STRICTLY AFTER the
        dump snapshot are returned — i.e. events already captured in the
        mysqldump are silently skipped at the query level.

        Comparison rule (mirrors MySQL binlog ordering):
            binlog_file > dump_binlog_file      → include (later file)
            binlog_file = dump_binlog_file
              AND binlog_pos >= dump_binlog_pos → include (same file, at or after
                                                  MASTER_LOG_POS = first post-dump event)
            binlog_file IS NULL                 → EXCLUDE (unknown origin; safer than
                                                  risking replay of pre-dump events)
            anything else                       → exclude (pre-dump)
    """
    # Match table_name case-insensitively regardless of the column's collation:
    # the app writes the same physical table under inconsistent case (`labdata`
    # vs `LabData`), and we always pass a lowercased target.  LOWER(table_name)
    # in the WHERE guarantees every spelling is pulled by this single worker.
    target_table = target_table.lower()

    # Build the dump-position WHERE fragment once; reused in COUNT + paging queries.
    dump_filter = ""
    dump_params: dict = {}
    if dump_binlog_file is not None and dump_binlog_pos is not None:
        # Keep only events that are STRICTLY AFTER the dump snapshot:
        #   binlog_file > dump_bf            → later file, always post-dump
        #   binlog_file = dump_bf
        #     AND binlog_pos >= dump_bp      → same file; MASTER_LOG_POS is the
        #                                      position of the FIRST post-dump event,
        #                                      so >= is correct (not >).
        #
        # NOTE: events with binlog_file IS NULL are intentionally EXCLUDED.
        # cdc_parser.py always records the binlog filename, so NULL only occurs for
        # legacy / manually-inserted rows whose position relative to the dump is
        # unknown.  Including them risks replaying pre-dump events into staging.
        dump_filter = (
            " AND binlog_file IS NOT NULL AND ("
            "    binlog_file > :dump_bf"
            "    OR (binlog_file = :dump_bf AND binlog_pos >= :dump_bp)"
            ")"
        )
        dump_params = {"dump_bf": dump_binlog_file, "dump_bp": dump_binlog_pos}
        logger.info(
            "[%s] Pre-dump filter active: replay events from %s @ %s onwards",
            target_table, dump_binlog_file, dump_binlog_pos,
        )

    with engine.connect() as conn:
        total_rows = conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(f"SELECT COUNT(*) FROM {cdc_table} WHERE LOWER(table_name) = :tname {dump_filter}"),
            {"tname": target_table, **dump_params},
        ).scalar()
    logger.info("[%s] %s CDC rows to process (post-dump snapshot)", target_table, f"{total_rows:,}")

    last_bf: str = ""
    last_bp: int = 0
    last_id: int = 0
    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    query = text(f"""
        SELECT * FROM {cdc_table}
        WHERE LOWER(table_name) = :tname
        AND binlog_file IS NOT NULL
        AND (
                binlog_file > :last_bf
            OR (binlog_file = :last_bf AND binlog_pos > :last_bp)
            OR (binlog_file = :last_bf AND binlog_pos = :last_bp AND id > :last_id)
        )
        {dump_filter}
        ORDER BY binlog_file ASC, binlog_pos ASC, id ASC
        LIMIT :limit
    """)

    while True:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION sort_buffer_size = 268435456"))
            result = conn.execute(
                query,
                {"last_bf": last_bf, "last_bp": last_bp, "last_id": last_id, "limit": batch_size, "tname": target_table, **dump_params},
            ).fetchall()

        if not result:
            break

        for row in result:
            yield row
            last_bf = row.binlog_file
            last_bp = row.binlog_pos
            last_id = row.id

        logger.info("[%s] Progress: streamed up to %s @ %s (id=%s)", target_table, last_bf, last_bp, last_id)


def _find_latest_snapshot_table(conn, run_date: str, max_lookback: int = 30) -> str | None:
    """
    Walk backwards up to max_lookback days from run_date looking for a daily
    snapshot table named dump_metadata_{mmddyyyy}.  Returns the table name of
    the most recent one found, or None if none exist within the window.
    """
    from datetime import datetime, timedelta
    current = datetime.strptime(run_date, "%Y-%m-%d").date() - timedelta(days=1)
    for _ in range(max_lookback):
        candidate = f"dump_metadata_{current.strftime('%m%d%Y')}"
        exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t"
            ),
            {"t": candidate},
        ).scalar()
        if exists:
            return candidate
        current -= timedelta(days=1)
    return None


def load_dump_metadata(cdc_engine, schema_name: str, run_date: str) -> dict:
    """
    Return { table_name_lower: (binlog_file, binlog_pos) } for the most
    precise starting position available.

    Resolution order:
      1. Most recent daily snapshot dump_metadata_{mmddyyyy} within 30 days
         — records where the last CDC run ended, so this run starts exactly
           there and does not re-replay already-processed events.
      2. Base dump_metadata table — original dump binlog position, used on
         the very first run before any snapshot exists.

    Tables absent from the resolved source (or with NULL positions) receive no
    pre-dump filter and all their CDC events will be replayed.
    """
    try:
        with cdc_engine.connect() as conn:
            source = _find_latest_snapshot_table(conn, run_date)
            if source:
                logger.info("Using daily snapshot as lower-bound filter: %s", source)
            else:
                source = "dump_metadata"
                logger.info("No daily snapshot found — using base dump_metadata")

            rows = conn.execute(
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                text(f"""
                    SELECT table_name, binlog_file, binlog_pos
                    FROM   `{source}`
                    WHERE  schema_name  = :schema
                      AND  binlog_file  IS NOT NULL
                      AND  binlog_pos   IS NOT NULL
                """),
                {"schema": schema_name},
            ).fetchall()
        metadata = {r[0].lower(): (r[1], int(r[2])) for r in rows}
        logger.info(
            "Loaded %d table positions from %s (schema=%s)",
            len(metadata), source, schema_name,
        )
        return metadata
    except Exception as e:
        logger.warning(
            "Could not load dump_metadata — pre-dump filtering disabled: %s", e
        )
        return {}


# ============================
# SQL parsing
# ============================
def detect_insert_format(sql: str):
    s = sql.upper()
    if _RE_VALUES.search(s):
        return "VALUES"
    if " SET " in s:
        return "SET"
    if "INSERT INTO" in s and "SELECT" in s and "VALUES" not in s and " SET " not in s:
        return "INSERT_SELECT"
    return None


def find_matching_paren(s, start_index):
    """
    Return the index of the closing parenthesis that matches s[start_index],
    skipping parentheses inside single-quoted strings.
    """
    depth = 0
    inside_quotes = False
    escaped = False

    for i in range(start_index, len(s)):
        ch = s[i]

        if ch == "\\" and not escaped:
            escaped = True
            continue

        if ch == "'" and not escaped:
            inside_quotes = not inside_quotes

        if inside_quotes:
            escaped = False
            continue

        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i

        escaped = False

    raise ValueError("Unbalanced parentheses")


def parse_values_format(sql):
    sql = sql.strip().rstrip(";")
    upper = sql.upper()

    col_start   = upper.find("(")
    col_end     = find_matching_paren(sql, col_start)
    columns_raw = sql[col_start + 1:col_end].strip()

    val_start  = upper.find("VALUES")
    val_start  = upper.find("(", val_start)
    val_end    = find_matching_paren(sql, val_start)
    values_raw = sql[val_start + 1:val_end].strip()

    columns = [c.strip().strip("`") for c in columns_raw.split(",")]

    values = []
    current = ""
    depth = 0
    inside_quotes = False
    escaped = False

    for ch in values_raw:
        if ch == "\\" and not escaped:
            escaped = True
            current += ch
            continue

        if ch == "'" and not escaped:
            inside_quotes = not inside_quotes
            current += ch
            continue

        if ch == "(" and not inside_quotes:
            depth += 1
            current += ch
            continue

        if ch == ")" and not inside_quotes:
            depth -= 1
            current += ch
            continue

        if ch == "," and depth == 0 and not inside_quotes:
            values.append(current.strip())
            current = ""
            escaped = False
            continue

        current += ch
        escaped = False

    if current:
        values.append(current.strip())

    return columns, values


def parse_set_format(sql):
    upper = sql.upper()
    set_pos = upper.find(" SET ")
    if set_pos == -1:
        raise ValueError("SET keyword not found")

    segment = sql[set_pos + len(" SET "):].strip()

    tokens = []
    current = ""
    inside_quotes = False
    escaped = False

    for ch in segment:
        if ch == "\\" and not escaped:
            escaped = True
            current += ch
            continue

        if ch == "'" and not escaped:
            inside_quotes = not inside_quotes
            current += ch
            continue

        if ch == "," and not inside_quotes:
            tokens.append(current.strip())
            current = ""
        else:
            current += ch

        escaped = False

    if current:
        tokens.append(current.strip())

    columns, values = [], []
    for tok in tokens:
        if "=" in tok:
            key, val = tok.split("=", 1)
        elif ":" in tok:
            key, val = tok.split(":", 1)
        else:
            continue

        key = key.strip()
        val = val.strip()

        if val.lower() == "null":
            val = "NULL"
        if val.startswith("\\'") and val.endswith("\\'"):
            val = "'" + val[2:-2] + "'"

        columns.append(key)
        values.append(val)

    return columns, values


def remove_generated_columns(table_name, columns, values, generated_cols):
    gen_set = generated_cols.get(table_name.lower())
    if not gen_set:
        return columns, values

    cleaned_cols, cleaned_vals = [], []
    for c, v in zip(columns, values):
        if c not in gen_set:
            cleaned_cols.append(c)
            cleaned_vals.append(v)

    return cleaned_cols, cleaned_vals


def convert_update_join_to_select(sql):
    lower = sql.lower()

    update_pos   = lower.find("update")
    set_pos      = lower.find(" set ")
    update_block = sql[update_pos + len("update"):set_pos].strip()
    tokens       = update_block.split()
    base_table   = tokens[0]

    if len(tokens) > 1 and tokens[1].lower() not in ("left", "right", "inner", "join"):
        alias = tokens[1]
    else:
        alias = base_table

    where_pos    = lower.rfind(" where ")
    where_clause = sql[where_pos:] if where_pos != -1 else ""

    return f"SELECT {alias}.* FROM {update_block} {where_clause}".strip()


def parse_insert_select(sql: str):
    sql = sql.strip().rstrip(";")
    lower = sql.lower()

    insert_pos = lower.find("insert into")
    if insert_pos == -1:
        raise ValueError("Not an INSERT INTO statement")

    col_start = sql.find("(", insert_pos)
    if col_start == -1:
        raise ValueError("Column list not found")

    col_end = find_matching_paren(sql, col_start)

    header      = sql[insert_pos + len("insert into"):col_start].strip()
    table_name  = header.split()[0]
    insert_cols = [c.strip(" `") for c in sql[col_start + 1:col_end].split(",")]
    select_sql  = sql[col_end + 1:].strip()

    return table_name, insert_cols, select_sql


# ============================
# Enrichment helpers
# ============================
def _build_enriched_insert(table_name, prod_data, table_columns, generated_cols, op, ai_col=None,
                           force_active_flag=None, force_operation=None):
    """
    Build an upsert for the UPDATE→fallback path (row missing from staging): take
    the prod row and map it to staging columns BY NAME, excluding generated columns.
    Returns (insert_sql, enriched_rows, insert_columns).

    Prod normally mirrors staging (incl. the nd_ audit columns), but a few tables
    lack nd_operation (added only during the CDC merge) — name-based mapping handles
    both without column-count drift.  Audit handling: nd_updated_at is always set to
    now; nd_extracted_date/nd_operation/nd_ActiveFlag take prod's value when present,
    else a default (now / op / 'Y'); the AUTO_INCREMENT PK and nd_auto_increment_id
    are preserved from the prod row.  Called by both JOIN and non-JOIN UPDATE paths.

    force_active_flag / force_operation: when set, override nd_ActiveFlag /
    nd_operation regardless of prod's value — used by the DELETE prod-fallback to
    insert the prod row already tombstoned ('N' / 'DELETE').
    """
    staging_cols = list(table_columns[table_name.lower()])
    gen_cols     = generated_cols.get(table_name.lower(), set())

    # Insert every staging column EXCEPT generated ones (those can't be written).
    # We map prod values to staging columns BY NAME (not position), because prod
    # and staging can differ by a column: prod normally mirrors staging — including
    # the nd_ audit columns — but a few tables lack nd_operation (added only during
    # the CDC merge).  Name-based mapping handles both cases without count drift.
    insert_columns = [c for c in staging_cols if c not in gen_cols]

    now = datetime.now()
    enriched_data = []
    for row in prod_data:
        m = dict(row._mapping)          # {prod_column_name: value}
        vals = []
        for c in insert_columns:
            if c == "nd_updated_at":
                vals.append(now)                                   # always stamp this event
            elif c == "nd_extracted_date":
                v = m.get(c); vals.append(v if v is not None else now)
            elif c == "nd_operation":
                if force_operation is not None:
                    vals.append(force_operation)                        # forced (e.g. DELETE tombstone)
                else:
                    v = m.get(c); vals.append(v if v is not None else op)   # default when prod lacks it
            elif c == "nd_ActiveFlag":
                if force_active_flag is not None:
                    vals.append(force_active_flag)                      # forced (e.g. 'N' for soft delete)
                else:
                    v = m.get(c); vals.append(v if v is not None else 'Y')
            else:
                vals.append(m.get(c))   # real columns + nd_auto_increment_id (preserve prod's, NULL if absent)
        enriched_data.append(tuple(vals))

    placeholders = ", ".join(["%s"] * len(insert_columns))
    insert_sql = (
        f"INSERT INTO `{table_name}` "
        f"({', '.join('`' + c + '`' for c in insert_columns)}) "
        f"VALUES ({placeholders})"
        f"{_upsert_clause(insert_columns, ai_col)}"
    )
    return insert_sql, enriched_data, insert_columns


def append_audit(columns, values, next_id, op):
    columns.extend(["nd_auto_increment_id", "nd_extracted_date", "nd_updated_at", "nd_operation", "nd_ActiveFlag"])
    values.extend([str(next_id), "NOW()", "NOW()", f"'{op}'", "'Y'"])
    return columns, values


# Columns never overwritten on an upsert conflict: the AUTO_INCREMENT PK and the
# unique audit id (identity — must stay stable), plus the first-seen extract
# timestamp.  Everything else is refreshed from the new event, so a conflicting
# row is updated to the latest data instead of being silently dropped.
_UPSERT_NEVER_UPDATE = {"nd_auto_increment_id"}


def _upsert_clause(columns, ai_col=None):
    """
    Build 'ON DUPLICATE KEY UPDATE `col`=VALUES(`col`), ...' for every column
    except the identity columns (PK + nd_auto_increment_id) and nd_extracted_date.
    Returns '' if nothing is left to update.
    """
    exclude = set(_UPSERT_NEVER_UPDATE)
    if ai_col:
        exclude.add(ai_col.lower())
    parts = [f"`{c}`=VALUES(`{c}`)" for c in columns if c.lower() not in exclude]
    return " ON DUPLICATE KEY UPDATE " + ", ".join(parts) if parts else ""


def build_final_insert(table_name, columns, values, staging_schema, ai_col=None):
    col_str = ", ".join(f"`{c}`" for c in columns)
    val_str = ", ".join(values)
    sql = f"INSERT INTO {staging_schema}.`{table_name}` ({col_str}) VALUES ({val_str})"
    return sql + _upsert_clause(columns, ai_col)


def _record_upsert(cursor, stats):
    """
    Classify a single-row INSERT ... ON DUPLICATE KEY UPDATE by MySQL's
    affected-rows count so the stats distinguish genuinely new rows from
    conflict-refreshed ones (instead of the old silent-drop ambiguity):
        rowcount == 1 → new row inserted
        rowcount == 2 → existing row updated (conflict refreshed)
        rowcount == 0 → existing row matched but unchanged
    """
    rc = cursor.rowcount
    if rc == 1:
        stats["inserted"] += 1
    elif rc == 2:
        stats["refreshed"] += 1
    else:
        stats["unchanged"] += 1


def handle_insert_select(
    sql,
    staging_conn,
    prod_conn,
    table_columns,
    generated_cols,
    stats,
    nd_counter,
    table_name_override=None,
):
    table_name, _insert_cols, select_sql = parse_insert_select(sql)

    if table_name_override:
        table_name = table_name_override

    # Normalise once so every dict key (nd_counter, table_columns, …) and the
    # INSERT target use the same case — mirrors the row-level handling.
    table_name = table_name.lower()

    try:
        prod_rows = prod_conn.execute(text(select_sql)).fetchall()
    except Exception:
        stats["errors_insert_select_select"] += 1
        return

    if not prod_rows:
        stats["insert_select_no_rows"] += 1
        return

    all_cols = list(table_columns[table_name.lower()])
    gen_cols = generated_cols.get(table_name.lower(), set())
    drop_idx = {i for i, col in enumerate(all_cols) if col in gen_cols}

    next_id = nd_counter[table_name] + 1
    enriched_rows = []
    for row in prod_rows:
        cleaned = [v for i, v in enumerate(row) if i not in drop_idx]
        cleaned.extend([next_id, datetime.now(), datetime.now(), "INSERT_SELECT", "Y"])
        enriched_rows.append(tuple(cleaned))
        next_id += 1

    insert_cols_cleaned = [c for c in all_cols if c not in gen_cols]
    insert_cols_cleaned.extend(["nd_auto_increment_id", "nd_extracted_date", "nd_updated_at", "nd_operation", "nd_ActiveFlag"])

    placeholders = ", ".join(["%s"] * len(enriched_rows[0]))
    insert_sql = (
        f"INSERT IGNORE INTO `{table_name}` "
        f"({', '.join('`' + c + '`' for c in insert_cols_cleaned)}) "
        f"VALUES ({placeholders})"
    )

    try:
        cursor = staging_conn.connection.cursor()
        cursor.executemany(insert_sql, enriched_rows)
        cursor.close()
        nd_counter[table_name] = next_id
        stats["insert_select"] += len(enriched_rows)
    except Exception:
        stats["errors_insert_select_insert"] += 1


# ============================
# Row-based event helpers
# ============================

_RE_UINT_OVERFLOW = re.compile(r'^-\d+ \((\d+)\)$')

def _parse_binlog_value(raw: str) -> str:
    """
    Convert a row-based CDC payload value to a SQL literal.

    Actual stored format (from cdc_parser.py capturing "###   @N=..." lines):
        "123"                   → "123"          bare integer
        "'Snow,John'"           → same            already single-quoted string
        "''"                    → same            empty string, already quoted
        "NULL"                  → "NULL"
        "'2026-06-04 13:21:06'" → same            datetime, already quoted
        "-1 (4294967295)"       → "4294967295"    unsigned int overflow form
        "0x48656C6C6F"          → same            hex/BLOB literal
        "3.14"                  → same            decimal/float
    """
    if raw.startswith("'") and raw.endswith("'"):
        return raw                           # already a quoted string literal

    if raw.upper() == "NULL":
        return "NULL"

    if raw.upper().startswith("0X"):
        return raw                           # hex / BLOB literal

    # BIT literal: b'0', b'1', b'01101...' — pass through as-is
    if raw.lower().startswith("b'") and raw.endswith("'"):
        return raw

    # Unsigned int overflow: "-1 (4294967295)" → extract the unsigned value
    m = _RE_UINT_OVERFLOW.match(raw)
    if m:
        return m.group(1)

    if _RE_BINLOG_INT.match(raw):
        return raw                           # plain integer

    if _RE_BINLOG_FLOAT.match(raw):
        return raw                           # decimal / float

    # Unexpected format — quote defensively
    return "'" + raw.replace("'", "\\'") + "'"


def _decode_row_payload(payload: dict, table_name: str, table_columns: dict):
    """
    Map {"1": raw_val, "2": raw_val, ...} to [(col_name, sql_literal), ...].
    Keys are 1-indexed plain integers matching ORDINAL_POSITION in table_columns.
    For MINIMAL binlog images (UPDATE) the payload may be sparse — only the PK
    and changed columns are present; unmapped ordinals are simply absent.
    Columns whose index exceeds the staging table width are silently dropped.
    Returns pairs sorted by column ordinal position.
    """
    cols = table_columns.get(table_name.lower()) or []
    col_order = {c: i for i, c in enumerate(cols)}
    pairs = []
    for key, raw_val in payload.items():
        try:
            idx = int(key) - 1               # "1" → index 0, "2" → index 1, …
        except (ValueError, TypeError):
            continue
        if idx < 0 or idx >= len(cols):
            continue
        pairs.append((cols[idx], _parse_binlog_value(str(raw_val))))
    pairs.sort(key=lambda cv: col_order.get(cv[0], 999))
    return pairs


# Soft-delete marker applied to staging rows instead of physically removing them:
# flip the active flag off, stamp the operation, and refresh the update time.
# nd_operation is VARCHAR(6) — "DELETE" fits exactly.
_SOFT_DELETE_SET = "`nd_ActiveFlag` = 'N', `nd_operation` = 'DELETE', `nd_updated_at` = NOW()"


def _build_where_from_pairs(col_val_pairs):
    """
    Build a WHERE clause that matches every column in a row-based before-image:
        `c1` = v1 AND `c2` IS NULL AND ...
    Values are already SQL literals (from _parse_binlog_value), so NULL becomes
    an IS NULL test.  Used for row-based DELETE on tables with no usable PK.
    """
    parts = []
    for col, val in col_val_pairs:
        if val == "NULL":
            parts.append(f"`{col}` IS NULL")
        else:
            parts.append(f"`{col}` = {val}")
    return " AND ".join(parts)


def _apply_soft_delete(
    row_table, where_clause, op,
    prod_conn, cursor,
    table_columns, generated_cols, auto_increment_by_table,
    stats, failed_cases,
):
    """
    Soft-delete the staging rows matching where_clause: flip nd_ActiveFlag to 'N'
    (and stamp nd_operation/nd_updated_at).

    If no staging row matches, fall back to prod — mirroring the UPDATE path.
    Here `prod` is the stale mirror DB this pipeline maintains, not live prod, so
    a row deleted upstream is usually still present there with its pre-delete
    data; we fetch it and insert it ALREADY tombstoned ('N' / 'DELETE') so the
    flag is correct even when matching on a full before-image (no PK).
    If prod has nothing either, the row is genuinely absent → delete_no_match.

    Shared by both the row-based and statement-based DELETE handlers.
    """
    update_sql = f"UPDATE `{row_table}` SET {_SOFT_DELETE_SET} WHERE {where_clause}"
    try:
        cursor._defer_warnings = True
        # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query,python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cursor.execute(update_sql)
    except Exception as e:
        failed_cases.append({
            "type": "errors_delete", "table_name": row_table,
            "operation": op, "sql": update_sql, "error": str(e),
        })
        stats["errors_delete"] += 1
        return

    if cursor.rowcount and cursor.rowcount > 0:
        stats["soft_deleted"] += 1
        return

    # Not in staging — pull the row from the (stale) mirror DB.
    try:
        prod_rows = prod_conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(f"SELECT * FROM `{row_table}` WHERE {where_clause}")
        ).fetchall()
    except Exception as e:
        failed_cases.append({
            "type": "errors_delete", "table_name": row_table,
            "operation": op, "sql": update_sql, "error": f"[prod-fallback-select] {e}",
        })
        stats["errors_delete"] += 1
        return

    if not prod_rows:
        stats["delete_no_match"] += 1
        return

    ai_col = auto_increment_by_table.get(row_table.lower())
    insert_sql, enriched_data, _ = _build_enriched_insert(
        row_table, prod_rows, table_columns, generated_cols,
        "DELETE", ai_col, force_active_flag="N", force_operation="DELETE",
    )
    try:
        cursor.executemany(insert_sql, enriched_data)
        stats["soft_deleted"] += len(enriched_data)
    except Exception as e:
        failed_cases.append({
            "type": "errors_delete", "table_name": row_table,
            "operation": op, "sql": insert_sql, "error": f"[prod-fallback-insert] {e}",
        })
        stats["errors_delete"] += 1


def _soft_delete_from_statement(
    sql, row_table, op,
    prod_conn, cursor,
    table_columns, generated_cols, auto_increment_by_table,
    stats, failed_cases,
):
    """
    Soft-delete from a statement-based DELETE's raw SQL.

    Only the DELETE's WHERE clause is reused (applied to row_table), so the write
    always targets the staging table — never a prod-qualified name in the raw SQL.

      * JOIN / multi-table DELETE  → flagged to failed_cases (the WHERE references
        other tables and can't be reduced to a single-table UPDATE safely).
      * DELETE ... WHERE <cond>    → soft-delete matching rows, with prod-mirror
        fallback for rows missing from staging (see _apply_soft_delete).
      * DELETE with NO WHERE       → table-wide delete.  We refuse to touch any
        rows (flipping an entire table off is too destructive to apply blindly,
        and a parse miss could trigger it accidentally); the event is flagged to
        failed_cases and counted as errors_delete_no_where for manual review.
    """
    if " join " in sql.lower():
        failed_cases.append({
            "type": "errors_delete", "table_name": row_table, "operation": op,
            "sql": sql, "error": "multi-table/JOIN DELETE not supported for soft delete",
        })
        stats["errors_delete"] += 1
        return

    m = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
    condition = sql[m.end():].strip().rstrip(";").strip() if m else None

    if condition:
        _apply_soft_delete(
            row_table, condition, op,
            prod_conn, cursor,
            table_columns, generated_cols, auto_increment_by_table,
            stats, failed_cases,
        )
        return

    # No WHERE → table-wide delete. Refuse: do NOT update any rows, just flag it.
    logger.warning(
        "[%s] statement DELETE has no WHERE — refusing table-wide soft delete, flagging",
        row_table,
    )
    failed_cases.append({
        "type": "errors_delete_no_where", "table_name": row_table, "operation": op,
        "sql": sql, "error": "DELETE has no WHERE clause — table-wide soft delete refused",
    })
    stats["errors_delete_no_where"] += 1


def handle_row_based_event(
    op, payload, row_table,
    staging_conn, prod_conn, cursor,
    nd_counter,
    table_columns, generated_cols,
    auto_increment_by_table, staging_schema,
    stats, failed_cases,
):
    """
    Apply a single row-based CDC event (INSERT, UPDATE or DELETE) to staging.

    The binlog row-based payload is the after-image for INSERT/UPDATE and the
    before-image for DELETE.
    For UPDATE, the PK value from the after-image is used as the WHERE key
    (PKs virtually never change, so before/after values are identical).
    If the row is absent from staging, UPDATE falls back to an upsert.
    DELETE is a soft delete: the matching staging row's nd_ActiveFlag is flipped
    to 'N' (matched by PK, or by the full before-image when no PK exists).
    """
    # Lazy-init nd_counter — mirrors the statement-based path
    if row_table not in nd_counter:
        max_nd = prod_conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(f"SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM `{row_table}`")
        ).scalar() or 0
        nd_counter[row_table] = int(max_nd)

    if not table_columns.get(row_table.lower()):
        logger.warning("[%s] row-based event: no column metadata — skipping", row_table)
        stats["skipped_row_based"] += 1
        return

    col_val_pairs = _decode_row_payload(payload, row_table, table_columns)
    if not col_val_pairs:
        stats["skipped_row_based"] += 1
        return

    ai_col = auto_increment_by_table.get(row_table.lower())

    # ----------------------------------------------------------
    # INSERT
    # ----------------------------------------------------------
    if op == "INSERT":
        columns = [c for c, _ in col_val_pairs]
        values  = [v for _, v in col_val_pairs]

        # Keep the explicit PK — row-based events always carry the real prod PK,
        # so staging gets the exact same PK as prod (no re-assignment).
        columns, values = remove_generated_columns(row_table, columns, values, generated_cols)

        if not columns or len(columns) != len(values):
            stats["errors_insert_mismatch"] += 1
            return

        # AI table whose after-image somehow lacks the PK column: refuse the insert
        # rather than let MySQL auto-assign and drift staging out of sync with prod.
        if ai_col and ai_col.lower() not in {c.lower() for c in columns}:
            failed_cases.append({
                "type": "errors_insert_no_pk", "table_name": row_table,
                "operation": op, "sql": "",
                "error": f"row-based INSERT after-image missing PK column '{ai_col}' — re-parse required",
            })
            stats["errors_insert_no_pk"] += 1
            return

        next_id = nd_counter[row_table] + 1
        columns, values = append_audit(columns, values, next_id, op)
        nd_counter[row_table] = next_id

        final_sql = build_final_insert(row_table, columns, values, staging_schema, ai_col)

        try:
            cursor._defer_warnings = True
            cursor.execute(final_sql)
            _record_upsert(cursor, stats)
        except Exception as e:
            failed_cases.append({
                "type": "errors_insert", "table_name": row_table,
                "operation": op, "sql": final_sql, "error": str(e),
            })
            stats["errors_insert"] += 1

    # ----------------------------------------------------------
    # UPDATE — payload is the after-image
    # ----------------------------------------------------------
    elif op == "UPDATE":
        if not ai_col:
            logger.warning(
                "[%s] row-based UPDATE: no PK/AI column known — cannot build WHERE, skipping",
                row_table,
            )
            stats["skipped_row_based"] += 1
            return

        pk_val = None
        set_parts = []
        for col, val in col_val_pairs:
            if col.lower() == ai_col.lower():
                pk_val = val
            else:
                set_parts.append(f"`{col}` = {val}")

        if pk_val is None:
            logger.warning(
                "[%s] row-based UPDATE: PK column '%s' not in payload — skipping",
                row_table, ai_col,
            )
            stats["skipped_row_based"] += 1
            return

        # staging_conn.execute raises on connection drop → propagates to outer except
        staging_count = staging_conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(f"SELECT COUNT(*) FROM `{row_table}` WHERE `{ai_col}` = {pk_val}")
        ).scalar() or 0

        if staging_count > 0:
            set_parts.extend(["`nd_updated_at` = NOW()", "`nd_operation` = 'UPDATE'"])
            update_sql = (
                f"UPDATE `{row_table}` SET {', '.join(set_parts)} "
                f"WHERE `{ai_col}` = {pk_val}"
            )
            try:
                cursor._defer_warnings = True
                cursor.execute(update_sql)
                stats["updated"] += 1
            except Exception as e:
                failed_cases.append({
                    "type": "errors_update", "table_name": row_table,
                    "operation": op, "sql": update_sql, "error": str(e),
                })
                stats["errors_update"] += 1
        else:
            # Row missing from staging — upsert the full after-image
            columns = [c for c, _ in col_val_pairs]
            values  = [v for _, v in col_val_pairs]

            # Keep the explicit PK — use the same after-image PK so the row
            # lands at the correct ID in staging, not a wrong auto-assigned one.
            columns, values = remove_generated_columns(row_table, columns, values, generated_cols)

            next_id = nd_counter[row_table] + 1
            columns, values = append_audit(columns, values, next_id, op)
            nd_counter[row_table] = next_id

            final_sql = build_final_insert(row_table, columns, values, staging_schema, ai_col)

            try:
                cursor._defer_warnings = True
                cursor.execute(final_sql)
                stats["updated"] += 1
            except Exception as e:
                failed_cases.append({
                    "type": "errors_update", "table_name": row_table,
                    "operation": op, "sql": final_sql, "error": str(e),
                })
                stats["errors_update"] += 1

    # ----------------------------------------------------------
    # DELETE — soft delete. Row-based DELETE carries the FULL before-image, so
    # match by PK when available, otherwise by every column in the image, and
    # flip nd_ActiveFlag to 'N' instead of removing the row.
    # ----------------------------------------------------------
    elif op == "DELETE":
        pk_val = None
        if ai_col:
            for col, val in col_val_pairs:
                if col.lower() == ai_col.lower():
                    pk_val = val
                    break

        if pk_val is not None:
            where_clause = f"`{ai_col}` = {pk_val}"
        else:
            # No usable PK (table has none, or it is absent from the image) —
            # fall back to matching the full before-image.
            if ai_col:
                logger.warning(
                    "[%s] row-based DELETE: PK column '%s' not in payload — "
                    "matching on full before-image", row_table, ai_col,
                )
            where_clause = _build_where_from_pairs(col_val_pairs)

        if not where_clause:
            stats["skipped_row_based"] += 1
            return

        _apply_soft_delete(
            row_table, where_clause, op,
            prod_conn, cursor,
            table_columns, generated_cols, auto_increment_by_table,
            stats, failed_cases,
        )


# ============================
# Per-table worker
# ============================
def process_table(
    table_name,
    cdc_table,
    staging_schema,
    cdc_engine,
    staging_engine,
    prod_engine,
    table_columns,
    generated_cols,
    new_tables,
    auto_increment_by_table,
    dump_metadata=None,
):
    """
    Process all CDC events for a single table.
    Each call opens its own DB connections, making it fully thread-safe.
    Returns (stats_dict, failed_cases_list).

    dump_metadata (optional):
        Dict returned by load_dump_metadata().  When present, CDC events that
        occurred at or before the mysqldump snapshot position for this table
        are skipped — they were already captured in the dump and replaying
        them would cause duplicate-key conflicts or silent INSERT IGNORE drops.
    """
    if table_name.lower() in new_tables:
        logger.info("[%s] Skipping — new table with no staging counterpart", table_name)
        return _empty_stats(), []

    # Resolve this table's dump snapshot position (may be None if not in metadata)
    _dump_pos    = (dump_metadata or {}).get(table_name.lower())
    dump_bf      = _dump_pos[0] if _dump_pos else None
    dump_bp      = _dump_pos[1] if _dump_pos else None

    # Warn loudly when dump_metadata was loaded but this table has no entry:
    # stream_cdc_data_for_table() will apply NO binlog-position filter, so ALL
    # change_log events (including pre-dump ones) will be replayed.
    # Typical causes: dump file missing from folder, or mysqldump ran without
    # --master-data=2 (so no CHANGE MASTER TO header was written).
    if dump_metadata is not None and _dump_pos is None:
        logger.warning(
            "[%s] NOT found in dump_metadata — pre-dump binlog filter is DISABLED. "
            "All change_log events for this table will be replayed. "
            "If this is unexpected, re-run parse_dump_metadata.py and ensure "
            "the dump file was created with --master-data=2 (or --source-data=2).",
            table_name,
        )
    elif _dump_pos is not None:
        logger.debug(
            "[%s] dump_metadata pos: %s @ %s", table_name, dump_bf, dump_bp
        )

    stats = _empty_stats()
    failed_cases = []
    nd_counter = {}
    BATCH_SIZE = 1000

    with prod_engine.connect() as prod_conn, staging_engine.connect() as staging_conn:
        staging_conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
        cursor = staging_conn.connection.cursor()

        # PKs are no longer re-assigned in staging: row-based events carry the real
        # prod PK in their after-image, and statement-based INSERTs inject the prod
        # PK from the captured SET INSERT_ID value.  MySQL advances its own
        # AUTO_INCREMENT counter correctly off those explicit PKs, so the old
        # in-memory tracker / ALTER TABLE reset machinery is no longer needed.

        for i, row in enumerate(
            stream_cdc_data_for_table(
                cdc_engine, cdc_table, table_name,
                batch_size=10000,
                dump_binlog_file=dump_bf,
                dump_binlog_pos=dump_bp,
            ), 1
        ):
            # Normalise the table name to lowercase.  The app writes the same
            # physical table under inconsistent case (e.g. `labdata` vs `LabData`),
            # which MySQL resolves to one table — but keying per-table state (esp.
            # nd_counter) by the raw case would create two independent counters that
            # emit identical nd_auto_increment_id values, colliding on the
            # uniq_nd_auto_increment_id UNIQUE key and triggering wrong-row upserts.
            row_table = row[1].lower()
            op        = row[2]
            _payload  = json.loads(row[3])
            if "raw_sql" not in _payload:
                # Row-based binlog event — payload is column-index dict (@1, @2, …).
                try:
                    handle_row_based_event(
                        op, _payload, row_table,
                        staging_conn, prod_conn, cursor,
                        nd_counter,
                        table_columns, generated_cols,
                        auto_increment_by_table, staging_schema,
                        stats, failed_cases,
                    )
                except SQLAlchemyError as e:
                    if _is_connection_drop(e):
                        raise
                    failed_cases.append({"type": "errors", "table_name": row_table, "operation": op, "sql": "", "error": str(e)})
                    stats["errors"] += 1
                if i % BATCH_SIZE == 0:
                    staging_conn.connection.commit()
                    cursor.close()
                    cursor = staging_conn.connection.cursor()
                    logger.info("[%s] Batch %s: %s", table_name, f"{i:,}", stats)
                continue
            sql = _payload["raw_sql"]

            try:
                if row_table not in nd_counter:
                    max_nd = prod_conn.execute(
                        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                        text(f"SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM `{row_table}`")
                    ).scalar() or 0
                    nd_counter[row_table] = int(max_nd)

                # ----------------------------------------------------------
                # UPDATE
                # ----------------------------------------------------------
                if op == "UPDATE":
                    is_join_update = " join " in sql.lower()

                    if is_join_update:
                        select_sql   = convert_update_join_to_select(sql)
                        staging_rows = staging_conn.execute(text(select_sql)).fetchall()

                        if staging_rows:
                            try:
                                cursor._defer_warnings = True
                                cursor.execute(sql)
                                stats["updated"] += 1
                            except Exception as e:
                                failed_cases.append({"type": "errors_update", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
                                stats["errors_update"] += 1
                            continue

                        prod_data = prod_conn.execute(text(select_sql)).fetchall()
                        if not prod_data:
                            failed_cases.append({"type": "errors_update_prod", "table_name": row_table, "operation": op, "sql": sql, "error": "no data found in prod"})
                            stats["errors_update_prod"] += 1
                            continue

                    else:
                        where_index = sql.lower().rfind("where")
                        condition   = sql[where_index + 5:].strip() if where_index != -1 else None

                        if condition is None:
                            stats["update_where_none"] += 1

                        staging_query = f"SELECT COUNT(*) FROM `{row_table}`" + (f" WHERE {condition}" if condition else "")
                        prod_query    = f"SELECT * FROM `{row_table}`"          + (f" WHERE {condition}" if condition else "")

                        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                        staging_count = staging_conn.execute(text(staging_query)).scalar()
                        if staging_count > 0:
                            try:
                                cursor._defer_warnings = True
                                cursor.execute(sql)
                                stats["updated"] += 1
                            except Exception as e:
                                failed_cases.append({"type": "errors_update", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
                                stats["errors_update"] += 1
                            continue

                        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                        prod_data = prod_conn.execute(text(prod_query)).fetchall()
                        if not prod_data:
                            failed_cases.append({"type": "errors_update_prod", "table_name": row_table, "operation": op, "sql": sql, "error": "no data found in prod"})
                            stats["errors_update_prod"] += 1
                            continue

                    insert_sql, enriched_data, _ = _build_enriched_insert(
                        row_table,
                        prod_data,
                        table_columns,
                        generated_cols,
                        op,
                        auto_increment_by_table.get(row_table.lower()),
                    )
                    try:
                        # Fallback path: insert the prod row first, then replay the UPDATE.
                        # Track which statement failed so the error isn't misattributed.
                        _stage = "fallback_insert"
                        cursor.executemany(insert_sql, enriched_data)
                        _stage = "update"
                        cursor._defer_warnings = True
                        cursor.execute(sql)
                        stats["updated"] += 1
                    except Exception as e:
                        failed_cases.append({
                            "type": "errors_update", "table_name": row_table, "operation": op,
                            "sql": insert_sql if _stage == "fallback_insert" else sql,
                            "error": f"[{_stage}] {e}",
                        })
                        stats["errors_update"] += 1

                # ----------------------------------------------------------
                # INSERT
                # ----------------------------------------------------------
                elif op == "INSERT":
                    fmt     = detect_insert_format(sql)
                    next_id = nd_counter[row_table] + 1

                    if fmt is None:
                        failed_cases.append({"type": "errors_insert_none_fmt", "table_name": row_table, "operation": op, "sql": sql, "error": "unknown format"})
                        stats["errors_insert_none_fmt"] += 1
                        continue
                    elif fmt == "VALUES":
                        try:
                            columns, values = parse_values_format(sql)
                        except ValueError:
                            stats["errors_insert"] += 1
                            continue
                    elif fmt == "SET":
                        try:
                            columns, values = parse_set_format(sql)
                        except ValueError:
                            stats["errors_insert"] += 1
                            continue
                    elif fmt == "INSERT_SELECT":
                        failed_cases.append({"type": "errors_insert_select", "table_name": row_table, "operation": op, "sql": sql, "error": "insert_select"})
                        stats["errors_insert_select"] += 1
                        continue

                    columns, values = remove_generated_columns(row_table, columns, values, generated_cols)
                    # Preserve the exact prod PK.  Statement-based INSERTs don't carry
                    # the AUTO_INCREMENT value in the VALUES list — it arrives as a
                    # separate SET INSERT_ID event, captured by cdc_parser as
                    # "insert_id".  Inject it as an explicit PK so staging gets the
                    # same PK as prod (no re-assignment, no drift).
                    ai_col     = auto_increment_by_table.get(row_table.lower())
                    pk_present = ai_col is not None and ai_col.lower() in {c.lower() for c in columns}
                    if ai_col and not pk_present:
                        # AI table whose statement has no explicit PK: the prod PK MUST
                        # come from a valid (positive) captured insert_id.  If it's
                        # missing/invalid, refuse the insert — letting MySQL auto-assign
                        # would silently drift staging PKs out of sync with prod.  Route
                        # to failed_cases so a re-parse (which captures insert_id) is forced.
                        insert_id = _payload.get("insert_id")
                        try:
                            valid_pk = insert_id is not None and int(insert_id) > 0
                        except (TypeError, ValueError):
                            valid_pk = False
                        if not valid_pk:
                            failed_cases.append({
                                "type": "errors_insert_no_pk", "table_name": row_table,
                                "operation": op, "sql": sql,
                                "error": f"missing/invalid insert_id ({insert_id!r}) for AUTO_INCREMENT table — re-parse required",
                            })
                            stats["errors_insert_no_pk"] += 1
                            continue
                        columns.append(ai_col)
                        values.append(str(int(insert_id)))

                    if len(columns) != len(values):
                        stats["errors_insert_mismatch"] += 1
                        continue

                    columns, values = append_audit(columns, values, next_id, op)
                    nd_counter[row_table] = next_id

                    final_sql = build_final_insert(row_table, columns, values, staging_schema, ai_col)

                    try:
                        cursor._defer_warnings = True
                        cursor.execute(final_sql)
                        _record_upsert(cursor, stats)
                    except Exception as e:
                        failed_cases.append({"type": "errors_insert", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
                        stats["errors_insert"] += 1

                # ----------------------------------------------------------
                # DELETE — soft delete (see _soft_delete_from_statement):
                # replay as an UPDATE that flips nd_ActiveFlag to 'N' on the rows
                # the DELETE would have removed, with prod-mirror fallback for rows
                # missing from staging and table-wide handling for no-WHERE deletes.
                # ----------------------------------------------------------
                elif op == "DELETE":
                    _soft_delete_from_statement(
                        sql, row_table, op,
                        prod_conn, cursor,
                        table_columns, generated_cols, auto_increment_by_table,
                        stats, failed_cases,
                    )

            except SQLAlchemyError as e:
                if _is_connection_drop(e):
                    raise  # propagate so run_restore can retry the whole table
                failed_cases.append({"type": "errors", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
                stats["errors"] += 1

            if i % BATCH_SIZE == 0:
                staging_conn.connection.commit()
                cursor.close()
                cursor = staging_conn.connection.cursor()
                logger.info("[%s] Batch %s: %s", table_name, f"{i:,}", stats)

        staging_conn.connection.commit()
        cursor.close()
        staging_conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

    logger.info("[%s] Done: %s", table_name, stats)
    return stats, failed_cases


def _empty_stats():
    return {
        "inserted": 0, "refreshed": 0, "unchanged": 0,
        "updated": 0, "update_where_none": 0,
        "soft_deleted": 0, "delete_no_match": 0, "errors_delete_no_where": 0, "errors_delete": 0,
        "insert_select": 0, "errors": 0, "errors_update": 0,
        "errors_update_prod": 0, "errors_insert_none_fmt": 0,
        "errors_insert": 0, "errors_insert_no_pk": 0, "errors_insert_select": 0,
        "errors_insert_select_select": 0, "insert_select_no_rows": 0,
        "errors_insert_select_insert": 0, "errors_insert_mismatch": 0,
        "skipped_row_based": 0,
    }


# ============================
# Core restore
# ============================
def run_restore(run_date, cdc_table, staging_schema, prod_schema, output_dir, max_workers=5):
    """
    Read every event from the CDC change-log table and apply it into the staging schema.
    Tables are processed in parallel (up to max_workers at a time); events within
    each table are always applied in their original CDC order.
    """
    start_time = datetime.now()

    cdc_engine     = create_engine(_db_url("cdc"),          pool_size=max_workers + 2, max_overflow=max_workers, pool_pre_ping=True)
    staging_engine = create_engine(_db_url(staging_schema), pool_size=max_workers + 2, max_overflow=max_workers, pool_pre_ping=True)
    prod_engine    = create_engine(_db_url(prod_schema),    pool_size=max_workers + 2, max_overflow=max_workers, pool_pre_ping=True)

    # Discover tables referenced in the CDC log
    with cdc_engine.connect() as conn:
        tables_statements = conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(f"SELECT DISTINCT table_name FROM {cdc_table}")
        ).fetchall()
    logger.info("Total tables in CDC: %d", len(tables_statements))

    # Generated column metadata (VIRTUAL / STORED — cannot be inserted directly)
    generated_cols = defaultdict(set)
    with staging_engine.connect() as conn:
        logger.info("Loading generated column metadata...")
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        gen_rows = conn.execute(text(f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{staging_schema}'
            AND (EXTRA LIKE '%VIRTUAL%' OR EXTRA LIKE '%STORED%')
        """)).fetchall()

    for table, col in gen_rows:
        generated_cols[table.lower()].add(col)
    logger.info("Tables with generated columns: %d", len(generated_cols))

    # Ordered column list per table (order matters for positional row alignment)
    table_columns = defaultdict(list)
    with staging_engine.connect() as conn:
        logger.info("Loading column metadata...")
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        existing_cols = conn.execute(text(f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{staging_schema}'
            ORDER BY ORDINAL_POSITION
        """)).fetchall()

    for tname, cname in existing_cols:
        table_columns[tname.lower()].append(cname)
    logger.info("Cached column metadata for %d tables", len(table_columns))

    auto_increment_by_table = {}
    with staging_engine.connect() as conn:
        logger.info("Loading AUTO_INCREMENT column per table...")
        ai_rows = conn.execute(
            text("""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND EXTRA LIKE '%auto_increment%'
            """),
            {"schema": staging_schema},
        ).fetchall()
    for tnm, cnm in ai_rows:
        auto_increment_by_table[tnm.lower()] = cnm
    logger.info("Tables with AUTO_INCREMENT: %d", len(auto_increment_by_table))

    # Load per-table dump snapshot positions from cdc.dump_metadata.
    # These tell us the exact binlog (file, pos) at which each table's dump
    # was taken.  Events at or before that position are already captured in the
    # dump and must NOT be replayed.
    dump_metadata = load_dump_metadata(cdc_engine, prod_schema, run_date)

    # Relax MySQL strict mode before any DDL / DML
    with staging_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
        conn.execute(text("SET GLOBAL sql_mode = REPLACE(@@GLOBAL.sql_mode, 'NO_ZERO_DATE', '');"))
        conn.execute(text("SET GLOBAL sql_mode = REPLACE(@@GLOBAL.sql_mode, 'STRICT_TRANS_TABLES', '');"))
        conn.execute(text("""
            SET SESSION sql_mode = (SELECT REPLACE(REPLACE(REPLACE(@@SESSION.sql_mode,
                'STRICT_TRANS_TABLES', ''),
                'NO_ZERO_DATE', ''),
                'NO_ZERO_IN_DATE', ''));
        """))
    logger.info("MySQL config updated")

    # Ensure all audit columns exist in every CDC-touched table
    new_tables = []
    with staging_engine.begin() as conn:
        for (tname,) in tables_statements:
            if tname.lower() not in table_columns:
                logger.warning("Found new table: %s", tname)
                new_tables.append(tname.lower())
                continue

            for col_name, col_def in _ALTER_COLS:
                if col_name in table_columns[tname.lower()]:
                    continue

                alter_sql = f"ALTER TABLE `{tname}` ADD COLUMN `{col_name}` {col_def}"
                try:
                    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                    conn.execute(text(alter_sql))
                    table_columns[tname.lower()].append(col_name)
                except Exception as e:
                    orig = getattr(e, "orig", None)
                    orig_code = orig.args[0] if (orig and hasattr(orig, "args") and orig.args) else None
                    if orig_code == 1118 and "VARCHAR" in col_def.upper():
                        # Row too wide for VARCHAR — retry with TEXT (stored off-page, no row-size cost)
                        try:
                            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                            conn.execute(text(f"ALTER TABLE `{tname}` ADD COLUMN `{col_name}` TEXT"))
                            table_columns[tname.lower()].append(col_name)
                            logger.warning("Added %s.%s as TEXT (row too wide for %s)", tname, col_name, col_def)
                        except Exception as e2:
                            logger.warning("Skipped %s.%s: %s", tname, col_name, e2)
                    else:
                        logger.warning("Skipped %s.%s: %s", tname, col_name, e)

    new_tables_set = set(new_tables)
    logger.warning("Found %d new tables in this batch: %s", len(new_tables), new_tables)

    # ------------------------------------------------------------------
    # Parallel dispatch — one worker per table, up to max_workers at once
    # ------------------------------------------------------------------
    # Lowercase + dedupe: the same physical table can appear under multiple case
    # spellings in change_log (e.g. `labdata` vs `LabData`).  Collapsing to one
    # lowercased name guarantees a single worker (hence a single nd_counter) per
    # physical table, so case variants can't spawn parallel workers that emit
    # colliding nd_auto_increment_id values.
    # all_tables    = [row[0] for row in tables_statements if row[0].lower() not in new_tables_set]
    # all_tables = ["assessment_notes_history", "billingdata", "cpt_validcodes", "cptcode_base", "document", "edi_dfr_info", "edi_inv_cpt", "edi_inv_diagnosis", "edi_inv_insurance", "edi_invoice", "enc", "encaddendums", "encounters", "hcpcscode_base", "icd10cm_desc", "immunizations", "insurance", "insurancedetail", "interactionnotes", "items", "labdata", "lablist", "ndclookupenteries", "obf_pastpregnancy", "obf_pregnancy", "oldrxmain", "problemlist", "properties", "race_codes", "referral", "rx_medication_alert", "structdatadetail", "structdemographics", "structhpi", "structobhistory", "structsocialhistory", "structured_data", "telenc", "users", "visitcodes", "vitals", "vitalshistory"]
    # all_tables = ['doctors', 'inpatientvisit', 'familyhxdetails', 'allergies', 'oldrxmain_addlinfo', 'family', 'patients', 'annualnotes', 'hpi', 'ptinstruction', 'referraldetail', 'review', 'procedurespl', 'hl7labdatadetail', 'treatmentnotes', 'surgicalhistory', 'encounterdata', 'hl7labnotes', 'social', 'notes', 'oldrxdetail']
    # all_tables = ['oldrxmain', 'labdata']
    all_tables = ['labdata']
    all_tables    = sorted({
        row[0].lower() for row in tables_statements
        if row[0].lower() not in new_tables_set
    })
    combined_stats = _empty_stats()
    all_failed_cases = []

    logger.info(
        "Dispatching %d tables across %d parallel workers",
        len(all_tables), max_workers,
    )

    retry_counts: dict = defaultdict(int)
    pending_tables = list(all_tables)

    while pending_tables:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    process_table,
                    tname,
                    cdc_table,
                    staging_schema,
                    cdc_engine,
                    staging_engine,
                    prod_engine,
                    table_columns,
                    generated_cols,
                    new_tables_set,
                    auto_increment_by_table,
                    dump_metadata,
                ): tname
                for tname in pending_tables
            }

            next_round = []
            max_retry_wait = 0
            for future in as_completed(futures):
                tname = futures[future]
                try:
                    table_stats, table_failed = future.result()
                    for k, v in table_stats.items():
                        combined_stats[k] += v
                    all_failed_cases.extend(table_failed)
                except Exception as e:
                    if _is_connection_drop(e) and retry_counts[tname] < _MAX_TABLE_RETRIES:
                        retry_counts[tname] += 1
                        wait = 2 ** retry_counts[tname]
                        max_retry_wait = max(max_retry_wait, wait)
                        orig_errno = getattr(getattr(e, "orig", None), "args", (None,))[0]
                        logger.warning(
                            "[%s] DB connection dropped (errno=%s), will retry (%d/%d)",
                            tname, orig_errno, retry_counts[tname], _MAX_TABLE_RETRIES,
                        )
                        next_round.append(tname)
                    else:
                        logger.error("[%s] Worker raised an unexpected exception: %s", tname, e, exc_info=True)

        if next_round and max_retry_wait > 0:
            logger.info("Sleeping %ds before retry round for %d table(s): %s", max_retry_wait, len(next_round), next_round)
            time.sleep(max_retry_wait)

        pending_tables = next_round

    end_time = datetime.now()
    runtime  = (end_time - start_time).total_seconds()

    logger.info("CDC sync complete")
    logger.info("Final Stats: %s", combined_stats)
    logger.info("Total run time: %.2f seconds", runtime)

    output_path = os.path.join(output_dir, f"failed_cases_{run_date}.csv")
    pd.DataFrame(all_failed_cases).to_csv(output_path, index=False)
    logger.info("Failed cases written to %s", output_path)


def main():
    args = parse_args()
    logger.info(
        "CDC restore | run_date=%s | staging=%s | prod=%s | max_workers=%d",
        args.run_date, args.staging_schema, args.prod_schema, args.max_workers,
    )
    run_restore(
        args.run_date,
        args.table_name,
        args.staging_schema,
        args.prod_schema,
        args.output_dir,
        args.max_workers,
    )


if __name__ == "__main__":
    main()
