import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime
from collections import defaultdict
import json
import re
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

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
_RE_VALUES = re.compile(r"\)\s*VALUES\s*\(", re.IGNORECASE)

# All audit columns added/ensured on every CDC-touched staging table
_ALTER_COLS = [
    ("nd_auto_increment_id", "BIGINT DEFAULT NULL"),
    ("nd_extracted_at",        "DATETIME DEFAULT NULL"),
    ("nd_updated_at",        "DATETIME DEFAULT NULL"),
    ("nd_operation",         "VARCHAR(100)"),
    ("nd_is_active",         "VARCHAR(100)"),
]


# ============================
# CLI
# ============================
def parse_args():
    """
    Parse command-line arguments for CDC restore.

    Example:
        python cdc_restore.py --run_date "2025-10-12" --table_name "change_log" \
            --staging_schema "mobiledoc_staging" --prod_schema "mobiledoc_oct"
    """
    parser = argparse.ArgumentParser(description="CDC restore: apply CDC change log into staging schema")
    parser.add_argument("--run_date",       required=True, help="Run date in YYYY-MM-DD format")
    parser.add_argument("--table_name",     required=True, help='CDC change-log table name (e.g. "change_log")')
    parser.add_argument("--staging_schema", required=True, help='Target staging schema (e.g. "mobiledoc_apr26_staging")')
    parser.add_argument("--prod_schema",    required=True, help='Source prod schema (e.g. "mobiledoc_apr26")')
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
    """Build a MySQL connection URL. Set DB_USER / DB_PASS / DB_HOST / DB_PORT env vars."""
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASS", "")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"


def stream_cdc_data(engine, table_name, batch_size=10000):
    """
    Yield rows from the CDC table in ID-ordered chunks.
    Prevents memory exhaustion and long-running transaction timeouts.
    """
    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    logger.info("Starting stream: %s rows from CDC table %s", f"{total_rows:,}", table_name)

    last_bf: str = ""
    last_bp: int = 0
    last_id: int = 0
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
            text(f"SELECT COUNT(*) FROM {cdc_table} WHERE table_name = :tname {dump_filter}"),
            {"tname": target_table, **dump_params},
        ).scalar()
    logger.info("[%s] %s CDC rows to process (post-dump snapshot)", target_table, f"{total_rows:,}")

    last_bf: str = ""   # empty string sorts before any real binlog filename (e.g. 'binarylogs.*')
    last_bp: int = 0
    last_id: int = 0
    query = text(f"""
        SELECT * FROM {cdc_table}
        WHERE table_name = :tname
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


def load_dump_metadata(cdc_engine, schema_name: str) -> dict:
    """
    Read cdc.dump_metadata and return a dict:
        { table_name_lower: (binlog_file, binlog_pos) }

    Only rows where BOTH binlog_file and binlog_pos are non-NULL are included.
    Tables absent from (or with NULL positions in) dump_metadata will receive
    no pre-dump filter — all their CDC events will be replayed.
    """
    try:
        with cdc_engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT table_name, binlog_file, binlog_pos
                    FROM   dump_metadata
                    WHERE  schema_name  = :schema
                      AND  binlog_file  IS NOT NULL
                      AND  binlog_pos   IS NOT NULL
                """),
                {"schema": schema_name},
            ).fetchall()
        metadata = {r[0].lower(): (r[1], int(r[2])) for r in rows}
        logger.info(
            "Loaded dump snapshot positions for %d tables (schema=%s)",
            len(metadata), schema_name,
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


def strip_auto_increment_from_insert(columns, values, ai_column):
    """
    Remove the AUTO_INCREMENT PK column from a CDC INSERT statement so that
    MySQL auto-assigns the next sequential ID in staging.

    Used exclusively for INSERT events replayed directly from change_log —
    those statements were originally executed in prod WITHOUT an explicit PK
    value (MySQL assigned it there too).  Stripping it here ensures staging
    gets the same sequential behaviour rather than an explicit prod ID that
    could collide or create gaps.

    NOT used for prod-fetched fallback INSERTs (UPDATE path), where we
    intentionally preserve the exact prod PK.
    """
    if not ai_column or not columns or len(columns) != len(values):
        return columns, values
    for i, c in enumerate(columns):
        if c.lower() == ai_column.lower():
            cols = list(columns)
            vals = list(values)
            cols.pop(i)
            vals.pop(i)
            return cols, vals
    return columns, values


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
def _build_enriched_insert(table_name, prod_data, table_columns, generated_cols, op):
    """
    Strip generated columns from prod rows and append audit fields.
    Returns (insert_sql, enriched_rows, insert_columns).
    Called by both JOIN and non-JOIN UPDATE paths — no duplication.
    The AUTO_INCREMENT PK column is intentionally kept so the prod value is
    preserved in staging rather than re-generated.
    """
    all_cols = list(table_columns[table_name.lower()])
    # gen_cols = generated_cols.get(table_name.lower(), set())
    # drop_idx = {i for i, col in enumerate(all_cols) if col in gen_cols}

    insert_columns = [col for i, col in enumerate(all_cols)]
    add_audit = "nd_extracted_at" not in insert_columns

    if add_audit:
        insert_columns.extend(["nd_extracted_at", "nd_updated_at", "nd_operation", "nd_is_active"])
        idx_extracted = idx_updated = idx_op = None
    else:
        idx_extracted = insert_columns.index("nd_extracted_at")
        idx_updated = insert_columns.index("nd_updated_at")
        idx_op      = insert_columns.index("nd_operation")
        idx_ac      = insert_columns.index("nd_is_active")

    now = datetime.now()
    enriched_data = []

    for row_data in prod_data:
        cleaned = [v for i, v in enumerate(row_data)]

        if add_audit:
            cleaned.extend([now, now, op, 'Yes'])
        else:
            try:
                if cleaned[idx_extracted] is None:
                    cleaned[idx_extracted] = now
                    cleaned[idx_updated] = now
                    cleaned[idx_op]      = op
                    cleaned[idx_ac]      = 'Yes'
                else:
                    cleaned[idx_updated] = now
            except IndexError:
                cleaned.extend([now, now, op, 'Yes'])

        enriched_data.append(tuple(cleaned))

    placeholders = ", ".join(["%s"] * len(insert_columns))
    insert_sql = (
        f"INSERT IGNORE INTO `{table_name}` "
        f"({', '.join('`' + c + '`' for c in insert_columns)}) "
        f"VALUES ({placeholders})"
    )
    return insert_sql, enriched_data, insert_columns


def append_audit(columns, values, next_id, op):
    columns.extend(["nd_auto_increment_id", "nd_extracted_at", "nd_updated_at", "nd_operation", "nd_is_active"])
    values.extend([str(next_id), "NOW()", "NOW()", f"'{op}'", "'Yes'"])
    return columns, values


def build_final_insert(table_name, columns, values, staging_schema):
    col_str = ", ".join(f"`{c}`" for c in columns)
    val_str = ", ".join(values)
    return f"INSERT INTO {staging_schema}.`{table_name}` ({col_str}) VALUES ({val_str})"


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
        cleaned.extend([next_id, datetime.now(), datetime.now(), "INSERT_SELECT", "Yes"])
        enriched_rows.append(tuple(cleaned))
        next_id += 1

    insert_cols_cleaned = [c for c in all_cols if c not in gen_cols]
    insert_cols_cleaned.extend(["nd_auto_increment_id", "nd_extracted_at", "nd_updated_at", "nd_operation", "nd_is_active"])

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

        # ------------------------------------------------------------------
        # AUTO_INCREMENT tracker
        # Tracks the highest PK value assigned so far in staging for this
        # table.  Read once from the DB before any cursor writes (so the
        # SELECT MAX is accurate); then maintained purely in memory.
        #
        # Why not re-query during the loop?
        #   cursor writes are uncommitted when staging_conn.execute() runs
        #   (different transaction view) → SELECT MAX() returns stale data →
        #   ALTER TABLE AUTO_INCREMENT = stale+1 rewinds the counter below
        #   IDs already given out → duplicate-key errors on later INSERTs.
        #
        # Rules:
        #   INSERT event succeeds  → tracker += 1  (MySQL just used tracker+1)
        #   UPDATE fallback INSERT → ALTER TABLE AUTO_INCREMENT = tracker+1
        #                           (undo the jump from the high prod PK;
        #                            tracker itself does NOT change)
        # ------------------------------------------------------------------
        _ai_tracker: dict = {}
        _ai_col_init = auto_increment_by_table.get(table_name.lower())
        if _ai_col_init:
            # Read the table's current AUTO_INCREMENT counter directly from
            # information_schema — this IS the next value MySQL will assign,
            # no +1 arithmetic required.  Read once before any cursor writes
            # so the value is accurate (no transaction-isolation lag).
            _auto_inc = staging_conn.execute(
                text(
                    "SELECT AUTO_INCREMENT FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tname"
                ),
                {"tname": table_name},
            ).scalar() or 1
            _ai_tracker[table_name.lower()] = int(_auto_inc)
            logger.debug(
                "[%s] AI tracker init: %s AUTO_INCREMENT = %s",
                table_name, _ai_col_init, _auto_inc,
            )

        for i, row in enumerate(
            stream_cdc_data_for_table(
                cdc_engine, cdc_table, table_name,
                batch_size=10000,
                dump_binlog_file=dump_bf,
                dump_binlog_pos=dump_bp,
            ), 1
        ):
            row_table = row[1]
            op        = row[2]
            sql       = json.loads(row[3])["raw_sql"]

            try:
                if row_table not in nd_counter:
                    max_nd = prod_conn.execute(
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

                        prod_data = prod_conn.execute(text(prod_query)).fetchall()
                        if not prod_data:
                            failed_cases.append({"type": "errors_update_prod", "table_name": row_table, "operation": op, "sql": sql, "error": "no data found in prod"})
                            stats["errors_update_prod"] += 1
                            continue

                    _ai_col = auto_increment_by_table.get(row_table.lower())
                    insert_sql, enriched_data, _ = _build_enriched_insert(
                        row_table,
                        prod_data,
                        table_columns,
                        generated_cols,
                        op,
                    )
                    try:
                        cursor.executemany(insert_sql, enriched_data)
                        # Reset AUTO_INCREMENT to the in-memory tracked value so
                        # subsequent auto-assigned INSERTs get the correct sequential
                        # ID, undoing any jump caused by inserting the high prod PK.
                        # tracker is NOT incremented here — only INSERT events do that.
                        if _ai_col and row_table.lower() in _ai_tracker:
                            cursor.execute(
                                f"ALTER TABLE `{row_table}` "
                                f"AUTO_INCREMENT = {_ai_tracker[row_table.lower()]}"
                            )
                        cursor._defer_warnings = True
                        cursor.execute(sql)
                        stats["updated"] += 1
                    except Exception as e:
                        failed_cases.append({"type": "errors_update", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
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
                    # Strip the AUTO_INCREMENT PK so MySQL assigns the next
                    # sequential ID in staging — matching how the original prod
                    # INSERT worked (client didn't specify the PK; MySQL did).
                    ai_col = auto_increment_by_table.get(row_table.lower())
                    if ai_col:
                        columns, values = strip_auto_increment_from_insert(columns, values, ai_col)

                    if len(columns) != len(values):
                        stats["errors_insert_mismatch"] += 1
                        continue

                    columns, values = append_audit(columns, values, next_id, op)
                    nd_counter[row_table] = next_id

                    final_sql = build_final_insert(row_table, columns, values, staging_schema)
                    final_sql = final_sql.replace("INSERT INTO", "INSERT IGNORE INTO", 1)

                    try:
                        cursor._defer_warnings = True
                        cursor.execute(final_sql)
                        stats["inserted"] += 1
                        # MySQL just assigned tracker+1 to this row — advance tracker
                        # so the next INSERT and any UPDATE fallback reset both agree
                        # on the correct next sequential ID.
                        if row_table.lower() in _ai_tracker:
                            _ai_tracker[row_table.lower()] += 1
                    except Exception as e:
                        failed_cases.append({"type": "errors_insert", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
                        stats["errors_insert"] += 1

            except SQLAlchemyError as e:
                failed_cases.append({"type": "errors", "table_name": row_table, "operation": op, "sql": sql, "error": str(e)})
                stats["errors"] += 1

            if i % BATCH_SIZE == 0:
                staging_conn.commit()
                cursor.close()
                cursor = staging_conn.connection.cursor()
                logger.info("[%s] Batch %s: %s", table_name, f"{i:,}", stats)

        staging_conn.commit()
        cursor.close()
        staging_conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

    logger.info("[%s] Done: %s", table_name, stats)
    return stats, failed_cases


def _empty_stats():
    return {
        "inserted": 0, "updated": 0, "update_where_none": 0,
        "insert_select": 0, "errors": 0, "errors_update": 0,
        "errors_update_prod": 0, "errors_insert_none_fmt": 0,
        "errors_insert": 0, "errors_insert_select": 0,
        "errors_insert_select_select": 0, "insert_select_no_rows": 0,
        "errors_insert_select_insert": 0, "errors_insert_mismatch": 0,
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

    cdc_engine     = create_engine(_db_url("cdc"),     pool_size=max_workers + 2, max_overflow=max_workers)
    staging_engine = create_engine(_db_url(staging_schema), pool_size=max_workers + 2, max_overflow=max_workers)
    prod_engine    = create_engine(_db_url(prod_schema),    pool_size=max_workers + 2, max_overflow=max_workers)

    # Discover tables referenced in the CDC log
    with cdc_engine.connect() as conn:
        tables_statements = conn.execute(
            text(f"SELECT DISTINCT table_name FROM {cdc_table}")
        ).fetchall()
    logger.info("Total tables in CDC: %d", len(tables_statements))

    # Generated column metadata (VIRTUAL / STORED — cannot be inserted directly)
    generated_cols = defaultdict(set)
    with staging_engine.connect() as conn:
        logger.info("Loading generated column metadata...")
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
    dump_metadata = load_dump_metadata(cdc_engine, "mobiledoc")

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
                    conn.execute(text(alter_sql))
                    table_columns[tname.lower()].append(col_name)
                except Exception as e:
                    logger.warning("Skipped %s.%s: %s", tname, col_name, e)

    new_tables_set = set(new_tables)
    logger.warning("Found %d new tables in this batch: %s", len(new_tables), new_tables)

    # ------------------------------------------------------------------
    # Parallel dispatch — one worker per table, up to max_workers at once
    # ------------------------------------------------------------------
    all_tables    = [row[0] for row in tables_statements if row[0].lower() not in new_tables_set]
    combined_stats = _empty_stats()
    all_failed_cases = []

    logger.info(
        "Dispatching %d tables across %d parallel workers",
        len(all_tables), max_workers,
    )

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
                dump_metadata,          # pre-dump position filter per table
            ): tname
            for tname in all_tables
        }

        for future in as_completed(futures):
            tname = futures[future]
            try:
                table_stats, table_failed = future.result()
                for k, v in table_stats.items():
                    combined_stats[k] += v
                all_failed_cases.extend(table_failed)
            except Exception as e:
                logger.error("[%s] Worker raised an unexpected exception: %s", tname, e, exc_info=True)

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
