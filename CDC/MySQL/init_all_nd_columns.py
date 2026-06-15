#!/usr/bin/env python
"""
init_all_nd_columns.py
-----------------------
One-time initialisation: adds four audit columns to every BASE TABLE in the
prod schema, then populates them.

  nd_auto_increment_id  BIGINT       – sequential per-table row number
  nd_extracted_date     DATETIME     – fixed baseline date (--extracted_date)
  nd_updated_at         DATETIME     – fixed update date   (--updated_at)
  nd_ActiveFlag         VARCHAR(10)  – 'Y' for all existing rows

Each column is checked individually — already-existing columns are skipped,
so the script is safe to re-run if it was interrupted.

Performance notes:
  1. Column existence is pre-fetched for ALL tables in a single
     INFORMATION_SCHEMA query — no per-table round-trip.
  2. All missing columns are added in ONE ALTER TABLE per table.
     ALGORITHM=INSTANT (MySQL 8.0.12+) is tried first; falls back to
     ALGORITHM=INPLACE, then COPY.
  3. nd_extracted_date, nd_updated_at, and nd_ActiveFlag are declared with
     DEFAULT values — no UPDATE required for those columns.
     nd_auto_increment_id requires a single UPDATE (sequential IDs cannot
     be expressed as a constant DEFAULT), followed by an ADD INDEX.

Usage:
    python init_all_nd_columns.py --prod_schema "dent" --extracted_date "2026-04-11" --updated_at "2026-04-17"
"""

import os
import sys
import argparse
import logging
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("init_all_nd_columns.log", mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

_AUDIT_COLS = (
    "nd_auto_increment_id",
    "nd_extracted_date",
    "nd_updated_at",
    "nd_ActiveFlag",
)


# ============================
# DB helpers
# ============================
def _db_url(schema: str) -> str:
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASS", "")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"


# ============================
# Batch column inspection
# ============================
def prefetch_existing_cols(engine, schema: str, tables: list) -> dict:
    """
    One INFORMATION_SCHEMA query for all tables → dict:
        { table_name_lower: set_of_existing_audit_col_names }
    """
    cols_in = ", ".join(f"'{c}'" for c in _AUDIT_COLS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM   INFORMATION_SCHEMA.COLUMNS
                WHERE  TABLE_SCHEMA = :schema
                  AND  COLUMN_NAME  IN ({cols_in})
            """),
            {"schema": schema},
        ).fetchall()

    result: dict = {}
    for tname, cname in rows:
        result.setdefault(tname.lower(), set()).add(cname)

    already_done = sum(
        1 for s in result.values() if len(s) == len(_AUDIT_COLS)
    )
    logger.info(
        "Column prefetch complete — %d tables have all 4 audit cols already, "
        "%d need work",
        already_done, len(tables) - already_done,
    )
    return result


# ============================
# ON UPDATE CURRENT_TIMESTAMP removal
# ============================
def drop_on_update_constraints(conn, schema: str, table_name: str) -> int:
    """
    For every column carrying ON UPDATE CURRENT_TIMESTAMP, redefine it as
    simply `{type} NULL` — no DEFAULT, no ON UPDATE — so subsequent UPDATEs
    (e.g. populating nd_auto_increment_id) don't silently overwrite date columns.
    Returns the number of columns modified.
    """
    rows = conn.execute(
        text("""
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM   INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = :schema
              AND  TABLE_NAME   = :table
              AND  EXTRA        LIKE '%on update%'
        """),
        {"schema": schema, "table": table_name},
    ).fetchall()

    if not rows:
        return 0

    qualified = f"`{schema}`.`{table_name}`"
    for col_name, col_type in rows:
        conn.execute(text(
            f"ALTER TABLE {qualified} MODIFY COLUMN `{col_name}` {col_type} NULL"
        ))
        logger.info(
            "[%s] Stripped DEFAULT / ON UPDATE CURRENT_TIMESTAMP from `%s` → %s NULL",
            table_name, col_name, col_type,
        )

    return len(rows)


# ============================
# Chunked NULL-fill helper
# ============================
def _chunked_update_nulls(
    conn,
    qualified: str,
    col_name: str,
    value_expr: str,
    batch_size: int = 10_000,
) -> int:
    """
    UPDATE qualified SET col = value WHERE col IS NULL LIMIT batch_size
    in a loop until no rows remain.  Each batch is its own round-trip so
    InnoDB row-locks are held only for that slice, avoiding lock-wait
    timeouts on large tables (error 1205).
    Returns total rows updated.
    """
    total = 0
    sql = text(
        f"UPDATE {qualified} SET `{col_name}` = {value_expr} "
        f"WHERE `{col_name}` IS NULL LIMIT {batch_size}"
    )
    while True:
        result = conn.execute(sql)
        total += result.rowcount
        if result.rowcount == 0:
            break
    return total


# ============================
# Per-table worker
# ============================
def process_table(
    engine,
    schema: str,
    table_name: str,
    extracted_date: str,
    updated_at: str,
    existing_cols: set,
) -> dict:
    """
    Add and populate whichever audit columns are missing.

    Strategy:
      • All missing columns → single ALTER TABLE (ALGORITHM=INSTANT preferred)
      • nd_extracted_date / nd_updated_at / nd_ActiveFlag → declared with
        DEFAULT in the ALTER, so existing rows get the right value immediately
        with no UPDATE pass needed
      • nd_auto_increment_id → one UPDATE with @row_num after the ALTER,
        then an ADD INDEX
      • Columns that already exist but have NULL rows → UPDATE to fill them
    """
    need_inc       = "nd_auto_increment_id" not in existing_cols
    need_extracted = "nd_extracted_date"    not in existing_cols
    need_updated   = "nd_updated_at"        not in existing_cols
    need_flag      = "nd_ActiveFlag"        not in existing_cols


    try:
        with engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(text("SET sql_safe_updates = 0;"))

            qualified = f"`{schema}`.`{table_name}`"

            # ── 0. Drop ON UPDATE CURRENT_TIMESTAMP so UPDATEs below don't mutate dates ──
            n = drop_on_update_constraints(conn, schema, table_name)
            if n:
                logger.info("[%s] Cleared ON UPDATE CURRENT_TIMESTAMP from %d column(s)", table_name, n)

            # ── 1. Build ADD COLUMN clauses ───────────────────────────────
            add_clauses = []
            if need_inc:
                add_clauses.append(
                    "ADD COLUMN `nd_auto_increment_id` BIGINT NULL"
                )
            if need_extracted:
                add_clauses.append(
                    f"ADD COLUMN `nd_extracted_date` DATETIME "
                    f"NULL DEFAULT '{extracted_date} 00:00:00'"
                )
            if need_updated:
                add_clauses.append(
                    f"ADD COLUMN `nd_updated_at` DATETIME "
                    f"NULL DEFAULT '{updated_at} 00:00:00'"
                )
            if need_flag:
                add_clauses.append(
                    "ADD COLUMN `nd_ActiveFlag` VARCHAR(10) "
                    "NULL DEFAULT 'Y'"
                )

            # ── 2. Single ALTER TABLE (INSTANT → INPLACE → default) ──────
            if add_clauses:
                alter_base = f"ALTER TABLE {qualified} " + ", ".join(add_clauses)
                for algo_hint in ("ALGORITHM=INSTANT", "ALGORITHM=INPLACE, LOCK=NONE", ""):
                    try:
                        suffix = f", {algo_hint}" if algo_hint else ""
                        conn.execute(text(f"{alter_base}{suffix}"))
                        label = algo_hint if algo_hint else "default algorithm"
                        logger.info("[%s] ALTER TABLE (%s)", table_name, label)
                        break
                    except Exception:
                        if not algo_hint:
                            raise

            # ── 3. Populate nd_auto_increment_id (single UPDATE) ─────────
            if need_inc:
                conn.execute(text("SET @row_num = 0;"))
                conn.execute(text(
                    f"UPDATE {qualified} "
                    f"SET `nd_auto_increment_id` = (@row_num := @row_num + 1)"
                ))

            # ── 4. Add index for nd_auto_increment_id ─────────────────────
            if need_inc:
                try:
                    conn.execute(text(
                        f"ALTER TABLE {qualified} "
                        f"ADD INDEX `idx_nd_auto_increment_id` "
                        f"(`nd_auto_increment_id`), ALGORITHM=INPLACE"
                    ))
                except Exception:
                    conn.execute(text(
                        f"ALTER TABLE {qualified} "
                        f"ADD INDEX `idx_nd_auto_increment_id` "
                        f"(`nd_auto_increment_id`)"
                    ))

            # ── 5. Fill NULLs in columns that already existed ────────────
            # (no-op for columns just added since DEFAULT already fills them)
            if not need_extracted:
                _chunked_update_nulls(conn, qualified, "nd_extracted_date", f"'{extracted_date} 00:00:00'")
                logger.info("[%s] Filled NULL nd_extracted_date", table_name)
            if not need_updated:
                _chunked_update_nulls(conn, qualified, "nd_updated_at", f"'{updated_at} 00:00:00'")
                logger.info("[%s] Filled NULL nd_updated_at", table_name)
            if not need_flag:
                _chunked_update_nulls(conn, qualified, "nd_ActiveFlag", "'Y'")
                logger.info("[%s] Filled NULL nd_ActiveFlag", table_name)

            conn.execute(text("SET sql_safe_updates = 1;"))

        logger.info("[%s] Done", table_name)
        return {"success": True, "action": "added"}

    except Exception as e:
        logger.error("[%s] Error: %s", table_name, e)
        return {"success": False, "action": "error"}


# ============================
# Table discovery
# ============================
def resolve_table_names(engine, schema: str, tables: list) -> list:
    """
    Given a list of table names (possibly wrong case), return the exact-case
    names as stored in INFORMATION_SCHEMA. Unrecognised names are logged and dropped.
    Needed when running against a case-sensitive MySQL instance (GCP default:
    lower_case_table_names=0) where `ALTER TABLE wrong_case` raises 1146.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT TABLE_NAME
                FROM   INFORMATION_SCHEMA.TABLES
                WHERE  TABLE_SCHEMA = :schema
                  AND  TABLE_TYPE   = 'BASE TABLE'
            """),
            {"schema": schema},
        ).fetchall()

    db_lookup = {r[0].lower(): r[0] for r in rows}

    resolved = []
    for t in tables:
        exact = db_lookup.get(t.lower())
        if exact is None:
            logger.warning("Table '%s' not found in schema '%s' — skipping", t, schema)
        else:
            if exact != t:
                logger.info("Case corrected: '%s' → '%s'", t, exact)
            resolved.append(exact)
    return resolved


def get_all_tables(engine, schema: str) -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT TABLE_NAME
                FROM   INFORMATION_SCHEMA.TABLES
                WHERE  TABLE_SCHEMA = :schema
                  AND  TABLE_TYPE   = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """),
            {"schema": schema},
        ).fetchall()
    tables = [r[0] for r in rows]
    logger.info("Found %d base tables in schema '%s'", len(tables), schema)
    return tables


def get_tables_from_cdc(cdc_schema: str, cdc_table: str) -> list:
    """Return distinct table names that appear in today's CDC change log."""
    engine = create_engine(_db_url(cdc_schema), pool_recycle=3600, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT DISTINCT table_name FROM `{cdc_schema}`.`{cdc_table}`")
        ).fetchall()
    tables = [r[0] for r in rows]
    logger.info(
        "Found %d distinct tables in CDC log `%s`.`%s`",
        len(tables), cdc_schema, cdc_table,
    )
    return tables


# ============================
# Core
# ============================
def run(
    prod_schema: str,
    extracted_date: str,
    updated_at: str,
    tables: list,
    max_workers: int = 10,
) -> None:
    engine = create_engine(
        _db_url(prod_schema),
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=max_workers + 2,
        max_overflow=max_workers,
    )

    if not tables:
        logger.warning("No tables provided — nothing to do")
        return

    existing_map = prefetch_existing_cols(engine, prod_schema, tables)

    logger.info(
        "Processing %d tables | workers=%d | extracted_date=%s | updated_at=%s",
        len(tables), max_workers, extracted_date, updated_at,
    )

    def _dispatch(table_list: list, workers: int) -> list:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    process_table,
                    engine,
                    prod_schema,
                    t,
                    extracted_date,
                    updated_at,
                    existing_map.get(t.lower(), set()),
                ): t
                for t in table_list
            }
            for future in as_completed(futures):
                tname = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.error("[%s] Unhandled exception: %s", tname, e)
                    result = {"success": False, "action": "error"}
                result["table"] = tname
                results.append(result)
        return results

    # First pass — parallel
    results = _dispatch(tables, max_workers)

    # Retry failed tables sequentially
    failed_tables = [r["table"] for r in results if not r["success"]]
    if failed_tables:
        logger.info("Retrying %d failed tables sequentially ...", len(failed_tables))
        retry_results = _dispatch(failed_tables, workers=1)
        retry_map = {r["table"]: r for r in retry_results}
        for i, r in enumerate(results):
            if not r["success"] and r["table"] in retry_map:
                results[i] = retry_map[r["table"]]

    added   = sum(1 for r in results if r.get("action") == "added")
    skipped = sum(1 for r in results if r.get("action") == "skipped")
    failed  = sum(1 for r in results if not r["success"])

    logger.info("=" * 60)
    logger.info(
        "Summary — total: %d | added: %d | skipped: %d | failed: %d",
        len(results), added, skipped, failed,
    )
    if failed:
        logger.error("Failed: %s", [r["table"] for r in results if not r["success"]])
    logger.info("=" * 60)


# ============================
# CLI
# ============================
def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Add nd_auto_increment_id, nd_extracted_date, "
            "nd_updated_at, nd_ActiveFlag to all tables"
        )
    )
    parser.add_argument(
        "--prod_schema",
        required=True,
        help="Schema to initialise (e.g. 'mobiledoc')",
    )
    parser.add_argument(
        "--extracted_date",
        default="2026-04-11",
        help="Value for nd_extracted_date YYYY-MM-DD (default: 2026-04-11)",
    )
    parser.add_argument(
        "--updated_at",
        required=True,
        help="Value for nd_updated_at YYYY-MM-DD (e.g. today's run_date)",
    )
    parser.add_argument(
        "--cdc_schema",
        default="cdc",
        help="CDC schema (e.g. 'cdc'). When provided with --cdc_table, tables are "
             "sourced from the change log instead of INFORMATION_SCHEMA.",
    )
    parser.add_argument(
        "--cdc_table",
        default=None,
        help="CDC change log table name (e.g. 'change_log_05052026').",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=10,
        help="Parallel workers (default: 10)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    for flag, val in [
        ("--extracted_date", args.extracted_date),
        ("--updated_at", args.updated_at),
    ]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            logger.error("%s must be YYYY-MM-DD, got: %s", flag, val)
            sys.exit(1)

    logger.info(
        "init_all_nd_columns | schema=%s | extracted_date=%s | updated_at=%s | workers=%d",
        args.prod_schema, args.extracted_date, args.updated_at, args.max_workers,
    )

    df = pd.read_csv("/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/input/deid_runner.csv", header=None, names=['table_name'])
    tables = df['table_name'].to_list()

    # Resolve to exact DB case before any DDL/DML (required for case-sensitive GCP MySQL)
    engine = create_engine(_db_url(args.prod_schema), pool_recycle=3600, pool_pre_ping=True)
    tables = resolve_table_names(engine, args.prod_schema, tables)

    run(args.prod_schema, args.extracted_date, args.updated_at, tables, args.max_workers)


if __name__ == "__main__":
    main()
