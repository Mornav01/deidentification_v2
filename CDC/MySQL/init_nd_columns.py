#!/usr/bin/env python
"""
init_nd_columns.py
------------------
One-time initialisation: adds three audit columns to every BASE TABLE in the
prod schema, then populates them.

  nd_auto_increment_id  BIGINT       – sequential per-table row number
                                       (same @row_num technique as
                                        add_nd_auto_increment_id.py)
  nd_extracted_date     DATETIME     – fixed baseline date (--extracted_date)
  nd_ActiveFlag         VARCHAR(10)  – 'Y' for all existing rows

Columns that already exist are skipped individually, so the script is safe
to re-run if it was interrupted.

Performance notes (critical for large table counts):
  1. Column existence is pre-fetched for ALL tables in a single
     INFORMATION_SCHEMA query — no per-table round-trip.
  2. All missing columns are added in ONE ALTER TABLE per table.
     ALGORITHM=INSTANT (MySQL 8.0.12+) is tried first; falls back to
     ALGORITHM=INPLACE, then COPY if the engine doesn't support it.
     INSTANT means zero table rebuild — purely a metadata change.
  3. nd_extracted_date and nd_ActiveFlag are declared with DEFAULT values
     in the ALTER TABLE, so MySQL returns the correct value for existing rows
     without any UPDATE.  Only nd_auto_increment_id requires a single UPDATE
     (sequential IDs cannot be expressed as a constant DEFAULT).
  Net result per table: 1 DDL (instant) + 0–1 UPDATE + 0–1 ADD INDEX.

Usage:
    python init_nd_columns.py \\
        --prod_schema   "mobiledoc" \\
        --extracted_date "2026-04-11"
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("init_nd_columns.log", mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

_AUDIT_COLS = ("nd_auto_increment_id", "nd_extracted_date", "nd_ActiveFlag")


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

    Tables with none of the three audit columns are absent from the dict
    (callers treat a missing key as an empty set).
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
        "Column prefetch complete — %d tables have all 3 audit cols already, "
        "%d need work",
        already_done, len(tables) - already_done,
    )
    return result


# ============================
# Per-table worker
# ============================
def process_table(
    engine,
    table_name: str,
    extracted_date: str,
    existing_cols: set,
) -> dict:
    """
    Add and populate audit columns that are missing.

    Strategy:
      • All missing columns → single ALTER TABLE (ALGORITHM=INSTANT preferred)
      • nd_extracted_date / nd_ActiveFlag → declared with DEFAULT in the ALTER,
        so existing rows get the right value without an UPDATE
      • nd_auto_increment_id → one UPDATE with @row_num after the ALTER
      • Index on nd_auto_increment_id → separate ADD INDEX (ALGORITHM=INPLACE)
    """
    need_inc  = "nd_auto_increment_id" not in existing_cols
    need_date = "nd_extracted_date"    not in existing_cols
    need_flag = "nd_ActiveFlag"        not in existing_cols

    if not need_inc and not need_date and not need_flag:
        logger.info("[%s] All audit columns present — skipping", table_name)
        return {"success": True, "action": "skipped"}

    try:
        with engine.begin() as conn:
            conn.execute(text("SET sql_log_bin      = 0;"))
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(text("SET sql_safe_updates = 0;"))

            # ── 1. Build ADD COLUMN clauses ───────────────────────────────
            add_clauses = []
            if need_inc:
                # No DEFAULT: will be populated by UPDATE below
                add_clauses.append(
                    "ADD COLUMN `nd_auto_increment_id` BIGINT NULL"
                )
            if need_date:
                # DEFAULT → existing rows immediately return this value;
                # no UPDATE required for this column
                add_clauses.append(
                    f"ADD COLUMN `nd_extracted_date` DATETIME "
                    f"NULL DEFAULT '{extracted_date} 00:00:00'"
                )
            if need_flag:
                add_clauses.append(
                    "ADD COLUMN `nd_ActiveFlag` VARCHAR(10) "
                    "NULL DEFAULT 'Y'"
                )

            # ── 2. Single ALTER TABLE (INSTANT → INPLACE → default) ──────
            alter_base = (
                f"ALTER TABLE `{table_name}` "
                + ", ".join(add_clauses)
            )
            for algo_hint in ("ALGORITHM=INSTANT", "ALGORITHM=INPLACE, LOCK=NONE", ""):
                try:
                    suffix = f", {algo_hint}" if algo_hint else ""
                    conn.execute(text(f"{alter_base}{suffix}"))
                    if algo_hint:
                        logger.info("[%s] ALTER TABLE (%s)", table_name, algo_hint)
                    else:
                        logger.info("[%s] ALTER TABLE (default algorithm)", table_name)
                    break
                except Exception:
                    if not algo_hint:
                        raise   # all three attempts failed

            # ── 3. Populate nd_auto_increment_id (single UPDATE) ─────────
            if need_inc:
                conn.execute(text("SET @row_num = 0;"))
                conn.execute(text(
                    f"UPDATE `{table_name}` "
                    f"SET `nd_auto_increment_id` = (@row_num := @row_num + 1)"
                ))

            # ── 4. Add index for nd_auto_increment_id ─────────────────────
            if need_inc:
                try:
                    conn.execute(text(
                        f"ALTER TABLE `{table_name}` "
                        f"ADD INDEX `idx_nd_auto_increment_id` "
                        f"(`nd_auto_increment_id`), ALGORITHM=INPLACE"
                    ))
                except Exception:
                    conn.execute(text(
                        f"ALTER TABLE `{table_name}` "
                        f"ADD INDEX `idx_nd_auto_increment_id` "
                        f"(`nd_auto_increment_id`)"
                    ))

            conn.execute(text("SET sql_log_bin      = 1;"))
            conn.execute(text("SET sql_safe_updates = 1;"))

        logger.info("[%s] Done", table_name)
        return {"success": True, "action": "added"}

    except Exception as e:
        logger.error("[%s] Error: %s", table_name, e)
        try:
            with engine.connect() as conn:
                conn.execute(text("SET sql_log_bin = 1;"))
        except Exception:
            pass
        return {"success": False, "action": "error"}


# ============================
# Table discovery
# ============================
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


# ============================
# Core
# ============================
def run(prod_schema: str, extracted_date: str, max_workers: int = 10) -> None:
    engine = create_engine(
        _db_url(prod_schema),
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=max_workers + 2,
        max_overflow=max_workers,
    )

    tables = get_all_tables(engine, prod_schema)
    if not tables:
        logger.warning("No tables found in schema '%s' — nothing to do", prod_schema)
        return

    # Single round-trip to learn which audit columns already exist
    existing_map = prefetch_existing_cols(engine, prod_schema, tables)

    logger.info(
        "Processing %d tables | workers=%d | extracted_date=%s",
        len(tables), max_workers, extracted_date,
    )

    def _dispatch(table_list: list, workers: int) -> list:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    process_table,
                    engine,
                    t,
                    extracted_date,
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
        description="Add nd_auto_increment_id, nd_extracted_date, nd_ActiveFlag to all tables"
    )
    parser.add_argument(
        "--prod_schema",
        required=True,
        help="Schema to initialise (e.g. 'mobiledoc')",
    )
    parser.add_argument(
        "--extracted_date",
        default="2026-04-11",
        help="Date value for nd_extracted_date YYYY-MM-DD (default: 2026-04-11)",
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
    try:
        datetime.strptime(args.extracted_date, "%Y-%m-%d")
    except ValueError:
        logger.error("--extracted_date must be YYYY-MM-DD, got: %s", args.extracted_date)
        sys.exit(1)

    logger.info(
        "init_nd_columns | schema=%s | extracted_date=%s | workers=%d",
        args.prod_schema, args.extracted_date, args.max_workers,
    )
    run(args.prod_schema, args.extracted_date, args.max_workers)


if __name__ == "__main__":
    main()
