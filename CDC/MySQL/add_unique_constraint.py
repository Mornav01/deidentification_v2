#!/usr/bin/env python
"""
Add Unique Constraint Script

This script adds UNIQUE constraint on nd_auto_increment_id column for all tables in a schema.
If duplicates exist, it deduplicates them first (keeping the oldest record based on nd_extracted_date).

Usage:
    python add_unique_constraint.py --schema "deidentified"
"""

import os
import sys
import argparse
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("add_unique_constraint.log", mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def _db_url(schema: str) -> str:
    """Built via URL.create (same as deid/config/schema.py DbConfig.connection_string) so
    special characters in DB_PASS (e.g. @) are percent-encoded correctly."""
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASS", "")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return URL.create(
        drivername="mysql+pymysql", username=user, password=password,
        host=host, port=int(port), database=schema,
    ).render_as_string(hide_password=False)


def prefetch_constraint_status(engine, schema: str, tables: list) -> dict:
    """
    Two schema-wide INFORMATION_SCHEMA queries — no TABLE_NAME IN (...) list.
    Filtering is done in Python against the input table set.
    Returns { table_name_lower: {'has_col': bool, 'has_unique': bool} }
    """
    target = {t.lower() for t in tables}
    with engine.connect() as conn:
        col_rows = conn.execute(text("""
            SELECT TABLE_NAME
            FROM   INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = :schema
              AND  COLUMN_NAME  = 'nd_auto_increment_id'
        """), {"schema": schema}).fetchall()

        idx_rows = conn.execute(text("""
            SELECT TABLE_NAME
            FROM   INFORMATION_SCHEMA.STATISTICS
            WHERE  TABLE_SCHEMA = :schema
              AND  COLUMN_NAME  = 'nd_auto_increment_id'
              AND  NON_UNIQUE   = 0
        """), {"schema": schema}).fetchall()

    has_col    = {r[0].lower() for r in col_rows} & target
    has_unique = {r[0].lower() for r in idx_rows} & target

    result = {
        t.lower(): {"has_col": t.lower() in has_col, "has_unique": t.lower() in has_unique}
        for t in tables
    }

    already_done = sum(1 for v in result.values() if v["has_unique"])
    logger.info(
        "Constraint prefetch complete — %d tables already have UNIQUE, %d need work",
        already_done, len(tables) - already_done,
    )
    return result


def _check_column_exists(conn, schema: str, table_name: str) -> bool:
    result = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = :schema
              AND TABLE_NAME = :table
              AND COLUMN_NAME = 'nd_auto_increment_id'
            LIMIT 1
        """),
        {"schema": schema, "table": table_name}
    ).fetchone()
    return result is not None


def _check_unique_exists(conn, schema: str, table_name: str) -> bool:
    result = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = :schema
              AND TABLE_NAME = :table
              AND COLUMN_NAME = 'nd_auto_increment_id'
              AND NON_UNIQUE = 0
            LIMIT 1
        """),
        {"schema": schema, "table": table_name}
    ).fetchone()
    return result is not None


def _deduplicate(conn, schema: str, table_name: str) -> int:
    has_date = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = :schema
              AND TABLE_NAME = :table
              AND COLUMN_NAME = 'nd_extracted_date'
            LIMIT 1
        """),
        {"schema": schema, "table": table_name}
    ).fetchone()

    if not has_date:
        logger.warning("[%s] 'nd_extracted_date' not found — skipping dedup", table_name)
        return 0

    conn.execute(text("SET sql_safe_updates = 0"))
    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    result = conn.execute(text(f"""
        DELETE t
        FROM `{schema}`.`{table_name}` t
        JOIN (
            SELECT
                nd_auto_increment_id,
                nd_extracted_date,
                ROW_NUMBER() OVER (
                    PARTITION BY nd_auto_increment_id
                    ORDER BY nd_extracted_date
                ) AS rn
            FROM `{schema}`.`{table_name}`
        ) d
        ON  t.nd_auto_increment_id = d.nd_auto_increment_id
        AND t.nd_extracted_date    = d.nd_extracted_date
        WHERE d.rn > 1
    """))
    conn.execute(text("SET sql_safe_updates = 1"))

    deleted = result.rowcount
    if deleted:
        logger.info("[%s] Removed %d duplicate rows", table_name, deleted)
    else:
        logger.info("[%s] No duplicates found", table_name)
    return deleted


def process_table(engine, schema: str, table_name: str) -> dict:
    try:
        with engine.begin() as conn:
            if not _check_column_exists(conn, schema, table_name):
                logger.info("[%s] 'nd_auto_increment_id' missing — skipping", table_name)
                return {"success": True, "action": "skipped"}

            if _check_unique_exists(conn, schema, table_name):
                logger.info("[%s] UNIQUE constraint already exists", table_name)
                return {"success": True, "action": "skipped"}

            logger.info("[%s] Adding UNIQUE constraint ...", table_name)
            try:
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                conn.execute(text(f"""
                    ALTER TABLE `{schema}`.`{table_name}`
                    ADD UNIQUE INDEX uniq_nd_auto_increment_id (nd_auto_increment_id)
                """))
                logger.info("[%s] Done", table_name)
                return {"success": True, "action": "added"}

            except Exception as e:
                if "Duplicate entry" in str(e) or "duplicate" in str(e).lower():
                    logger.warning("[%s] Duplicates detected — deduplicating ...", table_name)
                    _deduplicate(conn, schema, table_name)
                    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                    conn.execute(text(f"""
                        ALTER TABLE `{schema}`.`{table_name}`
                        ADD UNIQUE INDEX uniq_nd_auto_increment_id (nd_auto_increment_id)
                    """))
                    logger.info("[%s] Done (after dedup)", table_name)
                    return {"success": True, "action": "added"}
                raise

    except Exception as e:
        logger.error("[%s] Error: %s", table_name, e)
        return {"success": False, "action": "error"}


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
    return [r[0] for r in rows]


def run(schema: str, max_workers: int = 10) -> None:
    engine = create_engine(
        _db_url(schema),
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=max_workers + 2,
        max_overflow=max_workers,
    )

    tables = get_all_tables(engine, schema)
    # df = pd.read_csv("/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/input/deid_runner.csv", header=None, names=['table_name'])
    # tables = df['table_name'].to_list()

    if not tables:
        logger.warning("No tables found — nothing to do")
        return

    status_map = prefetch_constraint_status(engine, schema, tables)

    # Skip tables that already have the UNIQUE constraint — no worker needed.
    dispatch_list = [t for t in tables if not status_map.get(t.lower(), {}).get("has_unique")]
    skip_count    = len(tables) - len(dispatch_list)

    logger.info(
        "Processing %d tables (skipped %d already-constrained) | workers=%d | schema=%s",
        len(dispatch_list), skip_count, max_workers, schema,
    )

    if not dispatch_list:
        logger.info("Nothing to do — all tables already have the UNIQUE constraint")
        return

    def _dispatch(table_list: list, workers: int) -> list:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_table, engine, schema, t): t
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
    results = _dispatch(dispatch_list, max_workers)

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
        "Summary — total: %d | added: %d | skipped: %d (incl. %d pre-skipped) | failed: %d",
        len(tables), added, skipped + skip_count, skip_count, failed,
    )
    if failed:
        logger.error("Failed: %s", [r["table"] for r in results if not r["success"]])
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Add UNIQUE constraint on nd_auto_increment_id column")
    parser.add_argument("--schema", required=True, help="Schema name (e.g., 'deidentified')")
    parser.add_argument("--max_workers", type=int, default=10, help="Parallel workers (default: 10)")
    args = parser.parse_args()

    logger.info("add_unique_constraint | schema=%s | workers=%d", args.schema, args.max_workers)
    run(args.schema, args.max_workers)


if __name__ == "__main__":
    main()
