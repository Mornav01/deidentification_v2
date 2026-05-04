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
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASS", "")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"


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

    # tables = get_all_tables(engine, schema)
    df = pd.read_csv("/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/input/deid_runner.csv", header=None, names=['table_name'])
    tables = df['table_name'].to_list()

    if not tables:
        logger.warning("No tables found — nothing to do")
        return

    logger.info("Processing %d tables | workers=%d | schema=%s", len(tables), max_workers, schema)

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


def main():
    parser = argparse.ArgumentParser(description="Add UNIQUE constraint on nd_auto_increment_id column")
    parser.add_argument("--schema", required=True, help="Schema name (e.g., 'deidentified')")
    parser.add_argument("--max_workers", type=int, default=10, help="Parallel workers (default: 10)")
    args = parser.parse_args()

    logger.info("add_unique_constraint | schema=%s | workers=%d", args.schema, args.max_workers)
    run(args.schema, args.max_workers)


if __name__ == "__main__":
    main()
