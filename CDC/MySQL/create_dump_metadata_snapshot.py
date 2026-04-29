"""
create_dump_metadata_snapshot.py
---------------------------------
After a successful CDC run, creates cdc.dump_metadata_{mmddyyyy} for the
given run_date.

For each table:
  - Has events in change_log_{mmddyyyy}  → record MAX(binlog_file, binlog_pos)
  - No events today                      → carry position forward from
                                           dump_metadata_{yesterday} or, if
                                           that doesn't exist yet, from the
                                           original cdc.dump_metadata

The resulting table is consumed by cdc_restore.py on the NEXT run as the
lower-bound filter instead of the original dump_metadata, so each day starts
exactly where the previous day ended.

Usage:
    python create_dump_metadata_snapshot.py \\
        --run_date    "2026-04-29" \\
        --schema_name "mobiledoc"
"""

import os
import logging
import argparse
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================
# DDL
# ============================
_CREATE_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS `{table}` (
    `id`           INT          NOT NULL AUTO_INCREMENT,
    `schema_name`  VARCHAR(100) NOT NULL,
    `table_name`   VARCHAR(255) NOT NULL,
    `binlog_file`  VARCHAR(255) NOT NULL,
    `binlog_pos`   BIGINT       NOT NULL,
    `event_count`  BIGINT       DEFAULT NULL,
    `source`       VARCHAR(50)  NOT NULL DEFAULT 'change_log'
                                COMMENT 'change_log | carried_forward',
    `created_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE  KEY `uk_schema_table` (`schema_name`, `table_name`),
    KEY           `idx_binlog`   (`binlog_file`, `binlog_pos`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ============================
# DB helpers
# ============================
def _db_url(schema: str) -> str:
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASS", "")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"


def _table_exists(conn, table_name: str) -> bool:
    return bool(conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t"
        ),
        {"t": table_name},
    ).scalar())


def _to_mmddyyyy(date_str: str) -> str:
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%m%d%Y")


# ============================
# Core
# ============================
def build_snapshot(cdc_engine, schema_name: str, run_date: str) -> None:
    """
    Create and populate dump_metadata_{run_date} inside the cdc schema.
    """
    yesterday_str = (
        datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    today_fmt     = _to_mmddyyyy(run_date)
    yesterday_fmt = _to_mmddyyyy(yesterday_str)

    snapshot_table = f"dump_metadata_{today_fmt}"
    change_log     = f"change_log_{today_fmt}"
    prev_snapshot  = f"dump_metadata_{yesterday_fmt}"
    fallback       = "dump_metadata"          # original initial-load table

    with cdc_engine.begin() as conn:

        # ── 1. Create snapshot table ───────────────────────────────────────
        conn.execute(text(_CREATE_SNAPSHOT_DDL.format(table=snapshot_table)))
        logger.info("Snapshot table ready: %s", snapshot_table)

        # ── 2. MAX binlog per table from today's change_log ───────────────
        if not _table_exists(conn, change_log):
            logger.warning(
                "change_log table %s not found — snapshot will contain "
                "only carried-forward positions", change_log,
            )
            today_rows = []
        else:
            today_rows = conn.execute(text(f"""
                SELECT table_name, binlog_file, binlog_pos
                FROM (
                    SELECT
                        table_name,
                        binlog_file,
                        binlog_pos,
                        ROW_NUMBER() OVER (
                            PARTITION BY table_name
                            ORDER BY binlog_file DESC, binlog_pos DESC
                        ) AS rn
                    FROM `{change_log}`
                    WHERE binlog_file IS NOT NULL
                ) ranked
                WHERE rn = 1
            """)).fetchall()

        today_tables = {r[0].lower() for r in today_rows}
        logger.info(
            "%s: found max binlog for %d tables", change_log, len(today_rows)
        )

        # ── 3. Event count per table (for the event_count column) ─────────
        counts: dict = {}
        if today_rows and _table_exists(conn, change_log):
            counts = dict(conn.execute(text(f"""
                SELECT table_name, COUNT(*)
                FROM `{change_log}`
                WHERE binlog_file IS NOT NULL
                GROUP BY table_name
            """)).fetchall())

        # ── 4. Choose carry-forward source ────────────────────────────────
        if _table_exists(conn, prev_snapshot):
            carry_source = prev_snapshot
            logger.info("Carry-forward source: %s", prev_snapshot)
        else:
            carry_source = fallback
            logger.info(
                "No previous snapshot (%s) — falling back to %s",
                prev_snapshot, fallback,
            )

        # ── 5. Load carry-forward rows (tables absent from today's log) ───
        carry_rows = conn.execute(text(f"""
            SELECT table_name, binlog_file, binlog_pos
            FROM `{carry_source}`
            WHERE schema_name  = :schema
              AND binlog_file   IS NOT NULL
              AND binlog_pos    IS NOT NULL
        """), {"schema": schema_name}).fetchall()

        carry_rows = [r for r in carry_rows if r[0].lower() not in today_tables]
        logger.info(
            "Carry-forward: %d tables with no events today", len(carry_rows)
        )

        # ── 6. Upsert today's max positions ───────────────────────────────
        for tname, bf, bp in today_rows:
            cnt = counts.get(tname, counts.get(tname.lower(), 0))
            conn.execute(text(f"""
                INSERT INTO `{snapshot_table}`
                    (schema_name, table_name, binlog_file, binlog_pos,
                     event_count, source)
                VALUES
                    (:schema, :tname, :bf, :bp, :cnt, 'change_log')
                ON DUPLICATE KEY UPDATE
                    binlog_file  = VALUES(binlog_file),
                    binlog_pos   = VALUES(binlog_pos),
                    event_count  = VALUES(event_count),
                    source       = 'change_log'
            """), {
                "schema": schema_name,
                "tname":  tname,
                "bf":     bf,
                "bp":     int(bp),
                "cnt":    cnt,
            })

        # ── 7. Upsert carried-forward positions ───────────────────────────
        for tname, bf, bp in carry_rows:
            conn.execute(text(f"""
                INSERT INTO `{snapshot_table}`
                    (schema_name, table_name, binlog_file, binlog_pos, source)
                VALUES
                    (:schema, :tname, :bf, :bp, 'carried_forward')
                ON DUPLICATE KEY UPDATE
                    binlog_file = VALUES(binlog_file),
                    binlog_pos  = VALUES(binlog_pos),
                    source      = 'carried_forward'
            """), {
                "schema": schema_name,
                "tname":  tname,
                "bf":     bf,
                "bp":     int(bp),
            })

    logger.info(
        "Snapshot complete: %s — %d today + %d carried forward",
        snapshot_table, len(today_rows), len(carry_rows),
    )


# ============================
# CLI
# ============================
def parse_args():
    parser = argparse.ArgumentParser(
        description="Create daily binlog checkpoint from today's CDC change_log"
    )
    parser.add_argument(
        "--run_date",
        required=True,
        help="Run date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--schema_name",
        required=True,
        help="Source MySQL schema name (e.g. mobiledoc)",
    )
    return parser.parse_args()


def main():
    args   = parse_args()
    engine = create_engine(_db_url("cdc"), pool_size=2, max_overflow=2)
    build_snapshot(engine, args.schema_name, args.run_date)


if __name__ == "__main__":
    main()
