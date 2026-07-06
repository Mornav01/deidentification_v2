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


def _find_latest_snapshot(conn, run_date: str, max_lookback: int = 30) -> str | None:
    current = datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=1)
    for _ in range(max_lookback):
        candidate = f"dump_metadata_{current.strftime('%m%d%Y')}"
        if _table_exists(conn, candidate):
            return candidate
        current -= timedelta(days=1)
    return None


# ============================
# Core
# ============================
def build_snapshot(cdc_engine, schema_name: str, run_date: str) -> None:
    """
    Create and populate dump_metadata_{run_date} inside the cdc schema.
    """
    today_fmt = _to_mmddyyyy(run_date)

    snapshot_table = f"dump_metadata_{today_fmt}"
    change_log     = f"change_log_{today_fmt}"
    fallback       = "dump_metadata"          # original initial-load table

    with cdc_engine.begin() as conn:

        # Give the server enough time to aggregate large change_log tables.
        conn.execute(text("SET SESSION net_read_timeout  = 600"))
        conn.execute(text("SET SESSION net_write_timeout = 600"))

        # ── 1. Create snapshot table ───────────────────────────────────────
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        conn.execute(text(_CREATE_SNAPSHOT_DDL.format(table=snapshot_table)))
        logger.info("Snapshot table ready: %s", snapshot_table)

        # ── 2. MAX binlog per table from today's change_log ───────────────
        if not _table_exists(conn, change_log):
            logger.warning(
                "change_log table %s not found — snapshot will contain "
                "only carried-forward positions", change_log,
            )
            today_rows = []
            counts: dict = {}
        else:
            # Single-pass GROUP BY: pack (binlog_file, zero-padded binlog_pos)
            # into one string so MAX() picks the lexicographically latest combo,
            # then unpack. This scans the table once and needs no self-join.
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            raw = conn.execute(text(f"""
                SELECT
                    table_name,
                    SUBSTRING_INDEX(
                        MAX(CONCAT(binlog_file, '|', LPAD(binlog_pos, 20, '0'))),
                        '|', 1
                    )                            AS binlog_file,
                    CAST(
                        SUBSTRING_INDEX(
                            MAX(CONCAT(binlog_file, '|', LPAD(binlog_pos, 20, '0'))),
                            '|', -1
                        ) AS UNSIGNED
                    )                            AS binlog_pos,
                    COUNT(*)                     AS event_count
                FROM `{change_log}`
                WHERE binlog_file IS NOT NULL
                GROUP BY table_name
            """)).fetchall()
            today_rows = [(r[0], r[1], r[2]) for r in raw]
            counts     = {r[0]: r[3] for r in raw}

        today_tables = {r[0].lower() for r in today_rows}
        logger.info(
            "%s: found max binlog for %d tables", change_log, len(today_rows)
        )

        # ── 3. Choose carry-forward source ────────────────────────────────
        latest_snapshot = _find_latest_snapshot(conn, run_date)
        if latest_snapshot:
            carry_source = latest_snapshot
            logger.info("Carry-forward source: %s", latest_snapshot)
        else:
            carry_source = fallback
            logger.info("No prior snapshot found — falling back to %s", fallback)

        # ── 5. Load carry-forward rows (tables absent from today's log) ───
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
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
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
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
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
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
    engine = create_engine(
        _db_url("cdc"),
        pool_size=2,
        max_overflow=2,
        connect_args={"connect_timeout": 10, "read_timeout": 600, "write_timeout": 600},
    )
    build_snapshot(engine, args.schema_name, args.run_date)


if __name__ == "__main__":
    main()
