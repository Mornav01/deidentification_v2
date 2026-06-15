"""
parse_dump_metadata.py
----------------------
Scans per-table mysqldump .sql files and extracts three pieces of metadata
from each file without reading the entire file:

  1. binlog_file / binlog_pos  – from the CHANGE MASTER TO / CHANGE REPLICATION
                                  SOURCE TO comment in the file header.
  2. dump_started_at           – derived from the file's birth/creation timestamp
                                  (st_birthtime on macOS; st_mtime fallback on Linux).
  3. dump_completed_at         – from the "-- Dump completed on …" footer line.

Results are upserted into cdc.dump_metadata.

That metadata is consumed by cdc_restore.py to skip CDC events that occurred
before the dump snapshot for each table, preventing orphan rows and
AUTO_INCREMENT mismatches.

Usage:
    python parse_dump_metadata.py \
        --dump_folder "/Volumes/NDAIVol/MySQL Dump/mobiledoc" \
        --schema_name "mobiledoc"
"""

import os
import re
import logging
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text

# ============================
# Logging
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================
# DDL
# ============================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `dump_metadata` (
    `id`               INT           NOT NULL AUTO_INCREMENT,
    `schema_name`      VARCHAR(100)  NOT NULL,
    `table_name`       VARCHAR(255)  NOT NULL,
    `dump_file`        VARCHAR(1000) NOT NULL,
    `binlog_file`      VARCHAR(255)  DEFAULT NULL  COMMENT 'Value from MASTER_LOG_FILE / SOURCE_LOG_FILE',
    `binlog_pos`       BIGINT        DEFAULT NULL  COMMENT 'Value from MASTER_LOG_POS  / SOURCE_LOG_POS',
    `dump_started_at`  DATETIME      DEFAULT NULL  COMMENT 'File birth/creation timestamp (proxy for dump start time)',
    `dump_completed_at` DATETIME     DEFAULT NULL  COMMENT 'Timestamp from "Dump completed on" footer line',
    `parsed_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE  KEY `uk_schema_table`  (`schema_name`, `table_name`),
    KEY           `idx_binlog`     (`binlog_file`, `binlog_pos`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Migration SQL: safely adds new columns to tables created by older versions of this script.
# ALTER TABLE ... ADD COLUMN IF NOT EXISTS requires MySQL 8.0.3+.
# RENAME COLUMN requires MySQL 8.0.4+.
_MIGRATE_SQL = [
    # Rename legacy dump_ts → dump_completed_at (no-op if already renamed / table is fresh)
    "ALTER TABLE `dump_metadata` RENAME COLUMN `dump_ts` TO `dump_completed_at`",
    # Add dump_started_at if it doesn't exist yet
    (
        "ALTER TABLE `dump_metadata` "
        "ADD COLUMN IF NOT EXISTS `dump_started_at` DATETIME DEFAULT NULL "
        "COMMENT 'File birth/creation timestamp (proxy for dump start time)' "
        "AFTER `binlog_pos`"
    ),
    # Add dump_completed_at if it doesn't exist yet (handles fresh tables where RENAME wasn't needed)
    (
        "ALTER TABLE `dump_metadata` "
        "ADD COLUMN IF NOT EXISTS `dump_completed_at` DATETIME DEFAULT NULL "
        "COMMENT 'Timestamp from \"Dump completed on\" footer line' "
        "AFTER `dump_started_at`"
    ),
]

# ============================
# Regex patterns
# ============================

# Matches both old (MASTER) and new (REPLICATION SOURCE) syntax:
#   -- CHANGE MASTER TO MASTER_LOG_FILE='binarylogs.008904', MASTER_LOG_POS=1008690228;
#   -- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binarylogs.008904', SOURCE_LOG_POS=1008690228;
_RE_BINLOG = re.compile(
    r"CHANGE\s+(?:MASTER|REPLICATION\s+SOURCE)\s+TO\s+"
    r"(?:MASTER_LOG_FILE|SOURCE_LOG_FILE)\s*=\s*'([^']+)'\s*,\s*"
    r"(?:MASTER_LOG_POS|SOURCE_LOG_POS)\s*=\s*(\d+)",
    re.IGNORECASE,
)

# Matches: -- Dump completed on 2026-04-11  6:43:19
# The gap between date and time may be one OR two spaces (mysqldump quirk when
# the hour is a single digit), and the hour itself may be 1 or 2 digits.
_RE_DUMP_TS = re.compile(
    r"Dump completed on (\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}:\d{2})",
    re.IGNORECASE,
)

# How many lines from the TOP of the file to scan for the CHANGE MASTER TO line.
# It always appears within the first ~20 lines of a mysqldump file.
_MAX_HEADER_LINES = 60

# How many bytes from the END of the file to read for the "Dump completed on" line.
# The footer is tiny (~100 bytes); 512 bytes is a safe ceiling for any encoding.
_TAIL_BYTES = 512


# ============================
# Per-file extractor
# ============================
def extract_dump_info(sql_file_path: str) -> dict:
    """
    Extract binlog position, dump start time, and dump completion time from
    a mysqldump .sql file without reading the full file.

    Strategy:
      - FILE STAT:             dump_started_at  — file birth/creation time
                               (st_birthtime on macOS/BSD; falls back to
                                st_mtime on Linux where birth time is unavailable)
      - HEAD scan (first _MAX_HEADER_LINES lines): find CHANGE MASTER TO
      - TAIL seek (last _TAIL_BYTES bytes):         find "Dump completed on"

    A per-table dump file can be many gigabytes; reading the whole file
    for each of 8,000 tables would take hours.  Stat + head + tail keeps each
    call to a few milliseconds regardless of file size.
    """
    binlog_file      = None
    binlog_pos       = None
    dump_started_at  = None
    dump_completed_at = None

    try:
        # ── FILE STAT: dump start time ────────────────────────────────────────
        stat = os.stat(sql_file_path)
        # st_birthtime is available on macOS/BSD (true creation time).
        # Linux only exposes st_mtime (last-modified), used as fallback.
        birth_ts = getattr(stat, "st_birthtime", None) or stat.st_mtime
        dump_started_at = datetime.fromtimestamp(birth_ts)

        # ── HEAD: binlog position ────────────────────────────────────────────
        with open(sql_file_path, "r", encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                if i >= _MAX_HEADER_LINES:
                    break
                m = _RE_BINLOG.search(line)
                if m:
                    binlog_file = m.group(1)
                    binlog_pos  = int(m.group(2))
                    break   # found — stop immediately, don't read further

        # ── TAIL: dump completion timestamp ──────────────────────────────────
        file_size = os.path.getsize(sql_file_path)
        tail_size = min(_TAIL_BYTES, file_size)

        with open(sql_file_path, "rb") as fh:
            fh.seek(-tail_size, 2)          # seek from end of file
            tail = fh.read().decode("utf-8", errors="replace")

        ts_m = _RE_DUMP_TS.search(tail)
        if ts_m:
            try:
                # Collapse any run of whitespace between date and time to a
                # single space, then parse.  mysqldump zero-pads the hour on
                # some builds (17:10:22) but not others (6:43:19).
                ts_str = re.sub(r"\s+", " ", ts_m.group(1).strip())
                dump_completed_at = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logger.debug(
                    "Could not parse dump completion timestamp '%s' in %s; leaving dump_completed_at as None",
                    ts_m.group(1),
                    sql_file_path,
                )

    except OSError as e:
        logger.warning("Cannot read %s: %s", sql_file_path, e)

    return {
        "binlog_file":       binlog_file,
        "binlog_pos":        binlog_pos,
        "dump_started_at":   dump_started_at,
        "dump_completed_at": dump_completed_at,
    }


# ============================
# DB helpers
# ============================
def _db_url(schema: str) -> str:
    user     = os.environ.get("DB_USER", "ndadmin")
    password = os.environ.get("DB_PASS", "ndADMIN%402025")
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))

    # Run schema migrations in case the table was created by an older version
    # of this script (e.g. had dump_ts instead of dump_started_at / dump_completed_at).
    # Each statement is attempted independently; errors are silently swallowed
    # because the most common cause is "column already exists / doesn't exist".
    with engine.begin() as conn:
        for stmt in _MIGRATE_SQL:
            try:
                conn.execute(text(stmt))
            except Exception:
                pass  # column already exists, already renamed, etc.

    logger.info("cdc.dump_metadata table ready")


# ============================
# Core logic
# ============================
def parse_and_store(dump_folder: str, schema_name: str, engine, workers: int = 10) -> None:
    """
    1. Discover all .sql files in dump_folder.
    2. Extract binlog info from each file header in parallel.
    3. Upsert into cdc.dump_metadata.
    """
    sql_files = [
        os.path.join(dump_folder, f)
        for f in os.listdir(dump_folder)
        if f.endswith(".sql")
    ]

    if not sql_files:
        logger.warning("No .sql files found in %s", dump_folder)
        return

    logger.info("Scanning %d dump files with %d workers ...", len(sql_files), workers)

    # --- parallel extraction ---
    records = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_file = {
            executor.submit(extract_dump_info, f): f
            for f in sql_files
        }
        for future in as_completed(future_to_file):
            sql_path   = future_to_file[future]
            table_name = os.path.splitext(os.path.basename(sql_path))[0]
            info       = future.result()
            records.append({
                "schema_name":       schema_name,
                "table_name":        table_name,
                "dump_file":         sql_path,
                "binlog_file":       info["binlog_file"],
                "binlog_pos":        info["binlog_pos"],
                "dump_started_at":   info["dump_started_at"],
                "dump_completed_at": info["dump_completed_at"],
                "parsed_at":         datetime.now(),
            })

    # --- stats ---
    with_pos       = sum(1 for r in records if r["binlog_file"]       is not None)
    with_started   = sum(1 for r in records if r["dump_started_at"]   is not None)
    with_completed = sum(1 for r in records if r["dump_completed_at"] is not None)
    without        = len(records) - with_pos
    logger.info(
        "Extraction complete: %d files | %d with binlog pos | "
        "%d with dump_started_at | %d with dump_completed_at | %d missing binlog",
        len(records), with_pos, with_started, with_completed, without,
    )

    if without > 0:
        logger.warning(
            "%d dump files have no CHANGE MASTER TO line. "
            "Those tables will not benefit from position-based CDC filtering. "
            "Ensure mysqldump was run with --master-data=2 (or --source-data=2).",
            without,
        )

    # --- upsert ---
    upsert_sql = text("""
        INSERT INTO dump_metadata
            (schema_name, table_name, dump_file,
             binlog_file, binlog_pos,
             dump_started_at, dump_completed_at,
             parsed_at)
        VALUES
            (:schema_name, :table_name, :dump_file,
             :binlog_file, :binlog_pos,
             :dump_started_at, :dump_completed_at,
             :parsed_at)
        ON DUPLICATE KEY UPDATE
            dump_file         = VALUES(dump_file),
            binlog_file       = VALUES(binlog_file),
            binlog_pos        = VALUES(binlog_pos),
            dump_started_at   = VALUES(dump_started_at),
            dump_completed_at = VALUES(dump_completed_at),
            parsed_at         = VALUES(parsed_at)
    """)

    CHUNK = 500
    with engine.begin() as conn:
        for i in range(0, len(records), CHUNK):
            conn.execute(upsert_sql, records[i : i + CHUNK])

    logger.info("Upserted %d rows into cdc.dump_metadata", len(records))


def print_summary(engine, schema_name: str) -> None:
    """Log a quick summary of what was stored."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COUNT(*)                                        AS total_tables,
                SUM(binlog_file IS NOT NULL)                    AS with_binlog_pos,
                COUNT(DISTINCT binlog_file)                     AS distinct_binlog_files,
                MIN(binlog_pos)                                 AS earliest_pos,
                MAX(binlog_pos)                                 AS latest_pos,
                MIN(dump_started_at)                            AS earliest_started,
                MAX(dump_started_at)                            AS latest_started,
                MIN(dump_completed_at)                          AS earliest_completed,
                MAX(dump_completed_at)                          AS latest_completed
            FROM dump_metadata
            WHERE schema_name = :s
        """), {"s": schema_name}).fetchone()

    logger.info("── dump_metadata summary for schema '%s' ──", schema_name)
    logger.info("  Total tables          : %s", row[0])
    logger.info("  With binlog position  : %s", row[1])
    logger.info("  Distinct binlog files : %s", row[2])
    logger.info("  Binlog pos range      : %s  →  %s", row[3], row[4])
    logger.info("  dump_started_at range : %s  →  %s", row[5], row[6])
    logger.info("  dump_completed_at range: %s  →  %s", row[7], row[8])


# ============================
# CLI
# ============================
def parse_args():
    parser = argparse.ArgumentParser(
        description="Parse per-table mysqldump files and store binlog metadata in cdc.dump_metadata"
    )
    parser.add_argument(
        "--dump_folder",
        required=True,
        help='Folder containing per-table .sql dump files (e.g. "/Volumes/NDAIVol/MySQL Dump/mobiledoc")',
    )
    parser.add_argument(
        "--schema_name",
        default="mobiledoc",
        help='Source schema name stored in metadata (default: mobiledoc)',
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel file-scanning workers (default: 10)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info(
        "parse_dump_metadata | folder=%s | schema=%s | workers=%d",
        args.dump_folder, args.schema_name, args.workers,
    )

    engine = create_engine(_db_url("cdc"), pool_size=5, max_overflow=2)

    ensure_table(engine)
    parse_and_store(args.dump_folder, args.schema_name, engine, workers=args.workers)
    print_summary(engine, args.schema_name)

    engine.dispose()
    logger.info("Done")


if __name__ == "__main__":
    main()
