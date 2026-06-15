"""
Backfill nd_patient_id in encounter_mapping_table from patient_mapping_table.

JOIN key: encounter_mapping_table.patient_id = patient_mapping_table.patientid

Usage:
    cd deidentification_v2
    python tests/patient_general_tests/backfill_nd_patient_id.py
"""
import logging
import time
import sys

import sqlalchemy as sa
from sqlalchemy import text

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL = "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/mapping_test"
BATCH_SIZE = 50_000      # rows per id-range batch (increase if indexes are fast)
LOG_INTERVAL = 20        # seconds between progress logs
MAX_RETRIES = 3

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────
def ensure_index(conn, table: str, column: str, index_name: str) -> None:
    exists = conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.STATISTICS "
        "WHERE table_schema = DATABASE() "
        "AND table_name = :t AND index_name = :i"
    ), {"t": table, "i": index_name}).scalar()

    if exists:
        log.info("  index %-40s already exists — skip", index_name)
        return

    log.info("  creating index %-35s on %s(%s) …", index_name, table, column)
    t0 = time.monotonic()
    conn.execute(text(f"CREATE INDEX {index_name} ON {table} ({column})"))
    conn.commit()
    log.info("  index created in %.1fs", time.monotonic() - t0)


def run_batch(conn, start_id: int, end_id: int) -> int:
    result = conn.execute(text("""
        UPDATE encounter_mapping_table e
        JOIN  patient_mapping_table   p  ON e.patient_id = p.patientid
        SET   e.nd_patient_id = p.nd_patient_id
        WHERE e.id BETWEEN :s AND :e
          AND e.nd_patient_id IS NULL
    """), {"s": start_id, "e": end_id})
    conn.commit()
    return result.rowcount


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    engine = sa.create_engine(
        DB_URL,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 10,
            "read_timeout":    7200,
            "write_timeout":   7200,
        },
    )

    # ── Step 1: ensure indexes ────────────────────────────────────────────────
    log.info("=== Step 1: checking / creating indexes ===")
    with engine.connect() as conn:
        ensure_index(conn, "patient_mapping_table",   "patientid",    "idx_pmt_patientid")
        ensure_index(conn, "encounter_mapping_table", "id",           "idx_emt_id")
        ensure_index(conn, "encounter_mapping_table", "nd_patient_id","idx_emt_nd_patient_id")

    # ── Step 2: current state ─────────────────────────────────────────────────
    log.info("=== Step 2: checking current state ===")
    with engine.connect() as conn:
        total      = conn.execute(text("SELECT COUNT(*) FROM encounter_mapping_table")).scalar()
        null_count = conn.execute(text("SELECT COUNT(*) FROM encounter_mapping_table WHERE nd_patient_id IS NULL")).scalar()
        already    = total - null_count

        if null_count == 0:
            log.info("All %s rows already have nd_patient_id — nothing to do.", f"{total:,}")
            return

        min_null_id = conn.execute(text(
            "SELECT MIN(id) FROM encounter_mapping_table WHERE nd_patient_id IS NULL"
        )).scalar()
        max_id = conn.execute(text("SELECT MAX(id) FROM encounter_mapping_table")).scalar()

    total_batches = (max_id - min_null_id) // BATCH_SIZE + 1
    log.info(
        "Total rows : %s | Already done : %s | Remaining : %s (%.1f%%)",
        f"{total:,}", f"{already:,}", f"{null_count:,}", null_count / total * 100,
    )
    log.info(
        "ID range   : %s → %s | Batch size : %s | Est. batches : %s",
        f"{min_null_id:,}", f"{max_id:,}", f"{BATCH_SIZE:,}", f"{total_batches:,}",
    )

    # ── Step 3: batched UPDATE ────────────────────────────────────────────────
    log.info("=== Step 3: running batched UPDATE ===")
    rows_updated   = 0
    batch_num      = 0
    start          = time.monotonic()
    last_log_time  = start
    current_id     = min_null_id

    while current_id <= max_id:
        end_id     = current_id + BATCH_SIZE - 1
        batch_num += 1

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with engine.connect() as conn:
                    affected = run_batch(conn, current_id, end_id)
                rows_updated += affected
                break
            except Exception as exc:
                log.warning("Batch %d attempt %d/%d failed: %s", batch_num, attempt, MAX_RETRIES, exc)
                if attempt == MAX_RETRIES:
                    log.error("Batch %d (id %d–%d) failed after %d attempts — aborting.",
                              batch_num, current_id, end_id, MAX_RETRIES)
                    raise
                time.sleep(3 * attempt)

        current_id = end_id + 1

        # Progress log every LOG_INTERVAL seconds
        now     = time.monotonic()
        elapsed = now - start
        if now - last_log_time >= LOG_INTERVAL or current_id > max_id:
            total_done      = already + rows_updated
            total_remaining = total - total_done
            pct             = total_done / total * 100 if total else 0
            rate            = rows_updated / elapsed if elapsed > 0 else 0
            eta_s           = total_remaining / rate if rate > 0 else 0
            batches_done    = batch_num
            batches_left    = total_batches - batch_num

            log.info(
                "batch %d/%d | rows done %s/%s (%.1f%%) | remaining %s | "
                "rate %.0f rows/s | ETA %.1f min",
                batches_done, total_batches,
                f"{total_done:,}", f"{total:,}", pct,
                f"{total_remaining:,}",
                rate, eta_s / 60,
            )
            last_log_time = now

    elapsed = time.monotonic() - start
    log.info(
        "=== Done. Updated %s rows in %.1fs (%.0f rows/s) ===",
        f"{rows_updated:,}", elapsed, rows_updated / elapsed if elapsed else 0,
    )
    engine.dispose()


if __name__ == "__main__":
    main()
