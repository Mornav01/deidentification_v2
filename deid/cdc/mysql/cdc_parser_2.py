import os
import re
import json
import argparse
import subprocess
import logging
import concurrent.futures
import mysql.connector
import pandas as pd
from datetime import datetime
from functools import partial

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
# Configuration
# ============================
MYSQL_USER = "ndadmin"
MYSQL_PASS = "ndADMIN@2025"
MYSQL_DB = "cdc"
WHITELIST_SCHEMAS = {"mobiledoc"}
BATCH_INSERT_SIZE = 2000

# Path and in-memory cache of P0 tables — loaded once at import time
P0_TABLES_CSV_PATH = "/Users/ndaidcnd/Desktop/deidentification/NOTEBOOK/DENT/emd_serono_phi.csv"
DF_P0 = pd.read_csv(P0_TABLES_CSV_PATH)
P0_TABLES = set(DF_P0["TABLE_NAME"].unique())

# ============================
# DB Connection
# ============================
def get_db_conn():
    return mysql.connector.connect(
        host="localhost",
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB
    )

# ============================
# Main Parser
# ============================
def parse_binlog_file(binlog_path, cdc_table_name):
    # Single DB connection + cursor reused for all batches in this file
    conn = get_db_conn()
    cursor = conn.cursor()
    insert_sql = f"""
        INSERT INTO {cdc_table_name}
        (table_name, operation, record_data, binlog_file, binlog_pos)
        VALUES (%s, %s, %s, %s, %s)
    """

    def flush(records):
        if not records:
            return
        cursor.executemany(insert_sql, records)
        conn.commit()

    cmd = ["mysqlbinlog", "--base64-output=DECODE-ROWS", "--verbose", binlog_path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Precompiled regex
    RE_USE       = re.compile(r"[Uu][Ss][Ee] `(.*?)`")
    RE_STMT      = re.compile(r"^(INSERT|UPDATE)\s+", re.IGNORECASE)
    RE_TABLE     = re.compile(r"(?:INTO|UPDATE|FROM)\s+`?([a-zA-Z0-9_]+)`?(?:\.`?([a-zA-Z0-9_]+)`?)?", re.IGNORECASE)
    RE_ROW_INSERT = re.compile(r"### INSERT INTO")
    RE_ROW_UPDATE = re.compile(r"### UPDATE")
    RE_ROW_TABLE  = re.compile(r"### table: `.*?`\.`(.*?)`")

    current_schema = None
    current_op = None
    current_row_based_table = None
    current_data = {}
    current_pos = 0

    records_to_insert = []
    matched_events = 0
    total_lines = 0

    # Precompute basename so it isn't re-evaluated on every record
    binlog_basename = os.path.basename(binlog_path)

    # Local aliases for hot-path speed
    whitelist     = WHITELIST_SCHEMAS
    p0_tables     = P0_TABLES
    append_record = records_to_insert.append
    read_next     = proc.stdout.readline

    logger.info("Streaming and parsing → %s", binlog_path)

    for raw_line in proc.stdout:
        total_lines += 1

        try:
            line = raw_line.decode("utf-8")
        except UnicodeDecodeError:
            line = raw_line.decode("latin1", errors="ignore")

        # Track binlog position — always handled first, then skip to next line
        if line.startswith("# at "):
            sp = line.split(" ")
            if len(sp) > 2:
                try:
                    current_pos = int(sp[2])
                except ValueError:
                    pass
            continue

        # USE schema
        if line.startswith("use `") or line.startswith("USE `"):
            m = RE_USE.match(line)
            if m:
                current_schema = m.group(1)
            continue

        if current_schema not in whitelist:
            continue  # ultra-fast skip for non-whitelisted schemas

        # ------------------------------
        # STATEMENT-BASED (INSERT/UPDATE)
        # ------------------------------
        stmt = RE_STMT.match(line)
        if stmt:
            matched_events += 1
            op_type = stmt.group(1).upper()

            t_match = RE_TABLE.search(line)
            if not t_match:
                continue

            table_schema = t_match.group(1) if t_match.group(2) else current_schema
            table_name   = t_match.group(2) or t_match.group(1)

            if table_schema not in whitelist:
                continue

            if table_name not in p0_tables:
                continue

            # Capture multi-line SQL
            sql_lines = [line.rstrip("\n")]
            while True:
                nxt = read_next()
                if not nxt:
                    break
                try:
                    nxt_line = nxt.decode("utf-8")
                except UnicodeDecodeError:
                    nxt_line = nxt.decode("latin1", errors="ignore")
                stripped = nxt_line.strip()
                if stripped == "/*!*/;":
                    break
                sql_lines.append(nxt_line.rstrip("\n"))
                if stripped.endswith(");"):
                    break

            record_json = json.dumps({"raw_sql": "\n".join(sql_lines)}, ensure_ascii=False)
            append_record((table_name, op_type, record_json, binlog_basename, current_pos))

            if len(records_to_insert) >= BATCH_INSERT_SIZE:
                flush(records_to_insert)
                records_to_insert.clear()
            continue

        # ------------------------------
        # ROW-BASED EVENTS
        # ------------------------------
        if RE_ROW_INSERT.match(line):
            current_op = "INSERT"
            matched_events += 1
            continue

        if RE_ROW_UPDATE.match(line):
            current_op = "UPDATE"
            matched_events += 1
            continue

        t_match = RE_ROW_TABLE.match(line)
        if t_match:
            current_row_based_table = t_match.group(1)
            continue

        # Column lines: ### @1=...
        if line.startswith("###   @"):
            kv = line[7:].split("=", 1)
            if len(kv) == 2:
                current_data[kv[0]] = kv[1].strip()
            continue

        # Row-event commit boundary — only triggered by ### COMMIT
        # ("# at " lines always continue earlier and never reach here)
        if line.startswith("### COMMIT"):
            if current_op and current_data:
                append_record(
                    (current_row_based_table, current_op,
                     json.dumps(current_data, ensure_ascii=False),
                     binlog_basename, current_pos)
                )
            current_op = None
            current_row_based_table = None
            current_data = {}

            if len(records_to_insert) >= BATCH_INSERT_SIZE:
                flush(records_to_insert)
                records_to_insert.clear()

    # Final flush
    flush(records_to_insert)

    # Wait for mysqlbinlog to finish
    _, stderr = proc.communicate()
    err_text = stderr.decode("utf-8", errors="ignore").strip()
    if err_text:
        logger.warning("[mysqlbinlog stderr] %s", err_text)

    cursor.close()
    conn.close()

    logger.info("=== Diagnostic Summary for %s ===", binlog_basename)
    logger.info("Total lines read: %s | Matched CDC events: %s", f"{total_lines:,}", f"{matched_events:,}")
    logger.info("CDC parsing complete for %s", binlog_basename)


def get_files(folder_path, run_date, end_date):
    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    modified_files = []
    for file_path in all_files:
        try:
            mod_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
            if run_date <= mod_date <= end_date:
                modified_files.append(file_path)
        except OSError as e:
            logger.warning("Could not check modification time for %s: %s", file_path, e)

    return sorted([
        f for f in modified_files
        if os.path.basename(f).startswith("binarylogs.")
        and not os.path.basename(f).endswith(".index")
    ])


def parse_args():
    """
    Parse command-line arguments.

    Example:
        python cdc_parser.py --table_name "change_log" --run_date "2025-10-12" --end_date "2025-10-30"
    """
    parser = argparse.ArgumentParser(description="CDC binlog parser")
    parser.add_argument(
        "--table_name",
        required=True,
        help="Target CDC table name where parsed events will be stored",
    )
    parser.add_argument(
        "--run_date",
        required=True,
        help="Run date in YYYY-MM-DD format (used to pick binlog files by modified date)",
    )
    parser.add_argument(
        "--end_date",
        required=False,
        help="End date in YYYY-MM-DD format (used to pick binlog files by modified date)",
    )
    parser.add_argument(
        "--max_workers",
        required=False,
        type=int,
        default=None,
        help="Number of parallel worker processes for binlog parsing (default: number of CPU cores)",
    )
    return parser.parse_args()


def process_single_binlog(binlog_file, cdc_table_name):
    """
    Wrapper to process a single binlog file and capture timing.
    Used by both sequential and parallel execution paths.
    cdc_table_name is passed explicitly so subprocess workers use the correct value.
    """
    start_time = datetime.now()
    logger.info("Starting binlog file: %s", os.path.basename(binlog_file))
    parse_binlog_file(binlog_file, cdc_table_name)
    end_time = datetime.now()

    runtime_seconds = (end_time - start_time).total_seconds()
    logger.info("Completed %s | Runtime: %.2f seconds", os.path.basename(binlog_file), runtime_seconds)

    return {
        "filename":  os.path.basename(binlog_file),
        "starttime": start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "endtime":   end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "runtime":   f"{runtime_seconds:.4f}",
    }


def main():
    args = parse_args()
    cdc_table_name = args.table_name

    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()
    folder   = "/Volumes/NDAIVol/BinaryLog"
    end_date = (
        datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date
        else datetime.today().date()
    )

    logger.info(
        "CDC Parser started | run_date=%s | end_date=%s | table_name=%s | binlog_folder=%s | max_workers=%s",
        args.run_date, end_date, cdc_table_name, folder, args.max_workers or "auto",
    )

    binlog_files = get_files(folder, run_date, end_date)
    logger.info(
        "Total %d binlog files found for run_date=%s end_date=%s: %s",
        len(binlog_files), run_date, end_date, binlog_files,
    )

    if not binlog_files:
        return

    results = []
    worker_fn = partial(process_single_binlog, cdc_table_name=cdc_table_name)
    max_workers = args.max_workers

    if max_workers <= 1:
        for binlog_file in binlog_files:
            results.append(worker_fn(binlog_file))
    else:
        logger.info(
            "Processing %d binlog files in parallel with %d workers",
            len(binlog_files), max_workers,
        )
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(worker_fn, binlog_file): binlog_file
                for binlog_file in binlog_files
            }
            for future in concurrent.futures.as_completed(future_to_file):
                binlog_file = future_to_file[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    logger.exception(
                        "Error processing %s: %s", os.path.basename(binlog_file), exc
                    )

    if results:
        df = pd.DataFrame(results)
        df.to_csv(
            f"/Users/ndaidcnd/Desktop/deidentification/CDC/MySQL/cdc_parser_log_{run_date}.csv",
            index=False,
        )


# ============================
# Entry Point
# ============================
if __name__ == "__main__":
    main()
