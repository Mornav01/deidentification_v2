import os
import re
import sys
import json
import argparse
import subprocess
import logging
import pymysql.err
import pandas as pd
import multiprocessing as mp
import time
from datetime import datetime
from queue import Empty

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Try to import orjson for faster JSON serialization
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False
 
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
MYSQL_USER = os.environ.get("DB_USER", "")
MYSQL_PASS = os.environ.get("DB_PASS", "")
MYSQL_DB = os.environ.get("DB_CDC_SCHEMA", "cdc")
WHITELIST_SCHEMAS = {"mobiledoc"}
 
# ============================
# JSON Helper
# ============================
def json_dumps(data):
    if HAS_ORJSON:
        # orjson returns bytes, so we decode to string for compatibility with text-based DB drivers
        # If your DB driver supports bytes, you can remove .decode('utf-8')
        return orjson.dumps(data).decode('utf-8')
    return json.dumps(data, ensure_ascii=False)
 
# ============================
# Checkpoint Manager
# ============================
def load_checkpoint(checkpoint_file):
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logger.warning(f"Failed to load checkpoint file: {e}")
    return set()
 
def save_checkpoint(checkpoint_file, processed_files):
    try:
        # Load existing first to merge (to be safe against race conditions if multiple writers existed, though here main is single writer)
        # For performance in main loop, we might just append or overwrite if we maintain state in memory
        # But reading every time is safer if we want to be robust
        if os.path.exists(checkpoint_file):
             with open(checkpoint_file, 'r') as f:
                current = set(json.load(f))
        else:
            current = set()
            
        current.update(processed_files)
        
        # Atomic write pattern
        temp_file = checkpoint_file + ".tmp"
        with open(temp_file, 'w') as f:
            json.dump(list(current), f)
        os.replace(temp_file, checkpoint_file)
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")
 
# ============================
# DB URL (SQLAlchemy + PyMySQL)
# ============================
def get_cdc_db_url() -> str:
    """
    mysql+pymysql URL for the CDC database.
    Override with DB_USER / DB_PASS / DB_HOST / DB_PORT (same convention as cdc_restore.py).
    Built via URL.create (same as deid/config/schema.py DbConfig.connection_string) so
    special characters in DB_PASS (e.g. @) are percent-encoded correctly instead of
    breaking a hand-built f-string URL.
    """
    user     = os.environ.get("DB_USER", MYSQL_USER)
    password = os.environ.get("DB_PASS", MYSQL_PASS)
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=MYSQL_DB,
    ).render_as_string(hide_password=False)


def _writer_engine(db_url: str, pool_size: int):
    ps = max(1, min(int(pool_size), 32))
    return create_engine(
        db_url,
        pool_size=ps,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


# ============================
# Worker: DB Writer (Consumer)
# ============================
def worker_db_writer(queue, table_name, db_url, batch_size=2000, commit_interval=1.0, writer_pool_size=3):
    """
    Consumer process that pulls records from the queue and batch inserts into MySQL.

    Uses a SQLAlchemy engine (mysql+pymysql) with a per-process pool; checkout a raw
    DBAPI connection for fast executemany + commit.
    """
    logger.info(
        "DB Writer started | batch=%s | commit_interval=%ss | pool_size=%s",
        batch_size,
        commit_interval,
        writer_pool_size,
    )
    try:
        engine = _writer_engine(db_url, writer_pool_size)
    except Exception as e:
        logger.error("DB Writer failed to create engine: %s", e)
        return

    conn = None
    cursor = None

    def checkout():
        nonlocal conn, cursor
        if cursor is not None:
            try:
                cursor.close()
            except pymysql.err.Error:
                pass
            cursor = None
        if conn is not None:
            try:
                conn.close()
            except pymysql.err.Error:
                pass
            conn = None
        conn = engine.raw_connection()
        cursor = conn.cursor()

    try:
        checkout()
    except Exception as e:
        logger.error("DB Writer failed initial checkout: %s", e)
        try:
            engine.dispose()
        except Exception:
            pass
        return

    batch = []
    last_commit_time = time.time()

    sql = f"""
        INSERT IGNORE INTO `{table_name}`
        (table_name, operation, record_data, binlog_file, binlog_pos)
        VALUES (%s, %s, %s, %s, %s)
    """

    total_inserted = 0

    def flush_batch():
        nonlocal batch, last_commit_time, total_inserted
        if not batch:
            return
        cursor.executemany(sql, batch)
        conn.commit()
        total_inserted += len(batch)
        batch = []
        last_commit_time = time.time()

    def run_flush():
        if not batch:
            return
        try:
            flush_batch()
        except pymysql.err.Error as e:
            logger.error("DB Writer MySQL error during flush: %s", e)
            # Collect the binlog files touched by this batch so we can clean up
            # any partial records that may have been committed before the drop.
            affected_binlogs = {record[3] for record in batch if record[3]}
            try:
                checkout()
            except Exception as e2:
                logger.error("DB Writer reconnect failed: %s", e2)
                return
            # Drop all records for affected binlog files to avoid duplicates on retry.
            if affected_binlogs:
                try:
                    placeholders = ", ".join(["%s"] * len(affected_binlogs))
                    cleanup_cur = conn.cursor()
                    # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                    cleanup_cur.execute(
                        f"DELETE FROM `{table_name}` WHERE binlog_file IN ({placeholders})",
                        tuple(affected_binlogs),
                    )
                    conn.commit()
                    cleanup_cur.close()
                    logger.info(
                        "Cleaned up partial records for %d binlog file(s) before retry: %s",
                        len(affected_binlogs), affected_binlogs,
                    )
                except Exception as e_del:
                    logger.error("Failed to clean up binlog records: %s", e_del)
            if not batch:
                return
            try:
                flush_batch()
            except pymysql.err.Error as e3:
                logger.error("DB Writer flush after reconnect failed: %s", e3)

    try:
        while True:
            try:
                record = queue.get(timeout=0.5)
                if record is None:
                    break
                batch.append(record)
                if len(batch) >= batch_size:
                    run_flush()
            except Empty:
                if batch and (time.time() - last_commit_time > commit_interval):
                    run_flush()
                continue
            except Exception as e:
                logger.error("DB Writer error: %s", e)
                time.sleep(1)

        if batch:
            run_flush()
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except pymysql.err.Error:
                pass
        if conn is not None:
            try:
                conn.close()
            except pymysql.err.Error:
                pass
        engine.dispose()
    logger.info("DB Writer finished. Total inserted: %s", total_inserted)
 
# ============================
# Worker: Parser (Producer)
# ============================
def worker_parse(binlog_path, p0_tables_set, queue, run_date, end_date=None, exclude_p0=False):
    """
    Producer process that parses a binlog file and pushes records to the queue.
    """
    # --- Precompiled regex (big speed improvement) ---
    RE_USE = re.compile(r"[Uu][Ss][Ee] `(.*?)`")
    RE_STMT = re.compile(r"^(INSERT|UPDATE|DELETE)\s+", re.IGNORECASE)
    RE_TABLE = re.compile(r"(?:INTO|UPDATE|FROM)\s+`?([a-zA-Z0-9_]+)`?(?:\.`?([a-zA-Z0-9_]+)`?)?", re.IGNORECASE)
    # Match: ### INSERT INTO `schema`.`table` or ### INSERT INTO schema.table
    RE_ROW_INSERT = re.compile(r"### INSERT INTO (?:`([^`]+)`\.`([^`]+)`|([^`\.\s]+)\.([^`\.\s]+))")
    RE_ROW_UPDATE = re.compile(r"### UPDATE (?:`([^`]+)`\.`([^`]+)`|([^`\.\s]+)\.([^`\.\s]+))")
    RE_ROW_DELETE = re.compile(r"### DELETE FROM (?:`([^`]+)`\.`([^`]+)`|([^`\.\s]+)\.([^`\.\s]+))")
    RE_ROW_TABLE = re.compile(r"### table: `.*?`\.`(.*?)`")
    # Statement-based AUTO_INCREMENT value: MySQL emits a standalone Intvar event
    # (rendered by mysqlbinlog as `SET INSERT_ID=N`) immediately before an INSERT
    # that assigns an AUTO_INCREMENT column.  The PK value is NOT inside the INSERT
    # statement itself, so this is the only place to recover the exact prod PK.
    RE_INSERT_ID = re.compile(r"^SET INSERT_ID=(\d+)", re.IGNORECASE)

    current_schema = None
    current_op = None
    current_row_based_table = None
    current_data = {}
    current_pos = 0
    # Most recent SET INSERT_ID value seen; consumed by the next statement.
    pending_insert_id = None
 
    matched_events = 0
    total_lines = 0
    
    binlog_basename = os.path.basename(binlog_path)
    whitelist = WHITELIST_SCHEMAS
 
    # end_date is the lower bound (start of window), run_date is the upper bound (end of window)
    start_dt = datetime.combine(end_date, datetime.min.time())
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

    stop_dt = datetime.combine(run_date, datetime.max.time())
    stop_str = stop_dt.strftime("%Y-%m-%d %H:%M:%S")

    cmd = ["mysqlbinlog", "--base64-output=DECODE-ROWS", "--verbose",
           f"--start-datetime={start_str}", f"--stop-datetime={stop_str}", binlog_path]
    
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError(f"Failed to start mysqlbinlog for {binlog_path}: {e}") from e
 
    # Alias for speed
    read_next = proc.stdout.readline

    # Memoize the filter verdict per raw table name. The same name recurs
    # across thousands of rows in a binlog, so caching turns the repeated
    # .lower() + set lookup into a single dict hit after the first sighting.
    # Works for both modes; an allow-list can't be prebuilt for exclude mode
    # since the universe of non-P0 table names isn't known up front.
    _verdict_cache = {}

    def table_allowed(name):
        """Whether a table passes the P0 filter.

        No CSV (p0_tables_set is None) → all tables pass. With a CSV, the
        membership test is inverted by exclude_p0: include-mode keeps only
        listed tables; exclude-mode keeps everything except listed tables.
        """
        if p0_tables_set is None:
            return True
        verdict = _verdict_cache.get(name)
        if verdict is None:
            in_set = name.lower() in p0_tables_set
            verdict = (not in_set) if exclude_p0 else in_set
            _verdict_cache[name] = verdict
        return verdict

    def flush_row():
        """Emit the currently-buffered row event (if any) and reset row state.

        Row-based events can pack many rows into a single binlog event (e.g. a
        bulk INSERT/UPDATE). Each row restarts with a `### <OP>` marker
        but reuses the same `@N` column keys, so we must flush the previous row
        before the next marker overwrites current_data — otherwise only the last
        row of the event survives.
        """
        nonlocal current_op, current_row_based_table, current_data, matched_events
        if (current_schema in whitelist and current_op and current_data
                and current_row_based_table
                and table_allowed(current_row_based_table)):
            matched_events += 1
            # Store the table name lowercased — the change-log table holds
            # names in lowercase only, so downstream lookups stay consistent
            # regardless of the binlog's original casing.
            queue.put((
                current_row_based_table.lower(),
                current_op,
                json_dumps(current_data),
                binlog_basename,
                current_pos,
            ))
        current_op = None
        current_row_based_table = None
        current_data = {}

    for raw_line in proc.stdout:
        total_lines += 1
 
        # FAST decode (no strip!)
        try:
            line = raw_line.decode("utf-8")
        except UnicodeDecodeError:
            line = raw_line.decode("latin1", errors="replace")
 
        # Track binlog position (but don't continue - let it fall through to commit check)
        if line.startswith("# at "):
            sp = line.split(" ")
            if len(sp) > 2:
                try:
                    current_pos = int(sp[2])
                except (ValueError, IndexError):
                    pass
            # Don't continue here - let it fall through to the commit check below
 
        # USE schema
        if line.startswith("USE `") or line.startswith("use `"):
            m = RE_USE.match(line)
            if m:
                current_schema = m.group(1)
            continue
 
        if current_schema not in whitelist:
            continue  # ultra fast skip for non-whitelisted schemas

        # ------------------------------
        #  AUTO_INCREMENT value (statement-based)
        #  Buffer the SET INSERT_ID=N that precedes an auto-increment INSERT.
        #  The intervening `# at <pos>` event line only updates current_pos /
        #  flushes row state, so this value survives until the next statement.
        # ------------------------------
        m_iid = RE_INSERT_ID.match(line)
        if m_iid:
            pending_insert_id = int(m_iid.group(1))
            continue

        # ------------------------------
        #  STATEMENT-BASED (INSERT/UPDATE)
        # ------------------------------
        stmt = RE_STMT.match(line)
        if stmt:
            op_type = stmt.group(1).upper()
            # Consume the buffered INSERT_ID into a local immediately and clear the
            # shared state, so it can NEVER leak onto a later statement — even if
            # this one is dropped below (no table match / non-whitelisted schema).
            stmt_insert_id = pending_insert_id
            pending_insert_id = None

            # Extract table
            t_match = RE_TABLE.search(line)
            if not t_match:
                continue
 
            table_schema = t_match.group(1) if t_match.group(2) else current_schema
            table_name = t_match.group(2) or t_match.group(1)
 
            if table_schema not in whitelist:
                continue

            # Apply the P0 filter (case-insensitive), mirroring the row-based
            # path so both honor the same list and the same include/exclude mode.
            if not table_allowed(table_name):
                continue

            matched_events += 1
 
            # FAST MULTI-LINE SQL CAPTURE
            sql_lines = [line.rstrip("\n")]
 
            while True:
                nxt = read_next()
                if not nxt:
                    break
 
                try:
                    nxt_line = nxt.decode("utf-8")
                except UnicodeDecodeError:
                    nxt_line = nxt.decode("latin1", errors="replace")
 
                stripped = nxt_line.strip()
 
                # If mysqlbinlog metadata terminator encountered → STOP, but do not include
                if stripped == "/*!*/;":
                    break
 
                sql_lines.append(nxt_line.rstrip("\n"))
 
                # Real SQL end: line ends with );
                if stripped.endswith(");"):
                    break
 
            full_sql = "\n".join(sql_lines)

            # Convert to JSON for storage.  Attach the captured AUTO_INCREMENT value
            # so the restore can reproduce the exact prod PK instead of re-assigning
            # one (only meaningful for INSERTs; other statements ignore it).
            record = {"raw_sql": full_sql}
            # Store only a positive INSERT_ID.  0 / missing means "no PK captured",
            # which the restore treats as a hard error (it refuses to let MySQL
            # auto-assign a PK) rather than silently drifting.
            if op_type == "INSERT" and stmt_insert_id:
                record["insert_id"] = stmt_insert_id
            record_json = json_dumps(record)

            # Push to queue — lowercase the name to match the row-based path
            # so the change-log table stores table names in lowercase only.
            queue.put((table_name.lower(), op_type, record_json, binlog_basename, current_pos))
            continue
 
        # ------------------------------
        #  ROW-BASED EVENTS
        # ------------------------------
        m_insert = RE_ROW_INSERT.match(line)
        if m_insert:
            flush_row()  # flush previous row in a multi-row event
            current_op = "INSERT"
            # Extract schema and table - handle both backtick and non-backtick formats
            groups = m_insert.groups()
            if groups[0] and groups[1]:  # Backtick format: `schema`.`table`
                schema_from_line = groups[0]
                current_row_based_table = groups[1]
            elif groups[2] and groups[3]:  # Non-backtick format: schema.table
                schema_from_line = groups[2]
                current_row_based_table = groups[3]
            else:
                schema_from_line = None
                current_row_based_table = None
                
            # If we don't have a current_schema yet, use the one from the line
            if schema_from_line and current_schema is None:
                current_schema = schema_from_line
            continue
 
        m_update = RE_ROW_UPDATE.match(line)
        if m_update:
            flush_row()  # flush previous row in a multi-row event
            current_op = "UPDATE"
            # Extract schema and table - handle both backtick and non-backtick formats
            groups = m_update.groups()
            if groups[0] and groups[1]:  # Backtick format: `schema`.`table`
                schema_from_line = groups[0]
                current_row_based_table = groups[1]
            elif groups[2] and groups[3]:  # Non-backtick format: schema.table
                schema_from_line = groups[2]
                current_row_based_table = groups[3]
            else:
                schema_from_line = None
                current_row_based_table = None
                
            # If we don't have a current_schema yet, use the one from the line
            if schema_from_line and current_schema is None:
                current_schema = schema_from_line
            continue

        m_delete = RE_ROW_DELETE.match(line)
        if m_delete:
            flush_row()  # flush previous row in a multi-row event
            current_op = "DELETE"
            # Extract schema and table - handle both backtick and non-backtick formats
            groups = m_delete.groups()
            if groups[0] and groups[1]:  # Backtick format: `schema`.`table`
                schema_from_line = groups[0]
                current_row_based_table = groups[1]
            elif groups[2] and groups[3]:  # Non-backtick format: schema.table
                schema_from_line = groups[2]
                current_row_based_table = groups[3]
            else:
                schema_from_line = None
                current_row_based_table = None

            # If we don't have a current_schema yet, use the one from the line
            if schema_from_line and current_schema is None:
                current_schema = schema_from_line
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
 
        # Event boundary → commit the last buffered row event
        if line.startswith("### COMMIT") or line.startswith("# at "):
            flush_row()
 
    # Flush any row still buffered at EOF (no trailing boundary line followed it)
    flush_row()

    # Wait for mysqlbinlog to finish
    _, stderr = proc.communicate()
    err_text = stderr.decode("utf-8", errors="ignore").strip()
    if err_text:
        logger.warning("[mysqlbinlog stderr output] %s", err_text)
        
    return matched_events, total_lines
 
# ============================
# File Scanning
# ============================
def get_files(folder_path, run_date, end_date):
    # Optimized file scanning using os.scandir
    modified_files = []
    
    # end_date is the lower bound (start of window), run_date is the upper bound (end of window)
    end_ts = datetime.combine(end_date, datetime.min.time()).timestamp()
    run_ts = datetime.combine(run_date, datetime.max.time()).timestamp()

    logger.info(f"Scanning {folder_path} for files modified between {end_date} and {run_date}")

    try:
        with os.scandir(folder_path) as it:
            for entry in it:
                if not entry.is_file():
                    continue

                # Check name pattern first (fastest check)
                name = entry.name
                if not (name.startswith('binarylogs.') and not name.endswith('.index')):
                    continue

                try:
                    # Check modification time
                    mtime = entry.stat().st_mtime
                    if end_ts <= mtime <= run_ts:
                        modified_files.append(entry.path)
                except OSError:
                    continue
                    
    except OSError as e:
        logger.warning(f"Error scanning directory {folder_path}: {e}")
        
    # Sort files to ensure processing order
    return sorted(modified_files)
 
# ============================
# Argument Parsing
# ============================
def parse_args():
    parser = argparse.ArgumentParser(description="CDC binlog parser")
    parser.add_argument(
        "--table_name",
        required=True,
        help="Target CDC table name where parsed events will be stored",
    )
    parser.add_argument(
        "--run_date",
        required=True,
        help="Run date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        required=False,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--num_writers",
        type=int,
        default=5,
        help="Number of DB writer processes (default: 5)",
    )
    parser.add_argument(
        "--writer_pool_size",
        type=int,
        default=3,
        help="SQLAlchemy pool_size per writer process (mysql+pymysql, default: 3)",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=None,
        help="Number of parser worker processes (default: auto-detected)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=5000,
        help="DB insert batch size (default: 5000)",
    )
    parser.add_argument(
        "--commit_interval",
        type=float,
        default=1.0,
        help="DB commit interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--binlog_dir",
        default=os.environ.get("BINLOG_DIR", "/Volumes/NDAIVol/BinaryLog"),
        help="Directory containing binary logs",
    )
    parser.add_argument(
        "--binlog_file",
        default=None,
        help="Path to a single binary log file to process. When provided, "
             "--binlog_dir scanning and checkpoint filtering are skipped.",
    )
    parser.add_argument(
        "--p0_csv",
        default=None,
        help="Path to P0 tables CSV file. Omit to process ALL tables.",
    )
    parser.add_argument(
        "--exclude_p0",
        action="store_true",
        help="Invert --p0_csv: process all tables EXCEPT those in the CSV "
             "(instead of only those listed). No effect without --p0_csv.",
    )
    parser.add_argument(
        "--checkpoint_file",
        default="cdc_checkpoint.json",
        help="Path to checkpoint JSON file",
    )
    return parser.parse_args()
 
# ============================
# Worker Wrapper (must be module-level for multiprocessing spawn pickling)
# ============================
def worker_wrapper(files, p0_set, data_q, stats_q, r_date, e_date, exclude_p0=False):
    for f in files:
        try:
            matched, total = worker_parse(f, p0_set, data_q, r_date, e_date, exclude_p0)
            stats_q.put({
                'type': 'file_complete',
                'filename': os.path.basename(f),
                'matched_events': matched,
                'total_lines': total
            })
            logger.info(f"Processed {os.path.basename(f)}: {matched} events")
        except Exception as e:
            logger.error(f"Error processing {f}: {e}")
            stats_q.put({'type': 'file_error', 'filename': os.path.basename(f), 'error': str(e)})

    stats_q.put({'type': 'worker_done'})
 
 
# ============================
# Main Orchestrator
# ============================
def main():
    args = parse_args()
    cdc_table_name = args.table_name
 
    # Parse dates
    try:
        run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        else:
            end_date = run_date  # single-day window when end_date not provided
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return
 
    folder = args.binlog_dir
 
    logger.info(f"CDC Parser started | run_date={run_date} | end_date={end_date} | table={cdc_table_name}")
    if HAS_ORJSON:
        logger.info("Using orjson for faster JSON serialization")
 
    # 1. Load P0 Tables ONCE (Global Optimization)
    # p0_tables_set = None means no filter — process ALL tables
    p0_tables_set = None
    if args.p0_csv:
        try:
            logger.info(f"Loading P0 tables from {args.p0_csv}")
            df = pd.read_csv(args.p0_csv)
            # Normalise to lowercase so matching is case-insensitive (binlog table
            # names can differ in case from the CSV).  Strip whitespace and drop blanks.
            p0_tables_set = {str(t).strip().lower() for t in df['TABLE_NAME'].dropna().unique()}
            p0_tables_set.discard("")
            mode = "EXCLUDING" if args.exclude_p0 else "restricting to"
            logger.info(f"Loaded {len(p0_tables_set)} P0 tables — {mode} them")
        except Exception as e:
            logger.error(f"Failed to load P0 tables CSV: {e}")
            return
    else:
        logger.info("No --p0_csv provided — processing ALL tables")
 
    # 2. Get Files (Optimized)
    if args.binlog_file:
        # Single-file mode: process exactly the file provided, bypassing
        # directory scanning and checkpoint filtering.
        if not os.path.isfile(args.binlog_file):
            logger.error(f"--binlog_file not found: {args.binlog_file}")
            return
        binlog_files = [args.binlog_file]
        logger.info(f"Single-file mode: processing {args.binlog_file}")
    else:
        binlog_files = get_files(folder, run_date, end_date)
        logger.info(f"Found {len(binlog_files)} binlog files to process")

        # Checkpoint Filtering
        processed_files_set = load_checkpoint(args.checkpoint_file)
        if processed_files_set:
            original_count = len(binlog_files)
            binlog_files = [f for f in binlog_files if os.path.basename(f) not in processed_files_set]
            logger.info(f"Skipping {original_count - len(binlog_files)} already processed files based on checkpoint")

    if not binlog_files:
        logger.info("No new files to process. Exiting.")
        return
 
    # 3. Setup Multiprocessing
    # Queue size limit provides backpressure if DB writer is slow
    # Use mp.Queue() directly for better performance (avoiding Manager proxy overhead)
    queue = mp.Queue(maxsize=10000)
    
    writer_processes = []
    parser_processes = []
    
    try:
        # Start DB Writers (Consumer Pool)
        db_url = get_cdc_db_url()
        for i in range(args.num_writers):
            wp = mp.Process(
                target=worker_db_writer,
                args=(
                    queue,
                    cdc_table_name,
                    db_url,
                    args.batch_size,
                    args.commit_interval,
                    args.writer_pool_size,
                ),
            )
            wp.start()
            writer_processes.append(wp)
        
        # Determine worker count (leave one core for writer/OS)
        if args.max_workers:
            num_workers = args.max_workers
        else:
            # Adjust for multiple writers
            available_cores = mp.cpu_count() - args.num_writers - 1 
            num_workers = min(available_cores, len(binlog_files))
        
        num_workers = max(1, num_workers) # Ensure at least 1 worker
        logger.info(f"Starting {num_workers} parser worker processes and {args.num_writers} DB writers")
 
        # 4. Process Files (Producers)
        # Note: We cannot use mp.Pool with mp.Queue easily because Queue is not picklable 
        # in the way Pool expects on some platforms. Instead, we'll spawn processes manually.
        
        start_time = datetime.now()
        
        # Divide files among workers
        chunk_size = (len(binlog_files) + num_workers - 1) // num_workers
        
        # We need a way to get stats back. mp.Queue is good for this too.
        stats_queue = mp.Queue()
         
        for i in range(num_workers):
            files_chunk = binlog_files[i * chunk_size : (i + 1) * chunk_size]
            if not files_chunk:
                # If we have fewer files than workers, send a done signal immediately
                stats_queue.put({'type': 'worker_done'})
                continue
                
            p = mp.Process(target=worker_wrapper, args=(files_chunk, p0_tables_set, queue, stats_queue, run_date, end_date, args.exclude_p0))
            p.start()
            parser_processes.append(p)
 
        # Monitor progress and update checkpoint incrementally
        file_stats = []
        active_workers = num_workers
        processed_files_buffer = set()
        failed_files = []

        while active_workers > 0:
            try:
                msg = stats_queue.get(timeout=1.0)
                if msg['type'] == 'worker_done':
                    active_workers -= 1
                elif msg['type'] == 'file_complete':
                    file_stats.append(msg)
                    processed_files_buffer.add(msg['filename'])

                    # Update checkpoint every 10 files or so to reduce I/O
                    if len(processed_files_buffer) >= 10:
                        save_checkpoint(args.checkpoint_file, processed_files_buffer)
                        processed_files_buffer.clear()
                elif msg['type'] == 'file_error':
                    failed_files.append(msg['filename'])
            except Empty:
                continue
        
        # Final checkpoint save
        if processed_files_buffer:
            save_checkpoint(args.checkpoint_file, processed_files_buffer)
 
        # Wait for all parsers to finish (should be done by now)
        for p in parser_processes:
            p.join()
            
        # Signal DB writers to finish (one None per writer)
        for _ in range(args.num_writers):
            queue.put(None)
            
        for wp in writer_processes:
            wp.join()
            
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"All processing complete in {total_duration:.2f} seconds")
 
        # Save stats
        if file_stats:
            stats_df = pd.DataFrame(file_stats)
            output_csv = f"cdc_parser_log_{run_date}.csv"
            stats_df.to_csv(output_csv, index=False)
            logger.info(f"Stats saved to {output_csv}")

        if failed_files:
            logger.error(
                "%d binlog file(s) failed to process: %s — DAG will be marked as failed.",
                len(failed_files), failed_files,
            )
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Interrupted! Terminating processes...")
        for p in parser_processes:
            if p.is_alive():
                p.terminate()
        for wp in writer_processes:
            if wp.is_alive():
                wp.terminate()
        # Try to save whatever we have processed so far
        if 'processed_files_buffer' in locals() and processed_files_buffer:
             save_checkpoint(args.checkpoint_file, processed_files_buffer)
             
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        # Cleanup
        for p in parser_processes:
            if p.is_alive():
                p.terminate()
        for wp in writer_processes:
            if wp.is_alive():
                wp.terminate()
    finally:
        # Ensure everything is joined
        for p in parser_processes:
            if p.is_alive():
                p.join(timeout=1)
        for wp in writer_processes:
            if wp.is_alive():
                wp.join(timeout=1)
 
if __name__ == "__main__":
    # Set start method to 'spawn' for compatibility (default on macOS, good for safety)
    try:
        mp.set_start_method('spawn')
    except RuntimeError:
        pass
    main()