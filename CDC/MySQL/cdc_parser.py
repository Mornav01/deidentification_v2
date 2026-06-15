import os
import re
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
    DB_PASS may be stored URL-encoded (e.g. %40 for @); the f-string URL form lets SQLAlchemy
    decode it on parse, consistent with cdc_restore.py / cdc_merge.py.
    """
    user     = os.environ.get("DB_USER", MYSQL_USER)
    password = os.environ.get("DB_PASS", MYSQL_PASS)
    host     = os.environ.get("DB_HOST", "localhost")
    port     = os.environ.get("DB_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{MYSQL_DB}"


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
            try:
                checkout()
            except Exception as e2:
                logger.error("DB Writer reconnect failed: %s", e2)
                return
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
                # Best-effort cleanup during shutdown; ignore close errors.
                pass
        if conn is not None:
            try:
                conn.close()
            except pymysql.err.Error as e:
                logger.debug("DB Writer ignored connection close error during cleanup: %s", e)
        engine.dispose()
    logger.info("DB Writer finished. Total inserted: %s", total_inserted)
 
# ============================
# Worker: Parser (Producer)
# ============================
def worker_parse(binlog_path, p0_tables_set, queue, run_date, end_date=None):
    """
    Producer process that parses a binlog file and pushes records to the queue.
    """
    # --- Precompiled regex (big speed improvement) ---
    RE_USE = re.compile(r"[Uu][Ss][Ee] `(.*?)`")
    RE_STMT = re.compile(r"^(INSERT|UPDATE)\s+", re.IGNORECASE)
    RE_TABLE = re.compile(r"(?:INTO|UPDATE|FROM)\s+`?([a-zA-Z0-9_]+)`?(?:\.`?([a-zA-Z0-9_]+)`?)?", re.IGNORECASE)
    # Match: ### INSERT INTO `schema`.`table` or ### INSERT INTO schema.table
    RE_ROW_INSERT = re.compile(r"### INSERT INTO (?:`([^`]+)`\.`([^`]+)`|([^`\.\s]+)\.([^`\.\s]+))")
    RE_ROW_UPDATE = re.compile(r"### UPDATE (?:`([^`]+)`\.`([^`]+)`|([^`\.\s]+)\.([^`\.\s]+))")
    RE_ROW_TABLE = re.compile(r"### table: `.*?`\.`(.*?)`")
 
    current_schema = None
    current_op = None
    current_row_based_table = None
    current_where = {}   # before-image: UPDATE WHERE columns
    current_set   = {}   # after-image:  INSERT/UPDATE SET columns
    row_section   = None # "WHERE" | "SET" | None
    current_pos = 0
 
    matched_events = 0
    total_lines = 0
    
    binlog_basename = os.path.basename(binlog_path)
    whitelist = WHITELIST_SCHEMAS
 
    # Use start/stop datetime to filter events at the source (mysqlbinlog)
    # This significantly reduces the data piped to Python if we only need a specific window
    # Note: run_date is a date object, so we default to full day
    start_dt = datetime.combine(run_date, datetime.min.time())
    
    # Format for mysqlbinlog: "YYYY-MM-DD HH:MM:SS"
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    cmd = ["mysqlbinlog", "--base64-output=DECODE-ROWS", "--verbose", 
           f"--start-datetime={start_str}", binlog_path]
    
    if end_date:
        end_dt = datetime.combine(end_date, datetime.max.time())
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        cmd.append(f"--stop-datetime={end_str}")
    
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        logger.error(f"Failed to start mysqlbinlog for {binlog_path}: {e}")
        return 0, 0
 
    # Alias for speed
    read_next = proc.stdout.readline
 
    for raw_line in proc.stdout:
        total_lines += 1
 
        # FAST decode (no strip!)
        try:
            line = raw_line.decode("utf-8")
        except:
            line = raw_line.decode("latin1", errors="replace")
 
        # Track binlog position (but don't continue - let it fall through to commit check)
        if line.startswith("# at "):
            sp = line.split(" ")
            if len(sp) > 2:
                try:
                    current_pos = int(sp[2])
                except:
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
        #  STATEMENT-BASED (INSERT/UPDATE)
        # ------------------------------
        stmt = RE_STMT.match(line)
        if stmt:
            op_type = stmt.group(1).upper()
 
            # Extract table
            t_match = RE_TABLE.search(line)
            if not t_match:
                continue
 
            table_schema = t_match.group(1) if t_match.group(2) else current_schema
            table_name = t_match.group(2) or t_match.group(1)
 
            if table_schema not in whitelist:
                continue
 
            # Check P0 tables (O(1) lookup)
            # if table_name not in p0_tables_set:
            #     continue
            
            matched_events += 1
 
            # FAST MULTI-LINE SQL CAPTURE
            sql_lines = [line.rstrip("\n")]
 
            while True:
                nxt = read_next()
                if not nxt:
                    break
 
                try:
                    nxt_line = nxt.decode("utf-8")
                except:
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
 
            # Convert to JSON for storage
            record_json = json_dumps({"raw_sql": full_sql})
 
            # Push to queue
            queue.put((table_name, op_type, record_json, binlog_basename, current_pos))
            continue
 
        # ------------------------------
        #  ROW-BASED EVENTS
        # ------------------------------
        m_insert = RE_ROW_INSERT.match(line)
        if m_insert:
            # Flush any pending row before starting the next one (multi-row events)
            if current_op and current_set and current_row_based_table:
                if p0_tables_set is None or current_row_based_table in p0_tables_set:
                    matched_events += 1
                    if current_op == "UPDATE":
                        record = {"format": "row_update", "where": current_where, "set": current_set}
                    else:
                        record = {"format": "row", "data": current_set}
                    queue.put((current_row_based_table, current_op, json_dumps(record), binlog_basename, current_pos))

            current_op = "INSERT"
            current_where = {}
            current_set   = {}
            row_section   = None
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
            # Flush any pending row before starting the next one (multi-row events)
            if current_op and current_set and current_row_based_table:
                if p0_tables_set is None or current_row_based_table in p0_tables_set:
                    matched_events += 1
                    if current_op == "UPDATE":
                        record = {"format": "row_update", "where": current_where, "set": current_set}
                    else:
                        record = {"format": "row", "data": current_set}
                    queue.put((current_row_based_table, current_op, json_dumps(record), binlog_basename, current_pos))

            current_op = "UPDATE"
            current_where = {}
            current_set   = {}
            row_section   = None
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
 
        t_match = RE_ROW_TABLE.match(line)
        if t_match:
            current_row_based_table = t_match.group(1)
            continue
 
        # Row section headers — set routing flag for subsequent @N=val lines
        if line.startswith("### WHERE"):
            row_section = "WHERE"
            continue
        if line.startswith("### SET"):
            row_section = "SET"
            continue

        # Column lines: ###   @1=...
        if line.startswith("###   @"):
            kv = line[7:].split("=", 1)
            if len(kv) == 2:
                if row_section == "WHERE":
                    current_where[kv[0]] = kv[1].strip()
                else:  # "SET" or None (INSERT before SET header seen)
                    current_set[kv[0]] = kv[1].strip()
            continue
 
        # Event boundary → commit row event
        if line.startswith("### COMMIT") or line.startswith("# at "):
            if current_schema in whitelist and current_op and current_set and current_row_based_table:
                if p0_tables_set is None or current_row_based_table in p0_tables_set:
                    matched_events += 1
                    if current_op == "UPDATE":
                        record = {"format": "row_update", "where": current_where, "set": current_set}
                    else:
                        record = {"format": "row", "data": current_set}
                    queue.put((
                        current_row_based_table,
                        current_op,
                        json_dumps(record),
                        binlog_basename,
                        current_pos,
                    ))

            current_op = None
            current_row_based_table = None
            current_where = {}
            current_set   = {}
            row_section   = None
 
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
    
    # Convert dates to timestamps for faster comparison
    run_ts = datetime.combine(run_date, datetime.min.time()).timestamp()
    end_ts = datetime.combine(end_date, datetime.max.time()).timestamp()
    
    logger.info(f"Scanning {folder_path} for files modified between {run_date} and {end_date}")
    
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
                    if run_ts <= mtime <= end_ts:
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
        "--p0_csv",
        default=None,
        help="Path to P0 tables CSV file. Omit to process ALL tables.",
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
def worker_wrapper(files, p0_set, data_q, stats_q, r_date, e_date):
    for f in files:
        try:
            matched, total = worker_parse(f, p0_set, data_q, r_date, e_date)
            stats_q.put({
                'type': 'file_complete',
                'filename': os.path.basename(f),
                'matched_events': matched,
                'total_lines': total
            })
            logger.info(f"Processed {os.path.basename(f)}: {matched} events")
        except Exception as e:
            logger.error(f"Error processing {f}: {e}")
 
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
            end_date = datetime.today().date()
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
            p0_tables_set = set(df['TABLE_NAME'].unique())
            logger.info(f"Loaded {len(p0_tables_set)} P0 tables")
        except Exception as e:
            logger.error(f"Failed to load P0 tables CSV: {e}")
            return
    else:
        logger.info("No --p0_csv provided — processing ALL tables")
 
    # 2. Get Files (Optimized)
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
                
            p = mp.Process(target=worker_wrapper, args=(files_chunk, p0_tables_set, queue, stats_queue, run_date, end_date))
            p.start()
            parser_processes.append(p)
 
        # Monitor progress and update checkpoint incrementally
        file_stats = []
        active_workers = num_workers
        processed_files_buffer = set()
        
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