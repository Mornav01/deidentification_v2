import os
import queue
import subprocess
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("mysql_restore.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source (dump) credentials
# ---------------------------------------------------------------------------
SRC_HOST     = "172.30.0.74"
SRC_PORT     = "4928"
SRC_USER     = "ndiscoveryro"
SRC_PASSWORD = quote_plus("Silencer9-Carnivore-Seldom")
SRC_SCHEMA   = "mobiledoc"

# ---------------------------------------------------------------------------
# Destination (restore) credentials
# ---------------------------------------------------------------------------
DST_HOST     = "localhost"
DST_PORT     = "3306"
DST_USER     = "ndadmin"
DST_PASSWORD = "ndADMIN@2025"
DST_SCHEMA   = "mobiledoc"

DUMP_FOLDER  = SRC_SCHEMA          # local folder where .sql files are written
RESTORE_FROM = f"/Volumes/NDAIVol/MySQL Dump/mobiledoc"
DUMP_THREADS    = 30   # parallel mysqldump workers
RESTORE_THREADS = 20   # parallel mysql restore workers

# ---------------------------------------------------------------------------
# DUMP  –  parallel, one .sql file per table
# ---------------------------------------------------------------------------

def dump_table(table: str) -> None:
    os.makedirs(DUMP_FOLDER, exist_ok=True)
    dump_file = os.path.join(DUMP_FOLDER, f"{table}.sql")

    command = [
        "mysqldump",
        "--default-character-set=utf8",
        "--no-tablespaces",
        "--single-transaction",
        "--skip-lock-tables",
        "--quick",                  # stream rows instead of buffering whole table
        "--compression-algorithms=zlib",   # compress data in transit
        "-h", SRC_HOST,
        "-P", SRC_PORT,
        "-u", SRC_USER,
        f"-p{SRC_PASSWORD}",
        SRC_SCHEMA,
        table,
    ]

    try:
        with open(dump_file, "wb") as fh:           # binary is faster than text
            result = subprocess.run(
                command,
                stdout=fh,
                stderr=subprocess.PIPE,
            )
        if result.returncode == 0:
            logger.info(f"Dumped  {table}  →  {dump_file}")
        else:
            logger.error(f"mysqldump failed for {table}: {result.stderr.decode()}")
    except Exception as exc:
        logger.error(f"Exception dumping {table}: {exc}")


def dump_all_tables() -> None:
    engine = create_engine(
        f"mysql+pymysql://{SRC_USER}:{SRC_PASSWORD}@{SRC_HOST}:{SRC_PORT}/{SRC_SCHEMA}",
        pool_size=2,
        max_overflow=2,
    )
    tables = inspect(engine).get_table_names()
    engine.dispose()

    logger.info(f"Found {len(tables)} tables to dump with {DUMP_THREADS} threads.")

    with ThreadPoolExecutor(max_workers=DUMP_THREADS) as executor:
        futures = {executor.submit(dump_table, t): t for t in tables}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Unhandled error dumping {futures[future]}: {exc}")

    logger.info("All tables dumped.")


# ---------------------------------------------------------------------------
# RESTORE  –  parallel, skip tables that already exist
# ---------------------------------------------------------------------------

def get_existing_tables(engine, database: str) -> set:
    """Fetch all already-restored table names in one single query."""
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"schema": database}).fetchall()
    return {row[0] for row in rows}


def restore_sql_file(sql_file_path: str, host: str, user: str, password: str,
                     database: str, existing_tables: set) -> None:
    table_name = os.path.splitext(os.path.basename(sql_file_path))[0]

    if table_name in existing_tables:
        logger.info(f"Skipping  {table_name}  (already exists)")
        return

    command = ["mysql", f"-h{host}", f"-u{user}", f"-p{password}", database]

    logger.info(f"Restoring  {table_name} ...")
    try:
        with open(sql_file_path, "rb") as fh:       # binary avoids encoding overhead
            result = subprocess.run(
                command,
                stdin=fh,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        if result.returncode == 0:
            logger.info(f"Restored  {table_name}  successfully.")
        else:
            logger.error(f"Error restoring {table_name}:\n{result.stderr.decode()}")
    except Exception as exc:
        logger.error(f"Exception restoring {sql_file_path}: {exc}")


def restore_all_sql_files(folder: str, host: str, user: str, password: str,
                          database: str, max_threads: int) -> None:
    engine = create_engine(
        f"mysql+pymysql://{user}:{quote_plus(password)}@{host}/{database}",
        pool_size=100,
        max_overflow=100,
    )

    # Pre-fetch all existing tables once — avoids N round-trips to information_schema
    existing_tables = get_existing_tables(engine, database)
    engine.dispose()
    logger.info(f"Tables already in destination: {len(existing_tables)}")

    sql_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.endswith(".sql")
    ]
    # Sort largest files first so long-running restores start early and threads
    # stay saturated throughout instead of waiting on stragglers at the end.
    sql_files.sort(key=os.path.getsize, reverse=True)
    logger.info(f"SQL files to restore: {len(sql_files)}  |  threads: {max_threads}")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(
                restore_sql_file, f, host, user, password, database, existing_tables
            ): f
            for f in sql_files
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Unhandled error restoring {futures[future]}: {exc}")

    logger.info("All SQL files processed.")


# ---------------------------------------------------------------------------
# PIPELINE  –  dump and restore concurrently via a producer-consumer queue
# ---------------------------------------------------------------------------

_SENTINEL = None   # poison pill that tells restore workers the queue is done


def _dump_worker(table: str, work_queue: queue.Queue) -> None:
    """Dump one table; on success push the .sql path onto the restore queue."""
    os.makedirs(DUMP_FOLDER, exist_ok=True)
    dump_file = os.path.join(DUMP_FOLDER, f"{table}.sql")

    command = [
        "mysqldump",
        "--default-character-set=utf8",
        "--no-tablespaces",
        "--single-transaction",
        "--skip-lock-tables",
        "--quick",
        "--compression-algorithms=zlib",
        "-h", SRC_HOST,
        "-P", SRC_PORT,
        "-u", SRC_USER,
        f"-p{SRC_PASSWORD}",
        SRC_SCHEMA,
        table,
    ]

    try:
        with open(dump_file, "wb") as fh:
            result = subprocess.run(command, stdout=fh, stderr=subprocess.PIPE)

        if result.returncode == 0:
            logger.info(f"Dumped    {table}  →  {dump_file}")
            work_queue.put(dump_file)          # signal restore workers immediately
        else:
            logger.error(f"mysqldump failed for {table}: {result.stderr.decode()}")
    except Exception as exc:
        logger.error(f"Exception dumping {table}: {exc}")


def _restore_worker(work_queue: queue.Queue, existing_tables: set,
                    host: str, user: str, password: str, database: str,
                    active_count: threading.Semaphore) -> None:
    """Continuously pull .sql paths from the queue and restore them."""
    while True:
        sql_file_path = work_queue.get()

        if sql_file_path is _SENTINEL:
            work_queue.task_done()
            break                              # all dumps done, exit

        table_name = os.path.splitext(os.path.basename(sql_file_path))[0]

        if table_name in existing_tables:
            logger.info(f"Skipping  {table_name}  (already exists)")
            work_queue.task_done()
            continue

        command = ["mysql", f"-h{host}", f"-u{user}", f"-p{password}", database]
        logger.info(f"Restoring {table_name} ...")
        try:
            with open(sql_file_path, "rb") as fh:
                result = subprocess.run(
                    command, stdin=fh,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
            if result.returncode == 0:
                logger.info(f"Restored  {table_name}  successfully.")
            else:
                logger.error(f"Error restoring {table_name}:\n{result.stderr.decode()}")
        except Exception as exc:
            logger.error(f"Exception restoring {sql_file_path}: {exc}")
        finally:
            work_queue.task_done()


def dump_and_restore_pipeline() -> None:
    """
    Runs dump and restore concurrently:
      - DUMP_THREADS workers dump tables and push finished .sql paths to a queue.
      - RESTORE_THREADS workers pick up .sql paths from the queue and restore them
        immediately, without waiting for all dumps to finish first.

    Wall-clock time ≈ max(total_dump_time, total_restore_time)
    instead of dump_time + restore_time.
    """
    # -- fetch table list & existing tables up front -------------------------
    src_engine = create_engine(
        f"mysql+pymysql://{SRC_USER}:{SRC_PASSWORD}@{SRC_HOST}:{SRC_PORT}/{SRC_SCHEMA}",
        pool_size=50, max_overflow=50,
    )
    tables = inspect(src_engine).get_table_names()
    src_engine.dispose()
    logger.info(f"Tables to dump: {len(tables)}")

    dst_engine = create_engine(
        f"mysql+pymysql://{DST_USER}:{quote_plus(DST_PASSWORD)}@{DST_HOST}/{DST_SCHEMA}",
        pool_size=50, max_overflow=50,
    )
    existing_tables = get_existing_tables(dst_engine, DST_SCHEMA)
    dst_engine.dispose()
    logger.info(f"Tables already in destination: {len(existing_tables)}")

    # -- shared queue between dump producers and restore consumers -----------
    work_queue: queue.Queue = queue.Queue(maxsize=DUMP_THREADS * 4)
    active_count = threading.Semaphore(0)   # unused placeholder for future throttle

    # -- start restore consumer threads first --------------------------------
    restore_threads = [
        threading.Thread(
            target=_restore_worker,
            args=(work_queue, existing_tables,
                  DST_HOST, DST_USER, DST_PASSWORD, DST_SCHEMA, active_count),
            daemon=True,
        )
        for _ in range(RESTORE_THREADS)
    ]
    for t in restore_threads:
        t.start()

    # -- run dump producers in a thread pool ---------------------------------
    with ThreadPoolExecutor(max_workers=DUMP_THREADS) as executor:
        futures = {executor.submit(_dump_worker, t, work_queue): t for t in tables}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Unhandled dump error for {futures[future]}: {exc}")

    # -- send one sentinel per restore thread to signal end-of-queue ---------
    for _ in restore_threads:
        work_queue.put(_SENTINEL)

    # -- wait for all restore workers to drain the queue ---------------------
    for t in restore_threads:
        t.join()

    logger.info("Pipeline complete — all tables dumped and restored.")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Choose one:

    # dump_all_tables()                   # dump only

    # restore_all_sql_files(              # restore only (from existing .sql files)
    #     folder=RESTORE_FROM,
    #     host=DST_HOST,
    #     user=DST_USER,
    #     password=DST_PASSWORD,
    #     database=DST_SCHEMA,
    #     max_threads=RESTORE_THREADS,
    # )

    dump_and_restore_pipeline()           # dump + restore concurrently
