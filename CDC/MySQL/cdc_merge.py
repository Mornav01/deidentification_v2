from sqlalchemy import create_engine, text, inspect
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd
import logging
import argparse

try:
    from datetime import UTC  # Python 3.11+
except ImportError:
    from datetime import timezone
    UTC = timezone.utc

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cdc_merge.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

start_time = datetime.now(UTC)

BATCH_SIZE = 10000
MAX_WORKERS = 5

staging_engine = None
prod_engine = None

generated_cols = defaultdict(set)
table_columns = defaultdict(list)


def parse_args():
    """
    Parse command-line arguments for CDC merge.

    Example:
        python cdc_merge.py --staging_schema "dent_staging" --prod_schema "dent"
    """
    parser = argparse.ArgumentParser(description="CDC merge: upsert from staging schema into prod schema")
    parser.add_argument(
        "--staging_schema",
        required=True,
        help='Staging schema to read from (e.g. "mobiledoc_staging")',
    )
    parser.add_argument(
        "--prod_schema",
        required=True,
        help='Prod schema to write into (e.g. "mobiledoc")',
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=4,
        help="Number of merge worker processes (default: auto-detected)",
    )
    return parser.parse_args()


def init_databases(staging_schema_arg: str, prod_schema_arg: str):
    """
    Initialise engines and metadata based on the supplied schemas.
    """
    global staging_schema, prod_schema, staging_engine, prod_engine
    global generated_cols, table_columns

    staging_schema = staging_schema_arg
    prod_schema = prod_schema_arg

    logger.info(f"Using staging_schema={staging_schema}, prod_schema={prod_schema}")

    _db_user = os.environ.get("DB_USER", "")
    _db_pass = os.environ.get("DB_PASS", "")
    _db_host = os.environ.get("DB_HOST", "localhost")
    _db_port = os.environ.get("DB_PORT", "3306")

    staging_engine = create_engine(
        f"mysql+pymysql://{_db_user}:{_db_pass}@{_db_host}:{_db_port}/{staging_schema}",
        pool_recycle=3600,
        pool_pre_ping=True,
    )

    prod_engine = create_engine(
        f"mysql+pymysql://{_db_user}:{_db_pass}@{_db_host}:{_db_port}/{prod_schema}",
        pool_recycle=3600,
        pool_pre_ping=True,
    )

    # Reset metadata holders
    generated_cols.clear()
    table_columns.clear()

    # Load generated column metadata
    with prod_engine.connect() as conn:
        logger.info("🔍 Loading generated column metadata...")
        gen_rows = conn.execute(text(f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{prod_schema}'
            AND (
                EXTRA LIKE '%VIRTUAL%' 
                OR EXTRA LIKE '%STORED%'
            )
        """)).fetchall()

    for table, col in gen_rows:
        generated_cols[table.lower()].add(col)

    logger.info(f"✅ Tables with generated columns: {len(generated_cols)}")

    # Load table column metadata
    with prod_engine.connect() as conn:
        logger.info("🔍 Loading column metadata...")
        rows = conn.execute(text(f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{prod_schema}'
            ORDER BY ORDINAL_POSITION
        """)).fetchall()

    for table, col in rows:
        table_columns[table.lower()].append(col)

    logger.info(f"✅ Cached column metadata for {len(table_columns)} tables")

CDC_COLS = [
    ("nd_extracted_date",        "DATETIME DEFAULT NULL"),
    ("nd_updated_at",        "DATETIME DEFAULT NULL"),
    ("nd_operation",         "VARCHAR(6)"),
    ("nd_ActiveFlag",         "VARCHAR(1)"),
]

def ensure_cdc_columns_for_table(conn, table_name, table_columns):
    t = table_name.lower()

    # If table metadata not cached
    if t not in table_columns:
        logger.warning(f"⚠️ Table metadata not found for {table_name}")
        return

    # Disable FK checks for this session
    conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))

    for col_name, col_def in CDC_COLS:
        if col_name in table_columns[t]:
            continue

        alter_sql = f"""
            ALTER TABLE `{table_name}`
            ADD COLUMN `{col_name}` {col_def}
        """

        try:
            conn.execute(text(alter_sql))
            table_columns[t].append(col_name)
            logger.info(f"➕ Added {col_name} to {table_name}")
        except Exception as e:
            orig = getattr(e, "orig", None)
            orig_code = orig.args[0] if (orig and hasattr(orig, "args") and orig.args) else None
            if orig_code == 1118 and "VARCHAR" in col_def.upper():
                # Row too wide for VARCHAR — retry with TEXT (stored off-page, no row-size cost)
                try:
                    conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` TEXT"))
                    table_columns[t].append(col_name)
                    logger.warning(f"⚠️ Added {table_name}.{col_name} as TEXT (row too wide for {col_def})")
                except Exception as e2:
                    logger.warning(f"⚠️ Skipped adding {table_name}.{col_name}: {e2}")
            else:
                logger.warning(f"⚠️ Skipped adding {table_name}.{col_name}: {e}")

def remove_generated_columns(table_name, columns, values, generated_cols):
    gen_set = generated_cols.get(table_name.lower())
    if not gen_set:
        return columns, values

    clean_cols, clean_vals = [], []
    for c, v in zip(columns, values):
        if c not in gen_set:
            clean_cols.append(c)
            clean_vals.append(v)

    return clean_cols, clean_vals

def load_staging_rows(engine, table):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM `{table}`"))
        return [dict(row) for row in result.mappings()]

def bulk_upsert_batch(conn, table, rows, generated_cols):
    if not rows:
        return

    clean_rows = []
    for r in rows:
        cols, vals = remove_generated_columns(
            table, list(r.keys()), list(r.values()), generated_cols
        )
        clean_rows.append(dict(zip(cols, vals)))

    cols = list(clean_rows[0].keys())

    col_sql = ", ".join(f"`{c}`" for c in cols)
    val_sql = ", ".join(f":{c}" for c in cols)

    # Update ALL columns except auto-increment PK
    update_cols = [c for c in cols if c != "nd_auto_increment_id"]
    update_sql = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)

    stmt = text(f"""
        INSERT INTO `{table}` ({col_sql})
        VALUES ({val_sql})
        ON DUPLICATE KEY UPDATE
            {update_sql}
    """)

    conn.execute(stmt, clean_rows)

def ensure_cdc_columns_serial(tables):
    logger.info("🧱 Ensuring CDC columns (serial phase)")

    with prod_engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
        for table in tables:
            # Ensure CDC columns exist for this table
            ensure_cdc_columns_for_table(conn, table, table_columns)

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def merge_table(staging_table, target_table):
    logger.info(f"🔄 Merging {staging_table} → {target_table}")
    t0 = datetime.now(UTC)

    rows = load_staging_rows(staging_engine, staging_table)
    if not rows:
        logger.info("No rows found")
        return

    with prod_engine.begin() as prod_conn:
        # Disable FK checks for this session
        prod_conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))

        # Single UPSERT handles:
        # - same nd_auto_increment_id
        # - same business UNIQUE KEY
        # - new rows
        for batch in chunked(rows, BATCH_SIZE):
            bulk_upsert_batch(
                prod_conn,
                target_table,
                batch,
                generated_cols
            )

    logger.info(
        f"✅ {staging_table} done | "
        f"rows:{len(rows)} | "
        f"{(datetime.now(UTC) - t0).total_seconds():.2f}s"
    )

def discover_staging_tables(engine):
    # Get Table Statistics (Row Count, Column Count)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    stats = []

    with engine.connect() as connection:
        for table in tables:
            # Get Row Count
            row_count_query = text(f"SELECT COUNT(*) AS row_count FROM `{table}`")
            row_count = connection.execute(row_count_query).scalar()

            # Get Column Count
            columns = inspector.get_columns(table)
            column_count = len(columns)

            stats.append({"table_name": table, "row_count": row_count, "column_count": column_count})

    df = pd.DataFrame(stats)
    df = df[df['row_count'] > 0]

    return list(df['table_name'].unique())

def prepare_mysql_environment(engine):
    """
    Runs required GLOBAL MySQL settings once.
    """
    with engine.begin() as conn:
        logger.info("⚙️ Adjusting MySQL GLOBAL sql_mode...")
        conn.execute(text(
            "SET GLOBAL sql_mode = REPLACE(@@GLOBAL.sql_mode, 'NO_ZERO_DATE', '')"
        ))
        conn.execute(text(
            "SET GLOBAL sql_mode = REPLACE(@@GLOBAL.sql_mode, 'STRICT_TRANS_TABLES', '')"
        ))

def main():
    args = parse_args()

    # Initialise DB connections and metadata with chosen schemas
    init_databases(args.staging_schema, args.prod_schema)

    logger.info("🚀 Starting CDC merge process")
    start = datetime.now(UTC)

    # MySQL global prep
    prepare_mysql_environment(prod_engine)

    # Discover tables
    tables = discover_staging_tables(staging_engine)
    # df = pd.read_csv("/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/input/deid_runner.csv", header=None, names=['table_name'])
    # tables = df['table_name'].to_list()

    if not tables:
        logger.info("No staging tables found")
        return
    logger.info(f"Total staging tables found with delta data: {len(tables)}")
    logger.info(f"Tables: {tables}")

    # ✅ SERIAL DDL (NO DEADLOCKS)
    ensure_cdc_columns_serial(tables)

    num_workers = args.max_workers
    logger.info(f"⚡ Running merges in parallel (workers={num_workers})")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(merge_table, table, table): table
            for table in tables
        }

        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.exception(f"❌ Merge failed for {table}: {e}")

    logger.info(
        f"🏁 CDC merge completed in "
        f"{(datetime.now(UTC) - start).total_seconds():.2f}s"
    )

    # Trigger A — delta-identity QC after the merge (opt-in via DEID_CDC_DELTA_QC=1).
    _run_post_merge_delta_qc(tables)


def _run_post_merge_delta_qc(tables):
    """Run the delta-identity QC (source=prod ↔ dest=staging) after a CDC merge.

    Opt-in and fail-soft: enabled only when ``DEID_CDC_DELTA_QC=1`` so existing CDC runs are
    unaffected, and never raises (QC must not corrupt a completed merge). Results persist to
    ``DEID_QC_RESULTS_DB`` (default ./cdc_delta_qc_results.db).
    """
    if os.environ.get("DEID_CDC_DELTA_QC", "").strip() not in ("1", "true", "True"):
        return
    try:
        from deid.qc.delta_identity import DeltaIdentityConfig, run_delta_identity_qc

        src = prod_engine.url.render_as_string(hide_password=False)
        dst = staging_engine.url.render_as_string(hide_password=False)
        cfg = DeltaIdentityConfig(
            tables=list(tables),
            delta_after=os.environ.get("DEID_CDC_DELTA_AFTER") or None,
        )
        results = run_delta_identity_qc(
            source_conn_str=src, dest_conn_str=dst, cfg=cfg,
            qc_results_db_url=os.environ.get("DEID_QC_RESULTS_DB", "./cdc_delta_qc_results.db"),
        )
        n_fail = sum(1 for r in results if not r["is_qc_passed"])
        logger.info("[DeltaQC] Post-merge check: %d table(s), %d failing.", len(results), n_fail)
    except Exception as exc:
        logger.exception("[DeltaQC] Post-merge delta-identity QC errored (non-fatal): %s", exc)


if __name__ == "__main__":
    main()