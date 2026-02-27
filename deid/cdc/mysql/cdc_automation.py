import os
import subprocess
import logging
from datetime import datetime, timedelta

# ============================
# Logging
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Shared MySQL credentials/host (align with individual scripts)
MYSQL_USER = "ndadmin"
MYSQL_PASS = "ndADMIN@2025"
MYSQL_HOST = "localhost"


def run_step(cmd, step_name):
    """
    Run a single step as a subprocess and fail fast on error.
    """
    logger.info("=== Running step: %s ===", step_name)
    logger.info("Command: %s", " ".join(cmd))
    start = datetime.now()
    result = subprocess.run(cmd, check=False)
    duration = (datetime.now() - start).total_seconds()

    if result.returncode != 0:
        raise RuntimeError(f"Step '{step_name}' failed with exit code {result.returncode}")

    logger.info("Step '%s' completed in %.2fs", step_name, duration)


def preprocessing(run_date):
    """
    Placeholder for any preprocessing logic you want to add.
    Currently it just logs the run date.
    """
    logger.info("=== Preprocessing for run_date=%s ===", run_date)
    # TODO: Add real preprocessing here (e.g., cleanup temp tables, archive logs, etc.)


def truncate_cdc_table(table_name):
    """
    Truncate CDC table so each run_date starts fresh.
    """
    sql = f"TRUNCATE TABLE cdc.`{table_name}`;"
    cmd = ["mysql", f"-h{MYSQL_HOST}", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-e", sql]
    run_step(cmd, f"Truncate CDC table {table_name}")


def reset_staging_schema(staging_schema, prod_schema):
    """
    Drop and recreate staging schema from prod schema (structure only).
    """
    # Drop + create schema
    drop_create_sql = f"DROP SCHEMA IF EXISTS `{staging_schema}`; CREATE SCHEMA `{staging_schema}`;"
    run_step(
        ["mysql", f"-h{MYSQL_HOST}", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-e", drop_create_sql],
        f"Reset staging schema {staging_schema}",
    )

    # Restore structure from prod -> staging (schema only)
    dump_and_load = (
        f"mysqldump -h {MYSQL_HOST} -u {MYSQL_USER} -p'{MYSQL_PASS}' "
        f"--no-data --routines --triggers --set-gtid-purged=OFF {prod_schema} "
        f"| mysql -h {MYSQL_HOST} -u {MYSQL_USER} -p'{MYSQL_PASS}' {staging_schema}"
    )
    run_step(["bash", "-lc", dump_and_load], f"Clone structure {prod_schema} -> {staging_schema}")


def main():
    """
    End-to-end CDC automation with no external CLI arguments.

    Defaults:
      - run_date: yesterday (based on system date)
      - table_name: "change_log"
      - staging_schema: "mobiledoc_staging"
      - prod_schema: "mobiledoc_oct"
    """
    # --- Default configuration for this automation run ---
    # run_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_date = "2025-12-30"
    table_name = "change_log"
    staging_schema = "mobiledoc_staging"
    prod_schema = "mobiledoc_oct"

    # 1) Optional preprocessing step
    preprocessing(run_date)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # 2) Clean CDC table for this run_date
    truncate_cdc_table(table_name)

    # 3) Run cdc_parser.py with CLI parameters
    parser_script = os.path.join(base_dir, "cdc_parser.py")
    run_step(
        ["python", parser_script, "--table_name", table_name, "--run_date", run_date],
        step_name="CDC Parser",
    )

    # 4) Reset staging schema from prod schema (structure only)
    reset_staging_schema(staging_schema, prod_schema)

    # 5) Run downstream steps (restore + merge) – adjust as needed
    restore_script = os.path.join(base_dir, "cdc_restore.py")
    merge_script = os.path.join(base_dir, "cdc_merge.py")

    # If you want to skip any step, just comment it out.
    run_step(
        [
            "python",
            restore_script,
            "--run_date", run_date,
            "--table_name", table_name,
            "--staging_schema", staging_schema,
            "--prod_schema", prod_schema,
        ],
        step_name="CDC Restore",
    )
    run_step(
        [
            "python",
            merge_script,
            "--staging_schema", staging_schema,
            "--prod_schema", prod_schema,
        ],
        step_name="CDC Merge",
    )

    # # Run deidentification
    # run_step(
    #     [
    #         "python",
    #         deid_script,
    #         "--staging_schema", staging_schema,
    #         "--deid_schema", f"{staging_schema}_deidentified",
    #     ],
    #     step_name="Deidentification",
    # )

    # # Run deidentified data merge
    # run_step(
    #     [
    #         "python",
    #         merge_script,
    #         "--staging_schema", f"{staging_schema}_deidentified",
    #         "--prod_schema", "deidentified_oct",
    #     ],
    #     step_name="Deidentification",
    # )


if __name__ == "__main__":
    main()


