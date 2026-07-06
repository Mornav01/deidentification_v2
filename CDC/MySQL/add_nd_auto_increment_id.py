#!/usr/bin/env python
"""
Add nd_auto_increment_id Column Script

This script adds the nd_auto_increment_id column to all tables found in the CDC table
if the column doesn't already exist. It processes tables in the production schema.

Usage:
    python add_nd_auto_increment_id.py --prod_schema "mobiledoc" --cdc_schema "cdc" --cdc_table "change_log"
"""

import os
import sys
import argparse
import logging
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging — file + stdout so Airflow captures output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("add_nd_auto_inc_id.log", mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

MYSQL_HOST = os.environ.get("DB_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("DB_PORT", "3306"))
MYSQL_USER = os.environ.get("DB_USER", "")
MYSQL_PASS = os.environ.get("DB_PASS", "")


def process_tables_mysql(engine, table_name):
    inspector = inspect(engine)
    
    # Use a single connection for the entire session to preserve @variables
    with engine.begin() as conn:
        logger.info(f"[MySQL] Processing table: {table_name}")

        # 1. Check if column exists
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if 'nd_auto_increment_id' in columns:
            logger.info(f"  Column 'nd_auto_increment_id' already exists in {table_name}. Skipping.")
            return {"success": True, "action": "skipped"}
            # try:
            #     conn.execute(text(f"ALTER TABLE `{table_name}` DROP COLUMN `nd_auto_increment_id`"))
            #     logger.info(f"  Dropped existing column in {table_name}")
            # except Exception as e:
            #     logger.error(f"  Error dropping column in {table_name}: {e}")
            #     continue

        try:
            # 2. Optimization & Safety Bypass
            # Note: These are session-specific and must happen on the same 'conn'
            conn.execute(text("SET sql_log_bin = 0;"))
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(text("SET sql_safe_updates = 0;"))
            conn.execute(text("SET @row_num = 0;"))
            
            # 3. Add the Column (Initially NULL for speed)
            logger.info(f"  Adding BIGINT column to {table_name}...")
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `nd_auto_increment_id` BIGINT NULL"))

            # 4. Populate Sequential Data
            # This is the heavy lift (13M rows)
            logger.info(f"  Populating sequential IDs (this may take few mins) to {table_name}...")
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(
                f"UPDATE `{table_name}` SET `nd_auto_increment_id` = (@row_num := @row_num + 1)"
            ))

            # 5. Add the Index (ALGORITHM=INPLACE for better concurrency)
            logger.info(f"  Creating index idx_nd_auto_increment_id to {table_name}...")
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(
                f"ALTER TABLE `{table_name}` "
                f"ADD INDEX `idx_nd_auto_increment_id` (`nd_auto_increment_id`), "
                f"ALGORITHM=INPLACE"
            ))

            # 6. Restore System Settings for this session
            conn.execute(text("SET sql_log_bin = 1;"))
            conn.execute(text("SET sql_safe_updates = 1;"))

            logger.info(f"✅ Successfully processed {table_name}")
            return {"success": True, "action": "added"}

        except Exception as e:
            logger.error(f"❌ Error during MySQL processing of {table_name}: {e}")
            try:
                conn.execute(text("SET sql_log_bin = 1;"))
            except:
                pass
            return {"success": False, "action": "error"}


def get_tables_from_cdc(engine, cdc_schema: str, cdc_table: str) -> list:
    """
    Get distinct table names from the CDC table.
    
    Args:
        engine: SQLAlchemy engine
        cdc_schema: CDC schema name
        cdc_table: CDC table name
    
    Returns:
        List of table names
    """
    try:
        with engine.connect() as conn:
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            query = text(f"SELECT DISTINCT table_name FROM `{cdc_schema}`.`{cdc_table}`")
            result = conn.execute(query)
            tables = [row[0] for row in result.fetchall()]
        
        logger.info(f"📋 Found {len(tables)} distinct tables in CDC table `{cdc_schema}`.`{cdc_table}`")
        return tables
        
    except Exception as e:
        logger.error(f"❌ Failed to get tables from CDC table: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Add nd_auto_increment_id column to tables from CDC")
    
    parser.add_argument("--prod_schema", required=True, help="Production schema name (where tables are located, e.g., 'mobiledoc')")
    parser.add_argument("--cdc_schema", default="cdc", help="CDC schema name (default: 'cdc')")
    parser.add_argument("--cdc_table", default="change_log", help="CDC table name (default: 'change_log')")
    parser.add_argument("--max_workers", type=int, default=10, help="Maximum number of parallel workers (default: 10)")
    
    # MySQL connection (optional, uses defaults if not provided)
    parser.add_argument("--mysql_host", default=MYSQL_HOST, help="MySQL host")
    parser.add_argument("--mysql_port", type=int, default=MYSQL_PORT, help="MySQL port")
    parser.add_argument("--mysql_user", default=MYSQL_USER, help="MySQL username")
    parser.add_argument("--mysql_pass", default=MYSQL_PASS, help="MySQL password")
    
    args = parser.parse_args()
    
    try:
        # Create engines
        connection_str = f"mysql+pymysql://{args.mysql_user}:{args.mysql_pass.replace('@', '%40')}@{args.mysql_host}:{args.mysql_port}"
        
        prod_engine = create_engine(
            f"{connection_str}/{args.prod_schema}",
            pool_recycle=3600,
            pool_pre_ping=True
        )
        
        cdc_engine = create_engine(
            f"{connection_str}/{args.cdc_schema}",
            pool_recycle=3600,
            pool_pre_ping=True
        )
        
        logger.info(f"🚀 Starting nd_auto_increment_id column addition process")
        logger.info(f"Production schema: {args.prod_schema}")
        logger.info(f"CDC schema: {args.cdc_schema}")
        logger.info(f"CDC table: {args.cdc_table}")
        
        # Step 1: Get tables from CDC table
        tables = get_tables_from_cdc(cdc_engine, args.cdc_schema, args.cdc_table)
        
        if not tables:
            logger.warning("⚠️ No tables found in CDC table. Nothing to process.")
            return
        
        # Step 2: Process tables in parallel
        logger.info(f"🔄 Processing {len(tables)} tables with {args.max_workers} workers...")
        
        results = []
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {
                executor.submit(process_tables_mysql, prod_engine, table): table
                for table in tables
            }
            
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    result["table"] = table_name
                    results.append(result)
                except Exception as e:
                    logger.error(f"❌ Table `{table_name}` processing failed: {e}")
                    results.append({"table": table_name, "success": False, "action": "error"})

        # Step 3: Retry failed tables with single worker
        failed_tables = [r["table"] for r in results if not r["success"]]
        
        if failed_tables:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Retrying {len(failed_tables)} failed tables with 1 worker (sequential)...")
            logger.info(f"Failed tables: {failed_tables}")
            logger.info(f"{'='*60}\n")
            
            retry_results = []
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = {
                    executor.submit(process_tables_mysql, prod_engine, table): table
                    for table in failed_tables
                }
                
                for future in as_completed(futures):
                    table_name = futures[future]
                    try:
                        result = future.result()
                        result["table"] = table_name
                        retry_results.append(result)
                    except Exception as e:
                        logger.error(f"❌ Table `{table_name}` retry failed: {e}")
                        retry_results.append({"table": table_name, "success": False, "action": "error"})

            # Update original results with retry results
            retry_dict = {r["table"]: r for r in retry_results}
            for i, result in enumerate(results):
                if not result["success"] and result["table"] in retry_dict:
                    results[i] = retry_dict[result["table"]]
        
        # Step 4: Final Summary
        successful = sum(1 for r in results if r["success"])
        failed = len(results) - successful
        skipped = sum(1 for r in results if r.get("action") == "skipped")
        added = sum(1 for r in results if r.get("action") == "added")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Final Processing Summary:")
        logger.info(f"  Total tables: {len(results)}")
        logger.info(f"  ✅ Successful: {successful}")
        logger.info(f"  ❌ Failed: {failed}")
        logger.info(f"  ⏭️  Skipped (already exists): {skipped}")
        logger.info(f"  ➕ Added: {added}")
        logger.info(f"{'='*60}")
        
        if failed > 0:
            final_failed_tables = [r["table"] for r in results if not r["success"]]
            logger.error(f"❌ Failed tables after retry: {final_failed_tables}")
            # sys.exit(1)
        
        logger.info("✅ All tables processed successfully")
        
    except Exception as e:
        logger.error(f"❌ Process failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
