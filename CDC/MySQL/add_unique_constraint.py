#!/usr/bin/env python
"""
Add Unique Constraint Script

This script adds UNIQUE constraint on nd_auto_increment_id column for all tables in a schema.
If duplicates exist, it deduplicates them first (keeping the oldest record based on nd_extracted_date).

Usage:
    python add_unique_constraint.py --schema "deidentified"
"""

import os
import sys
import argparse
import logging
from sqlalchemy import create_engine, text, inspect

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Hardcoded MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "ndadmin"
MYSQL_PASS = "ndADMIN@2025"


def check_column_exists(conn, schema: str, table_name: str) -> bool:
    """
    Check if nd_auto_increment_id column exists in the table.
    
    Args:
        conn: Database connection
        schema: Schema name
        table_name: Table name
    
    Returns:
        True if column exists, False otherwise
    """
    try:
        result = conn.execute(
            text("""
                SELECT 1
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
                  AND COLUMN_NAME = 'nd_auto_increment_id'
                LIMIT 1
            """),
            {"schema": schema, "table": table_name}
        ).fetchone()
        
        return result is not None
    except Exception as e:
        logger.error(f"❌ Error checking column existence for `{schema}`.`{table_name}`: {e}")
        return False


def check_unique_exists(conn, schema: str, table_name: str) -> bool:
    """
    Check if UNIQUE constraint already exists on nd_auto_increment_id column.
    
    Args:
        conn: Database connection
        schema: Schema name
        table_name: Table name
    
    Returns:
        True if UNIQUE constraint exists, False otherwise
    """
    try:
        result = conn.execute(
            text("""
                SELECT 1
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
                  AND COLUMN_NAME = 'nd_auto_increment_id'
                  AND NON_UNIQUE = 0
                LIMIT 1
            """),
            {"schema": schema, "table": table_name}
        ).fetchone()
        
        return result is not None
    except Exception as e:
        logger.error(f"❌ Error checking UNIQUE constraint for `{schema}`.`{table_name}`: {e}")
        return False


def deduplicate_keep_one(conn, schema: str, table_name: str) -> int:
    """
    Deduplicate rows by keeping the oldest record (based on nd_extracted_date) for each nd_auto_increment_id.
    
    Args:
        conn: Database connection
        schema: Schema name
        table_name: Table name
    
    Returns:
        Number of duplicate rows removed
    """
    try:
        logger.info(f"🧹 Deduplicating `{schema}`.`{table_name}`")
        
        # Check if nd_extracted_date column exists
        has_created_at = conn.execute(
            text("""
                SELECT 1
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
                  AND COLUMN_NAME = 'nd_extracted_date'
                LIMIT 1
            """),
            {"schema": schema, "table": table_name}
        ).fetchone()
        
        if not has_created_at:
            logger.warning(f"⚠️ Table `{schema}`.`{table_name}`: 'nd_extracted_date' column not found. Skipping deduplication.")
            return 0
        
        # Disable safe updates for deletion
        conn.execute(text("SET sql_safe_updates = 0"))
        
        # Delete duplicates, keeping the oldest record (lowest nd_extracted_date)
        sql = text(f"""
            DELETE t
            FROM `{schema}`.`{table_name}` t
            JOIN (
                SELECT
                    nd_auto_increment_id,
                    nd_extracted_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY nd_auto_increment_id
                        ORDER BY nd_extracted_date
                    ) AS rn
                FROM `{schema}`.`{table_name}`
            ) d
            ON t.nd_auto_increment_id = d.nd_auto_increment_id
            AND t.nd_extracted_date = d.nd_extracted_date
            WHERE d.rn > 1
        """)
        
        result = conn.execute(sql)
        deleted_count = result.rowcount
        
        # Re-enable safe updates
        conn.execute(text("SET sql_safe_updates = 1"))
        
        if deleted_count > 0:
            logger.info(f"🗑️ Table `{schema}`.`{table_name}`: Removed {deleted_count} duplicate rows")
        else:
            logger.info(f"✅ Table `{schema}`.`{table_name}`: No duplicates found")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"❌ Error deduplicating `{schema}`.`{table_name}`: {e}")
        # Re-enable safe updates in case of error
        try:
            conn.execute(text("SET sql_safe_updates = 1"))
        except:
            pass
        raise


def add_unique_constraint(conn, schema: str, table_name: str) -> bool:
    """
    Add UNIQUE constraint on nd_auto_increment_id column if it doesn't exist.
    If duplicates exist, deduplicates first.
    
    Args:
        conn: Database connection
        schema: Schema name
        table_name: Table name
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if column exists
        if not check_column_exists(conn, schema, table_name):
            logger.info(f"⏭️ Table `{schema}`.`{table_name}`: Column 'nd_auto_increment_id' does not exist. Skipping.")
            return True
        
        # Check if UNIQUE already exists
        if check_unique_exists(conn, schema, table_name):
            logger.info(f"✅ Table `{schema}`.`{table_name}`: UNIQUE constraint already exists")
            return True
        
        # Try to add UNIQUE constraint
        # If it fails due to duplicates, deduplicate and retry
        try:
            logger.info(f"🔧 Table `{schema}`.`{table_name}`: Adding UNIQUE constraint...")
            conn.execute(text(f"""
                ALTER TABLE `{schema}`.`{table_name}`
                ADD UNIQUE INDEX uniq_nd_auto_increment_id (nd_auto_increment_id)
            """))
            logger.info(f"✅ Table `{schema}`.`{table_name}`: UNIQUE constraint added successfully")
            return True
            
        except Exception as e:
            error_msg = str(e)
            # Check if error is due to duplicates
            if "Duplicate entry" in error_msg or "duplicate" in error_msg.lower():
                logger.warning(f"⚠️ Table `{schema}`.`{table_name}`: Duplicates detected. Deduplicating...")
                # Deduplicate and retry
                deduplicate_keep_one(conn, schema, table_name)
                
                # Retry adding UNIQUE constraint
                logger.info(f"🔧 Table `{schema}`.`{table_name}`: Retrying UNIQUE constraint after deduplication...")
                conn.execute(text(f"""
                    ALTER TABLE `{schema}`.`{table_name}`
                    ADD UNIQUE INDEX uniq_nd_auto_increment_id (nd_auto_increment_id)
                """))
                logger.info(f"✅ Table `{schema}`.`{table_name}`: UNIQUE constraint added successfully after deduplication")
                return True
            else:
                # Some other error
                logger.error(f"❌ Table `{schema}`.`{table_name}`: Failed to add UNIQUE constraint: {error_msg}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Table `{schema}`.`{table_name}`: Error - {e}")
        return False


def get_all_tables(conn, schema: str) -> list:
    """
    Get all table names from the schema.
    
    Args:
        conn: Database connection
        schema: Schema name
    
    Returns:
        List of table names
    """
    try:
        result = conn.execute(
            text("""
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :schema
            """),
            {"schema": schema}
        ).scalars().all()
        
        return list(result)
    except Exception as e:
        logger.error(f"❌ Failed to get tables from schema: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Add UNIQUE constraint on nd_auto_increment_id column")
    
    parser.add_argument("--schema", required=True, help="Schema name (e.g., 'deidentified')")
    
    # MySQL connection (optional, uses defaults if not provided)
    parser.add_argument("--mysql_host", default=MYSQL_HOST, help="MySQL host")
    parser.add_argument("--mysql_port", type=int, default=MYSQL_PORT, help="MySQL port")
    parser.add_argument("--mysql_user", default=MYSQL_USER, help="MySQL username")
    parser.add_argument("--mysql_pass", default=MYSQL_PASS, help="MySQL password")
    
    args = parser.parse_args()
    
    try:
        # Create engine
        connection_str = f"mysql+pymysql://{args.mysql_user}:{args.mysql_pass.replace('@', '%40')}@{args.mysql_host}:{args.mysql_port}"
        
        engine = create_engine(
            f"{connection_str}/{args.schema}",
            pool_recycle=3600,
            pool_pre_ping=True
        )
        
        logger.info(f"🚀 Starting UNIQUE constraint addition process")
        logger.info(f"Schema: {args.schema}")
        
        # Get all tables
        with engine.connect() as conn:
            tables = get_all_tables(conn, args.schema)
        
        logger.info(f"📋 Found {len(tables)} tables in schema `{args.schema}`")
        
        if not tables:
            logger.warning("⚠️ No tables found in schema. Nothing to process.")
            return
        
        # Process each table
        results = []
        total_duplicates_removed = 0
        
        with engine.begin() as conn:
            for table in tables:
                try:
                    # Try to add unique constraint (will deduplicate if needed)
                    success = add_unique_constraint(conn, args.schema, table)
                    results.append({
                        "table": table,
                        "success": success
                    })
                except Exception as e:
                    logger.error(f"❌ Table `{table}` processing failed: {e}")
                    results.append({
                        "table": table,
                        "success": False,
                        "error": str(e)
                    })
        
        # Summary
        successful = sum(1 for r in results if r["success"])
        failed = len(results) - successful
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Processing Summary:")
        logger.info(f"  Total tables: {len(results)}")
        logger.info(f"  ✅ Successful: {successful}")
        logger.info(f"  ❌ Failed: {failed}")
        logger.info(f"{'='*60}")
        
        if failed > 0:
            failed_tables = [r["table"] for r in results if not r["success"]]
            logger.error(f"❌ Failed tables: {failed_tables}")
            sys.exit(1)
        
        logger.info("✅ UNIQUE constraint added to all applicable tables")
        
    except Exception as e:
        logger.error(f"❌ Process failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
