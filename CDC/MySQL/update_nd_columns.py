#!/usr/bin/env python
"""Converted from update_nd_columns.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
import mysql.connector
import pandas as pd
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     "localhost",
    "user":     os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASS", ""),
    "port":     3306,
}

TARGET_SCHEMAS = ["mobiledoc_apr26"]

NULL_DATE  = "2026-04-11 00:00:00"
NULL_FLAG  = "Y"

# %% [code cell 2]
def get_tables_from_schema(cursor):
    format_strings = ", ".join(["%s"] * len(TARGET_SCHEMAS))
    # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
    cursor.execute(f"""
        SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ({format_strings})
          AND COLUMN_NAME IN ('nd_extracted_at', 'nd_is_active')
    """, TARGET_SCHEMAS)
    return cursor.fetchall()


def column_exists(cursor, schema, table, column):
    cursor.execute("""
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (schema, table, column))
    return cursor.fetchone() is not None


def migrate_table(cursor, schema, table):
    # ── nd_extracted_at → nd_extracted_date DATETIME ─────────────────────────
    if column_exists(cursor, schema, table, "nd_extracted_at"):
        print(f"  [{schema}.{table}] Renaming nd_extracted_at → nd_extracted_date")
        # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query,python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cursor.execute(f"ALTER TABLE `{schema}`.`{table}` CHANGE COLUMN `nd_extracted_at` `nd_extracted_date` DATETIME")

        # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cursor.execute(f"""
            UPDATE `{schema}`.`{table}`
            SET `nd_extracted_date` = %s
            WHERE `nd_extracted_date` IS NULL
        """, (NULL_DATE,))
        print(f"  [{schema}.{table}] Updated {cursor.rowcount} NULL rows in nd_extracted_date")
    else:
        print(f"  [{schema}.{table}] nd_extracted_at not found, skipping")

    # ── nd_is_active → nd_ActiveFlag VARCHAR(10) ────────────────────────────
    if column_exists(cursor, schema, table, "nd_is_active"):
        print(f"  [{schema}.{table}] Renaming nd_is_active → nd_ActiveFlag")
        # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query,python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cursor.execute(f"ALTER TABLE `{schema}`.`{table}` CHANGE COLUMN `nd_is_active` `nd_ActiveFlag` VARCHAR(10)")

        # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cursor.execute(f"""
            UPDATE `{schema}`.`{table}`
            SET `nd_ActiveFlag` = %s
            WHERE `nd_ActiveFlag` IS NULL OR `nd_ActiveFlag` = 'Yes'
        """, (NULL_FLAG,))
        print(f"  [{schema}.{table}] Updated {cursor.rowcount} NULL rows in nd_ActiveFlag")
    else:
        print(f"  [{schema}.{table}] nd_is_active not found, skipping")

def migrate_table_threaded(schema, table):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("SET sql_mode = ''")

        print(f"Processing: {schema}.{table}")
        migrate_table(cursor, schema, table)

        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        print(f"Done: {schema}.{table}\n")

    except Error as e:
        print(f"Error on {schema}.{table}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# %% [code cell 3]
USE_MANUAL_LIST = False
MANUAL_TABLE_LIST = [('mobiledoc_apr26_staging', 'doctors'), ('mobiledoc_apr26', 'doctors')]

# %% [code cell 4]
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

if USE_MANUAL_LIST:
    tables = MANUAL_TABLE_LIST
    print(f"Using manual list — {len(tables)} table(s)\n")
else:
    tables = get_tables_from_schema(cursor)
    print(f"Discovered {len(tables)} table(s) via INFORMATION_SCHEMA\n")

cursor.close()
conn.close()

# %% [code cell 5]
tables

# %% [code cell 6]
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(migrate_table_threaded, schema, table): (schema, table) for schema, table in tables}
    for future in as_completed(futures):
        schema, table = futures[future]
        if future.exception():
            print(f"Failed: {schema}.{table} — {future.exception()}")
    
print("Done.")

# %% [code cell 7]
p0_df = pd.read_csv("/Users/ndaidcnd/Desktop/Air_DEID/airflow-automation/Airflow/input/deid_runner.csv")
tables = set(p0_df['table_name'].to_list())
len(tables), schema

# %% [code cell 8]
schema = 'mobiledoc_apr26_staging'
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(migrate_table_threaded, schema, table): (schema, table) for table in tables}
    for future in as_completed(futures):
        schema, table = futures[future]
        if future.exception():
            print(f"Failed: {schema}.{table} — {future.exception()}")
    
print("Done.")

# %% [code cell 9]
schema = 'deidentified'
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(migrate_table_threaded, schema, table): (schema, table) for table in tables}
    for future in as_completed(futures):
        schema, table = futures[future]
        if future.exception():
            print(f"Failed: {schema}.{table} — {future.exception()}")
    
print("Done.")
