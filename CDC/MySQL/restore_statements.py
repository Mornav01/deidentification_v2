#!/usr/bin/env python
"""Converted from restore_statements.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime
from collections import defaultdict
import json

# Connect to Database
cdc_engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/cdc")
staging_engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/mobiledoc_staging")
prod_engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/mobiledoc_oct")

# %% [code cell 2]
# nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
sql_statements = cdc_engine.connect().execute(text(f"SELECT * FROM cdc_change_log where table_name = 'enc' order by id")).fetchall()
len(sql_statements)

# %% [code cell 3]
# sql_statements[:5]

# %% [code cell 4]
with staging_engine.connect() as conn:
    print("🔍 Loading all column metadata...")
    existing_cols = conn.execute(text("""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'mobiledoc_staging'
    """)).fetchall()

# Build a dictionary: { table_name: set(columns) }
table_columns = defaultdict(set)
for tname, cname in existing_cols:
    table_columns[tname].add(cname)

print(f"✅ Cached column metadata for {len(table_columns)} tables")

# Now run alter only where missing
with staging_engine.begin() as conn:
    # for (table_name,) in conn.execute(text("SHOW TABLES")).fetchall():
    for table_name in ['enc']:
        print(f"⏳ Checking table: {table_name}")
        for col_name, col_def in [
            ("nd_created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
            ("nd_updated_at", "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
            ("nd_operation", "VARCHAR(100)"),
            ("nd_condition", "TEXT")
        ]:
            if col_name in table_columns[table_name]:
                continue  # already present

            alter_sql = f"""
                ALTER TABLE `{table_name}`
                ADD COLUMN `{col_name}` {col_def}
            """
            try:
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                conn.execute(text(alter_sql))
                print(f"✅ Added {col_name} to {table_name}")
                table_columns[table_name].add(col_name)
            except Exception as e:
                print(f"⚠️ Skipped {table_name}.{col_name}: {e}")

# %% [code cell 5]
def detect_insert_format(sql: str):
    s = sql.upper()
    if " VALUES" in s:
        return "VALUES"
    if " SET " in s:
        return "SET"
    raise None

def find_matching_paren(text, start_index):
    """Return the index of the matching closing parenthesis."""
    depth = 0
    for i in range(start_index, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("Unbalanced parentheses")

def parse_values_format(sql):
    # Remove trailing semicolon
    sql = sql.strip().rstrip(";")
    upper = sql.upper()

    # Extract columns list using parenthesis matching
    col_start = upper.find("(")
    col_end   = find_matching_paren(sql, col_start)
    columns = sql[col_start+1:col_end].strip()

    # Extract values list
    val_start = upper.find("VALUES")  
    val_start = upper.find("(", val_start)
    val_end   = find_matching_paren(sql, val_start)
    values = sql[val_start+1:val_end].strip()

    return columns, values

def parse_set_format(sql):
    upper = sql.upper()
    set_pos = upper.find(" SET ")
    if set_pos == -1:
        raise ValueError("SET keyword not found")

    # Everything after SET
    segment = sql[set_pos + len(" SET "):].strip()

    columns = []
    values = []

    current = ""
    inside_quotes = False
    escaped = False

    # 1️⃣ Tokenize on commas, but only if not inside quotes
    tokens = []
    for ch in segment:
        if ch == "\\" and not escaped:
            escaped = True
            current += ch
            continue

        if ch == "'" and not escaped:
            inside_quotes = not inside_quotes
            current += ch
            continue

        if ch == "," and not inside_quotes:
            tokens.append(current.strip())
            current = ""
        else:
            current += ch

        escaped = False

    if current:
        tokens.append(current.strip())

    # 2️⃣ Split key=value in each token
    for tok in tokens:
        if "=" in tok:
            key, val = tok.split("=", 1)
        elif ":" in tok:
            key, val = tok.split(":", 1)
        else:
            continue  # malformed

        key = key.strip()
        val = val.strip()

        # 3️⃣ Normalize NULL
        if val.lower() == "null":
            val = "NULL"

        # 4️⃣ Normalize escaped quotes
        if val.startswith("\\'") and val.endswith("\\'"):
            val = "'" + val[2:-2] + "'"

        columns.append(f"`{key}`")
        values.append(val)

    return ", ".join(columns), ", ".join(values)

def append_audit(columns, values, next_id, op, condition=None):
    columns += ", `nd_auto_increment_id`, `nd_created_at`, `nd_updated_at`, `nd_operation`, `nd_condition`"
    if condition:
        values  += f", {next_id}, NOW(), NOW(), '{op}', {repr(condition)}"
    else:
        values  += f", {next_id}, NOW(), NOW(), '{op}', NULL"
    return columns, values

def build_final_insert(table_name, columns, values):
    return f"""
        INSERT INTO mobiledoc_staging.{table_name}
        ({columns})
        VALUES ({values})
    """

# %% [code cell 6]
BATCH_SIZE = 1000
stats = {"inserted": 0, "updated": 0, "errors": 0}
nd_counter = {}  # table_name -> max_nd_auto_increment_id

prod_conn = prod_engine.connect()
staging_conn = staging_engine.connect()
staging_conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
staging_conn.execute(text("SET GLOBAL sql_mode = REPLACE(@@GLOBAL.sql_mode, 'NO_ZERO_DATE', '');"))
staging_conn.execute(text("SET GLOBAL sql_mode = REPLACE(@@GLOBAL.sql_mode, 'STRICT_TRANS_TABLES', '');"))

for i, row in enumerate(sql_statements, 1):
    table_name = row[1]
    op = row[2]
    sql = json.loads(row[3])['raw_sql']

    try:
        # Initialize per-table nd_auto_increment counter once
        if table_name not in nd_counter:
            max_nd = prod_conn.execute(
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                text(f"SELECT COALESCE(MAX(nd_auto_increment_id), 0) FROM `{table_name}`")
            ).scalar() or 0
            nd_counter[table_name] = int(max_nd)

        if op == 'UPDATE':
            where_index = sql.lower().rfind('where')
            if where_index == -1:
                continue
            condition = sql[where_index + 5:].strip()

            # fetch data from prod
            query = f"SELECT * FROM `{table_name}` WHERE {condition}"
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            data = prod_conn.execute(text(query)).fetchall()

            if data:
                enriched_data = []
                for row_data in data:
                    row_data = list(row_data)
                    row_data.extend([datetime.now(), datetime.now(), op, condition])
                    enriched_data.append(tuple(row_data))

                num_cols = len(enriched_data[0])
                placeholders = ", ".join(["%s"] * num_cols)
                insert_sql = f"INSERT IGNORE INTO `{table_name}` VALUES ({placeholders})"
                cursor = staging_conn.connection.cursor()
                cursor.executemany(insert_sql, enriched_data)
                cursor.close()
                stats["inserted"] += len(enriched_data)

            final_sql = sql.replace("%", "%%").replace(":", "\:")
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            staging_conn.execute(text(final_sql))
            stats["updated"] += 1

        elif op == 'INSERT':
            nd_counter[table_name] += 1
            next_id = nd_counter[table_name]

            fmt = detect_insert_format(sql)

            if fmt == "VALUES":
                try:
                    columns, values = parse_values_format(sql)
                except ValueError:
                    print(f"⚠️ Skipping malformed INSERT inside VALUES format: {sql}")
                    continue
            elif fmt == "SET":
                try:
                    columns, values = parse_set_format(sql)
                except ValueError:
                    print(f"⚠️ Skipping malformed INSERT inside SET format: {sql}")
                    continue
            else:
                print("⚠️ Unknown INSERT format, skipping:", sql)
                continue

            # Append audit fields
            columns, values = append_audit(columns, values, next_id, op)

            # Build final SQL
            final_sql = build_final_insert(table_name, columns, values).replace("%", "%%").replace(":", "\:")
            # print("Final SQL →", final_sql)

            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            staging_conn.execute(text(final_sql))
            stats["inserted"] += 1

    except SQLAlchemyError as e:
        print(f"[ERROR] {table_name} - {e}")
        stats["errors"] += 1

    if i % BATCH_SIZE == 0:
        staging_conn.commit()
        print(f"✅ Batch {i}: {stats}")

staging_conn.commit()
staging_conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
prod_conn.close()
staging_conn.close()

print("🎯 CDC sync complete.")
print(stats)
