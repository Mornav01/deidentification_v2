#!/usr/bin/env python
"""Converted from drop_nd_condition.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

# Connect to Database
engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/mobiledoc_staging_1d_deidentified")

df = pd.read_csv("cdc_tables.csv")
tables = df['table_name'].to_list()
len(tables)

# %% [code cell 2]
# Now run alter only where missing
with engine.begin() as conn:
    for table_name in tables:

        alter_sql = f"""
            ALTER TABLE `{table_name}`
            DROP COLUMN `nd_condition`
        """
        try:
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(alter_sql))
            print(f"✅ Dropped nd_condition from {table_name}")
        except Exception as e:
            print(f"⚠️ Skipped {table_name}: {e}")
