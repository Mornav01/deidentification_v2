#!/usr/bin/env python
"""Converted from db_stats.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
from sqlalchemy import create_engine, inspect, text
import pandas as pd

# Connect to Database
engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/deidentified_oct")
# engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/mobiledoc_staging_1d_deidentified")

# %% [code cell 2]
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
df.shape

# %% [code cell 3]
df.head()

# %% [code cell 4]
df[df['row_count'] > 0]
# df[df['table_name'] == 'prisma_ccd_document_section']
# .to_csv("cdc_tables.csv", index=False)

# %% [code cell 5]
df['row_count'].describe()

# %% [code cell 6]
tables = list(df[df['row_count'] > 0]['table_name'])
len(tables)

# %% [markdown cell 7]
# <h1>Data Merge QC</h1>

# %% [code cell 8]
from sqlalchemy import create_engine, inspect, text
import pandas as pd

# Connect to Database
engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/deidentified_oct")

# %% [code cell 9]
# Get Table Statistics (Row Count, Column Count)
inspector = inspect(engine)
stats = []

with engine.connect() as connection:
    for table in tables:
        # Get Row Count
        row_count_query = text(f"SELECT COUNT(*) AS row_count FROM `{table}` WHERE nd_operation IS NOT NULL")
        try:
            row_count = connection.execute(row_count_query).scalar()

            # Get Column Count
            columns = inspector.get_columns(table)
            column_count = len(columns)
        except:
            row_count = None
            column_count = None

        stats.append({"table_name": table, "row_count": row_count, "column_count": column_count})

prod_df = pd.DataFrame(stats)
prod_df.shape

# %% [code cell 10]
prod_df

# %% [code cell 11]
final = df.merge(prod_df, on=['table_name', 'row_count', 'column_count'], how='inner')
final.shape

# %% [code cell 12]
final
