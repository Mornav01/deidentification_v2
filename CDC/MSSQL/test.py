#!/usr/bin/env python
"""Converted from test.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import pandas as pd
from parser import read_trn_log_file, parse_trn_rows
from users_schema import USERS_TABLE_SCHEMA

trn_path = "/var/opt/mssql/dump/20251017/PrimeRecord1697/PrimeRecord1697_LOG_20251012_200000.trn"
rows = read_trn_log_file(trn_path)
len(rows)

# %% [code cell 2]
df = pd.DataFrame(rows)
df.shape

# %% [code cell 3]
df.head()

# %% [code cell 4]
# USERS_TABLE_SCHEMA

# %% [code cell 5]
parsed = parse_trn_rows(rows, USERS_TABLE_SCHEMA)
len(parsed)

# %% [code cell 6]
parsed

# %% [code cell 7]
for i in parsed:
    if i['before'] and i['after']:
        print(parsed)
