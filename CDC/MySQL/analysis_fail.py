#!/usr/bin/env python
"""Converted from analysis_fail.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import pandas as pd
pd.set_option('display.max_colwidth', None)

df = pd.read_csv("failed_cases_10122025.csv")
df.shape

# %% [code cell 2]
df['table_name'].nunique()

# %% [code cell 3]
df.head()

# %% [code cell 4]
df['operation'].value_counts()

# %% [code cell 5]
udf = df[df['operation'] == 'UPDATE']
udf.shape

# %% [code cell 6]
udf['error'].value_counts()

# %% [code cell 7]
udf['table_name'].value_counts()

# %% [code cell 8]
ndf = df[df['type'] == 'errors_insert_none_fmt']
ndf.shape

# %% [code cell 9]
# ndf.to_csv("insert_select_statements.csv", index=False)
