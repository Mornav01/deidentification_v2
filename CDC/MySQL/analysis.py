#!/usr/bin/env python
"""Converted from analysis.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import pandas as pd

df = pd.read_csv("cdc_parser_log.csv")
df.shape

# %% [code cell 2]
df.head()

# %% [code cell 3]
df['runtime'].describe()

# %% [code cell 4]
sum(df['runtime'])/3600
