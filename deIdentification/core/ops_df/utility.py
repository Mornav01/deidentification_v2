import pandas as pd
from typing import List, Optional


class DistinctValueFetcher:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_distinct_values(self, column: str) -> List:
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        return self.df[column].dropna().unique().tolist()


def join_dataframes(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_on: str,
    right_on: Optional[str] = None,
    how: str = "left",
    right_suffix: Optional[str] = None,
    drop_left_join_column: bool = False,
    drop_right_join_column: bool = False,
) -> pd.DataFrame:
    """
    Joins two dataframes with optional suffix for right_df columns and ability to drop join columns.

    Args:
        left_df (pd.DataFrame): Left/original dataframe.
        right_df (pd.DataFrame): Right/reference dataframe.
        left_on (str): Column name in left_df to join on.
        right_on (Optional[str]): Column name in right_df to join on. Defaults to left_on.
        how (str): Type of join (default = "left").
        right_suffix (Optional[str]): Suffix to append to right_df column names.
        drop_left_join_column (bool): Whether to drop the left join column after merge.
        drop_right_join_column (bool): Whether to drop the right join column after merge.

    Returns:
        pd.DataFrame: Joined dataframe.
    """
    if right_on is None:
        right_on = left_on

    # Ensure columns used for join are of compatible types
    left_df[left_on] = pd.to_numeric(left_df[left_on], errors="coerce").astype("Int64")
    right_df[right_on] = pd.to_numeric(right_df[right_on], errors="coerce").astype("Int64")

    # Optional: Print or log dtypes for debugging
    # print(f"Joining on left_df[{left_on}] (dtype: {left_df[left_on].dtype}) "
    #       f"and right_df[{right_on}] (dtype: {right_df[right_on].dtype})")

    # Apply suffixes to right_df columns if provided
    if right_suffix:
        right_df = right_df.rename(columns={
            col: f"{col}_{right_suffix}" for col in right_df.columns
        })
        right_on = f"{right_on}_{right_suffix}"

    # Perform join
    joined_df = pd.merge(left_df, right_df, how=how, left_on=left_on, right_on=right_on)

    # Drop join columns if requested
    if drop_left_join_column and left_on in joined_df.columns:
        joined_df.drop(columns=[left_on], inplace=True)

    if drop_right_join_column and right_on in joined_df.columns:
        joined_df.drop(columns=[right_on], inplace=True)

    return joined_df
