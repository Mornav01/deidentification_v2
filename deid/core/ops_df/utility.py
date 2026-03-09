import polars as pl
from typing import List, Optional
from pydantic import validate_call


class DistinctValueFetcher:
    """Extract distinct non-null values from a Polars DataFrame column."""

    def __init__(self, df: pl.DataFrame):
        self.df = df

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_distinct_values(self, column: str) -> List:
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        return self.df[column].drop_nulls().unique().to_list()


@validate_call(config=dict(arbitrary_types_allowed=True))
def join_dataframes(
    left_df: pl.DataFrame,
    right_df: pl.DataFrame,
    left_on: str,
    right_on: Optional[str] = None,
    how: str = "left",
    right_suffix: Optional[str] = None,
    drop_left_join_column: bool = False,
    drop_right_join_column: bool = False,
) -> pl.DataFrame:
    """Join two Polars DataFrames with optional column renaming and key dropping.

    Performance vs. Pandas pd.merge():
    - Polars joins run in parallel on multiple CPU cores using Rust.
    - For the mapping-table joins in this pipeline (10 K–500 K row tables),
      expect 5–20× speed improvement over pandas.merge().

    Args:
        left_df:                Left / source DataFrame.
        right_df:               Right / reference DataFrame.
        left_on:                Column in left_df to join on.
        right_on:               Column in right_df to join on (defaults to left_on).
        how:                    Join type — "left", "inner", "outer", etc.
        right_suffix:           If provided, ALL right_df columns are renamed to
                                ``col + "_" + right_suffix`` before the join (matching
                                the original Pandas behaviour).
        drop_left_join_column:  Drop the left join key from the result.
        drop_right_join_column: Drop the right join key from the result.
                                Note: Polars left-join already excludes the right key
                                when left_on != right_on, so this is a safety guard.

    Returns:
        Joined pl.DataFrame.
    """
    if right_on is None:
        right_on = left_on

    # Harmonize join-key types: if either side is string-like, cast both to Utf8.
    # Otherwise cast both to Int64 for numeric compatibility.
    # strict=False silently turns un-castable values into null rather than raising.
    left_dtype = left_df[left_on].dtype
    right_dtype = right_df[right_on].dtype

    if left_dtype in (pl.Utf8, pl.Categorical) or right_dtype in (pl.Utf8, pl.Categorical):
        cast_type = pl.Utf8
    else:
        cast_type = pl.Int64

    left_df = left_df.with_columns(
        pl.col(left_on).cast(cast_type, strict=False)
    )
    right_df = right_df.with_columns(
        pl.col(right_on).cast(cast_type, strict=False)
    )

    # Optionally rename ALL right_df columns before joining.
    # This prevents column-name collisions and matches the Pandas suffix behaviour.
    right_on_actual = right_on
    if right_suffix:
        rename_map = {col: f"{col}_{right_suffix}" for col in right_df.columns}
        right_df = right_df.rename(rename_map)
        right_on_actual = f"{right_on}_{right_suffix}"

    # Perform the join.  Use suffix="_right" so any remaining name conflicts
    # (non-key columns with the same name in both frames) get a predictable suffix.
    joined_df = left_df.join(
        right_df,
        left_on=left_on,
        right_on=right_on_actual,
        how=how,
        suffix="_right",
    )

    # Drop join columns if requested.
    # In a Polars left-join where left_on != right_on_actual, the right key is
    # already excluded from the result; the guard below handles the symmetric case.
    if drop_left_join_column and left_on in joined_df.columns:
        joined_df = joined_df.drop(left_on)

    if drop_right_join_column and right_on_actual in joined_df.columns:
        joined_df = joined_df.drop(right_on_actual)

    return joined_df
