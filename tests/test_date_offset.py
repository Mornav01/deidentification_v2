"""Tests for DateOffsetRule MySQL date-range clamping."""
import polars as pl
import pytest


def _apply_offset(source_date: str, offset_days: int) -> str:
    """Run DateOffsetRule on a single-row DataFrame and return the result string."""
    from deid.core.process_df.rules import DateOffsetRule

    rule = DateOffsetRule()
    df = pl.DataFrame({
        "datecol": [source_date],
        "_resolved_offset": [offset_days],
    })
    column_config = {
        "column_name": "datecol",
        "de_identification_rule": "DATAOFFSET",
        "is_phi": True,
    }
    result_df = rule.apply(df, column_config)
    return result_df["datecol"][0]


def test_date_offset_clamps_beyond_year_9999():
    """A date that shifts past year 9999 must be clamped to MySQL max, not produce +10000-..."""
    result = _apply_offset("9999-12-20 00:00:00", offset_days=365)
    assert result == "9999-12-31 23:59:59", f"Expected clamped max, got {result!r}"
    assert not result.startswith("+"), f"Year overflow not clamped: {result!r}"


def test_date_offset_far_future_sentinel_clamped():
    """Source value already beyond MySQL range (e.g. year 10000 MSSQL sentinel) → clamped."""
    # Polars can parse ISO strings with year > 9999 if passed correctly;
    # simulate by using the largest valid Polars datetime string we can construct.
    # The real-world trigger is MSSQL storing +10000-01-30 — we test that if it
    # reaches the rule it is safely clamped.
    result = _apply_offset("9999-12-31 23:59:58", offset_days=2)
    assert result == "9999-12-31 23:59:59", f"Expected clamped max, got {result!r}"


def test_date_offset_normal_date_unaffected():
    """A normal date within range must not be clamped."""
    result = _apply_offset("2020-06-15 00:00:00", offset_days=30)
    assert result == "2020-07-15 00:00:00", f"Unexpected result: {result!r}"


def test_date_offset_clamps_below_year_1000():
    """A date shifted before year 1000 is clamped to MySQL min."""
    result = _apply_offset("1000-01-05 00:00:00", offset_days=-10)
    assert result == "1000-01-01 00:00:00", f"Expected clamped min, got {result!r}"
