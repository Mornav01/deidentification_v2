"""Exclusion gating for tables that carry ONLY the ENCOUNTER_ID rule.

Such tables have no patient-ID column at all, so the *only* route to a
de-identified patient is the encounter_mapping join. Dropping the excluded
patient from patient_mapping_table alone would leave those rows resolving fine,
because encounter_mapping carries nd_patient_id itself and the resolver
coalesces it.

This walks the production chain for that shape — the enrichment + batch joins
exactly as deid/tasks/process.py performs them, then PatientIdentifierResolver
and InvalidRowHandler — and asserts the excluded patient's row is rejected while
an ordinary patient's row survives.
"""

from __future__ import annotations

import polars as pl

from deid.core.ops_df.utility import join_dataframes
from deid.core.process_df.main import PatientIdentifierResolver
from deid.core.process_df.rowhandler import InvalidRowHandler

# ENCOUNTER_ID rule on `encounterid`; no PATIENT_* / referencepid / appointment / chart.
KEY_PHI_COLUMNS = (["encounterid"], {}, [], [], [])


def _join_as_process_does(df: pl.DataFrame, enc_df: pl.DataFrame, pat_df: pl.DataFrame):
    """Mirror deid/tasks/process.py's preloaded encounter path."""
    _enc = enc_df.rename({"nd_patient_id": "nd_patient_id_from_encounter_mapping"})
    enc_enriched = join_dataframes(
        _enc,
        pat_df,
        left_on="nd_patient_id_from_encounter_mapping",
        right_on="nd_patient_id",
        how="left",
        right_suffix="from_encounter_mapping",
        drop_right_join_column=False,
    )
    return join_dataframes(
        df,
        enc_enriched,
        left_on=KEY_PHI_COLUMNS[0][0],
        right_on="encounter_id",
        how="left",
        right_suffix="",
        drop_right_join_column=True,
    )


def _resolve_and_filter(df: pl.DataFrame, tmp_path) -> pl.DataFrame:
    resolver = PatientIdentifierResolver(
        KEY_PHI_COLUMNS, possible_patient_identifier_columns=["patientid"], offset_days=34
    )
    df = resolver.transform(df)
    handler = InvalidRowHandler(
        db_name="mobiledoc",
        table_name="enc_only_table",
        db_path=str(tmp_path / "failed_rows.db"),
    )
    return handler.handle(df)


def test_encounter_only_table_rejects_rows_of_excluded_patients(tmp_path):
    # Source batch: two clinical rows, keyed only by encounter id.
    df = pl.DataFrame({
        "encounterid": [5001, 5002],
        "note": ["ordinary patient", "excluded patient"],
    })

    # Mapping state AFTER the exclusion gating: patient 1002 is excluded, so
    # neither their patient_mapping row nor their encounter_mapping row is loaded.
    pat_df = pl.DataFrame({
        "nd_patient_id": [1001],
        "patientid": [101],
        "offset": [30],
    })
    enc_df = pl.DataFrame({
        "nd_patient_id": [1001],
        "encounter_id": [5001],
        "nd_encounter_id": [10010001],
    })

    joined = _join_as_process_does(df, enc_df, pat_df)
    result = _resolve_and_filter(joined, tmp_path)

    assert result.height == 1, "the excluded patient's row must not survive"
    assert result["note"].to_list() == ["ordinary patient"]
    assert result["_resolved_nd_patient_id"].to_list() == [1001]


def test_encounter_only_table_would_leak_if_only_patient_table_were_filtered(tmp_path):
    """Guards the reason the encounter rows have to be dropped too.

    Same batch, but with the excluded patient's encounter_mapping row still
    present (i.e. only patient_mapping_table had been filtered). The row then
    resolves through the encounter path and reaches the output — which is exactly
    what _drop_excluded_patient_mappings / the anti-join in
    _get_mapping_with_patient_join exist to prevent.
    """
    df = pl.DataFrame({
        "encounterid": [5001, 5002],
        "note": ["ordinary patient", "excluded patient"],
    })
    pat_df = pl.DataFrame({
        "nd_patient_id": [1001],
        "patientid": [101],
        "offset": [30],
    })
    enc_df = pl.DataFrame({
        "nd_patient_id": [1001, 1002],       # 1002 NOT dropped
        "encounter_id": [5001, 5002],
        "nd_encounter_id": [10010001, 10020001],
    })

    joined = _join_as_process_does(df, enc_df, pat_df)
    result = _resolve_and_filter(joined, tmp_path)

    assert result.height == 2
    assert sorted(result["_resolved_nd_patient_id"].to_list()) == [1001, 1002]
