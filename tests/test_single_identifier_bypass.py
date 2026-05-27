"""Tests for the single-identifier bypass rule remapping in async_runner.py.

When identifier_columns has exactly one entry, PATIENT_ID and PATIENT_DOB in
the rules CSV are automatically remapped to PATIENT_{identifier} and DOB before
validation runs.
"""
import pytest
from unittest.mock import MagicMock, patch
from deid.config.schema import TableConfig, MappingTableConfig


def _make_config(identifier_cols, table_rules, reference_mappings=None):
    """Build a minimal DeidConfig-like mock with the given tables and identifier_columns."""
    pat_cfg = MappingTableConfig(
        source_column="patient_id",
        destination_column="nd_patient_id",
        identifier_columns=identifier_cols,
    )
    tables = [TableConfig(name=tname, rules=rules) for tname, rules in table_rules.items()]
    cfg = MagicMock()
    cfg.mapping_tables = {"patient": pat_cfg}
    cfg.tables = tables
    cfg.reference_mappings = reference_mappings or {}
    cfg.pii_db = None
    return cfg


def _run_remap(config):
    """
    Extract and run only the underscore-guard + single-identifier-bypass logic
    from async_runner without spinning up the full pipeline.
    """
    _pat_cfg = config.mapping_tables.get("patient")
    _identifier_cols = _pat_cfg.identifier_columns if _pat_cfg else []

    # --- underscore guard (copy of production code) ---
    _bad_id_cols = [c for c in _identifier_cols if "_" in c]
    if _bad_id_cols:
        raise SystemExit(
            f"identifier_columns entries must not contain underscores: {_bad_id_cols}. "
            "Rename these columns in your patient_mapping_table to remove underscores "
            "(e.g. 'chart_id' → 'chartid')."
        )

    # --- single-identifier bypass (copy of production code) ---
    if len(_identifier_cols) == 1:
        _single_id = _identifier_cols[0]
        _remap = {
            "PATIENT_ID": f"PATIENT_{_single_id.upper()}",
            "PATIENT_DOB": "DOB",
        }
        _remapped_count = 0
        for _table in (config.tables or []):
            _new_rules: dict[str, str] = {}
            for _col, _rule in _table.rules.items():
                if _rule in _remap:
                    _new_rules[_col] = _remap[_rule]
                    _remapped_count += 1
                else:
                    _new_rules[_col] = _rule
            _table.rules = _new_rules

        if config.reference_mappings:
            for _tbl_name, _ref_cfg in config.reference_mappings.items():
                if isinstance(_ref_cfg, dict):
                    _dct = _ref_cfg.get("destination_column_type", "")
                    if _dct in _remap:
                        config.reference_mappings[_tbl_name]["destination_column_type"] = _remap[_dct]
                        _remapped_count += 1

    return config


# ---------------------------------------------------------------------------
# Single-identifier remapping
# ---------------------------------------------------------------------------

class TestSingleIdentifierBypass:

    def test_patient_id_remapped_to_patient_identifier(self):
        """PATIENT_ID → PATIENT_CHARTID when identifier is 'chartid'."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={"patients": {"pid": "PATIENT_ID", "name": "MASK"}},
        )
        result = _run_remap(config)
        assert result.tables[0].rules == {"pid": "PATIENT_CHARTID", "name": "MASK"}

    def test_patient_dob_remapped_to_dob(self):
        """PATIENT_DOB → DOB when identifier is 'chartid'."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={"patients": {"dob": "PATIENT_DOB", "name": "MASK"}},
        )
        result = _run_remap(config)
        assert result.tables[0].rules == {"dob": "DOB", "name": "MASK"}

    def test_both_rules_remapped_in_same_table(self):
        """Both PATIENT_ID and PATIENT_DOB are remapped in the same table."""
        config = _make_config(
            identifier_cols=["pid"],
            table_rules={
                "patients": {
                    "patient_id": "PATIENT_ID",
                    "dob": "PATIENT_DOB",
                    "name": "MASK",
                }
            },
        )
        result = _run_remap(config)
        assert result.tables[0].rules == {
            "patient_id": "PATIENT_PID",
            "dob": "DOB",
            "name": "MASK",
        }

    def test_other_rules_unchanged(self):
        """Rules that are not PATIENT_ID or PATIENT_DOB pass through unchanged."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={
                "encounters": {
                    "enc_id": "ENCOUNTER_ID",
                    "appt_id": "APPOINTMENT_ID",
                    "notes": "GENERIC_NOTES",
                }
            },
        )
        result = _run_remap(config)
        assert result.tables[0].rules == {
            "enc_id": "ENCOUNTER_ID",
            "appt_id": "APPOINTMENT_ID",
            "notes": "GENERIC_NOTES",
        }

    def test_remap_across_multiple_tables(self):
        """PATIENT_ID is remapped in every table, not just the first."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={
                "patients": {"pid": "PATIENT_ID"},
                "encounters": {"pid": "PATIENT_ID", "enc_id": "ENCOUNTER_ID"},
            },
        )
        result = _run_remap(config)
        for table in result.tables:
            assert table.rules.get("pid") == "PATIENT_CHARTID"

    def test_identifier_uppercased_in_rule_name(self):
        """Identifier is uppercased regardless of how it's declared in config."""
        config = _make_config(
            identifier_cols=["mrn"],
            table_rules={"patients": {"pid": "PATIENT_ID"}},
        )
        result = _run_remap(config)
        assert result.tables[0].rules["pid"] == "PATIENT_MRN"

    def test_reference_mappings_remapped(self):
        """PATIENT_ID in reference_mappings.destination_column_type is also remapped."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={"patients": {}},
            reference_mappings={
                "some_table": {
                    "source_column": "ext_pid",
                    "destination_column_type": "PATIENT_ID",
                    "reference_table": "patients",
                    "reference_column": "patient_id",
                }
            },
        )
        result = _run_remap(config)
        assert result.reference_mappings["some_table"]["destination_column_type"] == "PATIENT_CHARTID"

    def test_reference_mappings_non_patient_id_unchanged(self):
        """reference_mappings with PATIENT_CHARTID already set are left alone."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={"patients": {}},
            reference_mappings={
                "some_table": {
                    "destination_column_type": "PATIENT_CHARTID",
                }
            },
        )
        result = _run_remap(config)
        assert result.reference_mappings["some_table"]["destination_column_type"] == "PATIENT_CHARTID"


# ---------------------------------------------------------------------------
# Multi-identifier: no remap
# ---------------------------------------------------------------------------

class TestMultiIdentifierNoRemap:

    def test_two_identifiers_no_remap(self):
        """With 2 identifier_columns, PATIENT_ID is NOT remapped."""
        config = _make_config(
            identifier_cols=["chartid", "patientid"],
            table_rules={"patients": {"pid": "PATIENT_ID"}},
        )
        result = _run_remap(config)
        assert result.tables[0].rules["pid"] == "PATIENT_ID"

    def test_zero_identifiers_no_remap(self):
        """With no identifier_columns, rules pass through unchanged."""
        config = _make_config(
            identifier_cols=[],
            table_rules={"patients": {"pid": "PATIENT_ID"}},
        )
        result = _run_remap(config)
        assert result.tables[0].rules["pid"] == "PATIENT_ID"


# ---------------------------------------------------------------------------
# Underscore guard
# ---------------------------------------------------------------------------

class TestUnderscoreGuard:

    def test_identifier_with_underscore_raises(self):
        """An identifier_columns entry with an underscore raises SystemExit."""
        config = _make_config(
            identifier_cols=["chart_id"],
            table_rules={"patients": {"pid": "PATIENT_ID"}},
        )
        with pytest.raises(SystemExit, match="must not contain underscores"):
            _run_remap(config)

    def test_identifier_without_underscore_passes(self):
        """An identifier_columns entry without an underscore is accepted."""
        config = _make_config(
            identifier_cols=["chartid"],
            table_rules={"patients": {}},
        )
        _run_remap(config)  # should not raise

    def test_multiple_identifiers_one_bad_raises(self):
        """Even in multi-identifier mode, an underscore in any entry raises."""
        config = _make_config(
            identifier_cols=["chartid", "patient_id"],
            table_rules={"patients": {}},
        )
        with pytest.raises(SystemExit, match="must not contain underscores"):
            _run_remap(config)
