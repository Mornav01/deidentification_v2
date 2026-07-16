"""Tests for deid.core.process_df.main — helper functions and PatientIdentifierResolver."""
import pytest
import polars as pl


# ---------------------------------------------------------------------------
# get_key_phi_column_list
# ---------------------------------------------------------------------------

class TestGetKeyPhiColumnList:

    def test_categorises_columns_correctly(self):
        from deid.core.process_df.main import get_key_phi_column_list
        columns = [
            {"column_name": "pid", "de_identification_rule": "PATIENT_ID", "is_phi": True},
            {"column_name": "enc_id", "de_identification_rule": "ENCOUNTER_ID", "is_phi": True},
            {"column_name": "ref_pid", "de_identification_rule": "REFERENCE_PID", "is_phi": True},
            {"column_name": "appt_id", "de_identification_rule": "APPOINTMENT_ID", "is_phi": True},
            {"column_name": "name", "de_identification_rule": "MASK", "is_phi": True},
        ]
        enc, pat, ref, appt, chart = get_key_phi_column_list(columns)
        assert pat == {"PATIENT_ID": ["pid"]}
        assert enc == ["enc_id"]
        assert ref == ["ref_pid"]
        assert appt == ["appt_id"]
        assert chart == []

    def test_non_phi_columns_ignored(self):
        from deid.core.process_df.main import get_key_phi_column_list
        columns = [
            {"column_name": "pid", "de_identification_rule": "PATIENT_ID", "is_phi": False},
        ]
        enc, pat, ref, appt, chart = get_key_phi_column_list(columns)
        assert pat == {}
        assert enc == []
        assert chart == []

    def test_none_raises(self):
        from deid.core.process_df.main import get_key_phi_column_list
        with pytest.raises(ValueError, match="Table Config Not set"):
            get_key_phi_column_list(None)

    def test_empty_list(self):
        from deid.core.process_df.main import get_key_phi_column_list
        enc, pat, ref, appt, chart = get_key_phi_column_list([])
        assert enc == [] and pat == {} and ref == [] and appt == [] and chart == []


# ---------------------------------------------------------------------------
# PatientIdentifierResolver
# ---------------------------------------------------------------------------

class TestPatientIdentifierResolver:

    def test_resolved_offset_from_patient_mapping(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "offset_from_patient_id_mapping": [10, 20],
        })
        key_phi_columns = ([], {"PATIENT_ID": ["patient_id"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=34)
        result = resolver.transform(df)
        assert "_resolved_offset" in result.columns
        assert result["_resolved_offset"].to_list() == [10, 20]

    def test_resolved_offset_fills_null_with_default(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "offset_from_patient_id_mapping": [None, 20],
        })
        key_phi_columns = ([], {"PATIENT_ID": ["patient_id"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=34)
        result = resolver.transform(df)
        assert result["_resolved_offset"].to_list() == [34, 20]

    def test_resolved_offset_no_mapping_columns(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({"patient_id": [1, 2]})
        key_phi_columns = ([], {"PATIENT_ID": ["patient_id"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=7)
        result = resolver.transform(df)
        assert result["_resolved_offset"].to_list() == [7, 7]

    def test_resolved_nd_patient_id(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "nd_patient_id_from_patient_id_mapping": [100, 200],
        })
        key_phi_columns = ([], {"PATIENT_ID": ["patient_id"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=34)
        result = resolver.transform(df)
        assert "_resolved_nd_patient_id" in result.columns
        assert result["_resolved_nd_patient_id"].to_list() == [100, 200]

    def test_drops_intermediate_columns(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1],
            "nd_patient_id_from_patient_id_mapping": [100],
            "offset_from_patient_id_mapping": [10],
        })
        key_phi_columns = ([], {"PATIENT_ID": ["patient_id"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns)
        result = resolver.transform(df)
        # Intermediate mapping columns should be dropped
        assert "nd_patient_id_from_patient_id_mapping" not in result.columns
        assert "offset_from_patient_id_mapping" not in result.columns
        # But patient_id (the original PHI column) should remain
        assert "patient_id" in result.columns

    def test_coalesce_prefers_referencepid_over_patient(self):
        """When both reference PID and patient mapping provide nd_patient_id,
        reference PID should take priority (coalesce order)."""
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1],
            "nd_patient_id_from_referencepid_mapping": [999],
            "nd_patient_id_from_patient_id_mapping": [100],
        })
        key_phi_columns = ([], {"PATIENT_ID": ["patient_id"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns)
        result = resolver.transform(df)
        assert result["_resolved_nd_patient_id"].to_list() == [999]

    def test_multiple_patient_columns_resolve_independently(self):
        """Two patient-ID columns in one row (e.g. mergelogs FromID/ToID) that reference
        DIFFERENT patients must each get their own per-column de-identified value.

        Simulates the DataFrame after apply_patient_mappings has joined each column:
        the first column keeps the identifier-keyed suffix, the second uses the
        per-column suffix.
        """
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "fromid": [1, 2],
            "toid": [10, 20],
            "nd_patient_id_from_patient_id_mapping": [111, 222],       # from fromid join
            "nd_patient_id_from_col_toid_mapping": [910, 920],          # from toid join
        })
        key_phi_columns = ([], {"PATIENT_ID": ["fromid", "toid"]}, [], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns)
        result = resolver.transform(df)

        # Per-column resolved values are distinct and match their own source column's mapping.
        assert result["_resolved_ndpid_col_fromid"].to_list() == [111, 222]
        assert result["_resolved_ndpid_col_toid"].to_list() == [910, 920]
        # Row-level fallback still coalesces to the primary (first) column.
        assert result["_resolved_nd_patient_id"].to_list() == [111, 222]

    def test_patient_id_rule_uses_per_column_value(self):
        """PatientIDRule must write each patient-ID column's OWN de-identified value,
        not the single coalesced _resolved_nd_patient_id (mergelogs FromID/ToID bug)."""
        from deid.core.process_df.rules import PatientIDRule
        df = pl.DataFrame({
            "fromid": [1, 2],
            "toid": [10, 20],
            "_resolved_nd_patient_id": [111, 222],
            "_resolved_ndpid_col_fromid": [111, 222],
            "_resolved_ndpid_col_toid": [910, 920],
        })
        rule = PatientIDRule()
        df = rule.apply(df, {"column_name": "fromid"})
        df = rule.apply(df, {"column_name": "toid"})

        assert df["fromid"].to_list() == [111, 222]
        assert df["toid"].to_list() == [910, 920]          # NOT [111, 222]
        # The two distinct patients stay distinct after de-identification.
        assert df["fromid"].to_list() != df["toid"].to_list()

    def test_patient_id_rule_falls_back_to_row_level(self):
        """Single-patient tables (no per-column value) keep using _resolved_nd_patient_id."""
        from deid.core.process_df.rules import PatientIDRule
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "_resolved_nd_patient_id": [111, 222],
        })
        df = PatientIDRule().apply(df, {"column_name": "patient_id"})
        assert df["patient_id"].to_list() == [111, 222]


# ---------------------------------------------------------------------------
# _serialize_dict_values
# ---------------------------------------------------------------------------

class TestSerializeDictValues:

    def test_serializes_dict_cells(self):
        from deid.core.process_df.main import _serialize_dict_values
        df = pl.DataFrame({
            "data": [{"key": "value"}, {"nested": {"a": 1}}],
        }, schema={"data": pl.Object})
        result = _serialize_dict_values(df)
        vals = result["data"].to_list()
        assert vals[0] == '{"key": "value"}'
        assert '"nested"' in vals[1]

    def test_non_dict_passthrough(self):
        from deid.core.process_df.main import _serialize_dict_values
        df = pl.DataFrame({"name": ["Alice", "Bob"]})
        result = _serialize_dict_values(df)
        assert result["name"].to_list() == ["Alice", "Bob"]

    def test_mixed_types(self):
        from deid.core.process_df.main import _serialize_dict_values
        df = pl.DataFrame({
            "mixed": [{"a": 1}, "plain_string", None],
        }, schema={"mixed": pl.Object})
        result = _serialize_dict_values(df)
        vals = result["mixed"].to_list()
        assert vals[0] == '{"a": 1}'
        assert vals[1] == "plain_string"
        assert vals[2] is None


# ---------------------------------------------------------------------------
# _sql_result_to_polars
# ---------------------------------------------------------------------------

class TestSqlResultToPolars:

    def test_empty_result(self):
        """Empty result should return DataFrame with correct schema."""
        from unittest.mock import MagicMock
        from deid.core.process_df.main import _sql_result_to_polars

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = ["id", "name"]

        df = _sql_result_to_polars(mock_result)
        assert df.height == 0
        assert df.columns == ["id", "name"]

    def test_with_rows(self):
        from unittest.mock import MagicMock
        from deid.core.process_df.main import _sql_result_to_polars

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_result.keys.return_value = ["id", "name"]

        df = _sql_result_to_polars(mock_result)
        assert df.height == 2
        assert df["id"].to_list() == [1, 2]
        assert df["name"].to_list() == ["Alice", "Bob"]
