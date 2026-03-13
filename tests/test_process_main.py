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
        enc, pat, ref, appt = get_key_phi_column_list(columns)
        assert pat == ["pid"]
        assert enc == ["enc_id"]
        assert ref == ["ref_pid"]
        assert appt == ["appt_id"]

    def test_non_phi_columns_ignored(self):
        from deid.core.process_df.main import get_key_phi_column_list
        columns = [
            {"column_name": "pid", "de_identification_rule": "PATIENT_ID", "is_phi": False},
        ]
        enc, pat, ref, appt = get_key_phi_column_list(columns)
        assert pat == []
        assert enc == []

    def test_none_raises(self):
        from deid.core.process_df.main import get_key_phi_column_list
        with pytest.raises(ValueError, match="Table Config Not set"):
            get_key_phi_column_list(None)

    def test_empty_list(self):
        from deid.core.process_df.main import get_key_phi_column_list
        enc, pat, ref, appt = get_key_phi_column_list([])
        assert enc == [] and pat == [] and ref == [] and appt == []


# ---------------------------------------------------------------------------
# PatientIdentifierResolver
# ---------------------------------------------------------------------------

class TestPatientIdentifierResolver:

    def test_resolved_offset_from_patient_mapping(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "offset_from_patient_mapping": [10, 20],
        })
        key_phi_columns = ([], ["patient_id"], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=34)
        result = resolver.transform(df)
        assert "_resolved_offset" in result.columns
        assert result["_resolved_offset"].to_list() == [10, 20]

    def test_resolved_offset_fills_null_with_default(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "offset_from_patient_mapping": [None, 20],
        })
        key_phi_columns = ([], ["patient_id"], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=34)
        result = resolver.transform(df)
        assert result["_resolved_offset"].to_list() == [34, 20]

    def test_resolved_offset_no_mapping_columns(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({"patient_id": [1, 2]})
        key_phi_columns = ([], ["patient_id"], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=7)
        result = resolver.transform(df)
        assert result["_resolved_offset"].to_list() == [7, 7]

    def test_resolved_nd_patient_id(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1, 2],
            "nd_patient_id_from_patient_mapping": [100, 200],
        })
        key_phi_columns = ([], ["patient_id"], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns, offset_days=34)
        result = resolver.transform(df)
        assert "_resolved_nd_patient_id" in result.columns
        assert result["_resolved_nd_patient_id"].to_list() == [100, 200]

    def test_drops_intermediate_columns(self):
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1],
            "nd_patient_id_from_patient_mapping": [100],
            "offset_from_patient_mapping": [10],
        })
        key_phi_columns = ([], ["patient_id"], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns)
        result = resolver.transform(df)
        # Intermediate mapping columns should be dropped
        assert "nd_patient_id_from_patient_mapping" not in result.columns
        assert "offset_from_patient_mapping" not in result.columns
        # But patient_id (the original PHI column) should remain
        assert "patient_id" in result.columns

    def test_coalesce_prefers_referencepid_over_patient(self):
        """When both reference PID and patient mapping provide nd_patient_id,
        reference PID should take priority (coalesce order)."""
        from deid.core.process_df.main import PatientIdentifierResolver
        df = pl.DataFrame({
            "patient_id": [1],
            "nd_patient_id_from_referencepid_mapping": [999],
            "nd_patient_id_from_patient_mapping": [100],
        })
        key_phi_columns = ([], ["patient_id"], [], [])
        resolver = PatientIdentifierResolver(key_phi_columns)
        result = resolver.transform(df)
        assert result["_resolved_nd_patient_id"].to_list() == [999]


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
