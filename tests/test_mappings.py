"""Tests for mappings consolidation: join_dataframes, MappingDb, get_or_create retry."""
import pytest
import polars as pl
from sqlalchemy import inspect


# ---------------------------------------------------------------------------
# join_dataframes: smart type casting
# ---------------------------------------------------------------------------


class TestJoinDataframesTypeCasting:
    """Verify join_dataframes casts to Utf8 when either side is string-typed."""

    def test_both_numeric_casts_to_int64(self):
        from deid.core.ops_df.utility import join_dataframes

        left = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        right = pl.DataFrame({"id": [1, 2], "score": [10, 20]})
        result = join_dataframes(left, right, left_on="id", right_on="id", how="left")
        assert result.height == 3
        assert result["score"].to_list() == [10, 20, None]

    def test_left_string_right_int_casts_to_utf8(self):
        from deid.core.ops_df.utility import join_dataframes

        left = pl.DataFrame({"patient_id": ["PAT001", "PAT002", "PAT003"], "val": [1, 2, 3]})
        right = pl.DataFrame({"pid": ["PAT001", "PAT002"], "nd_id": [100, 200]})
        result = join_dataframes(left, right, left_on="patient_id", right_on="pid", how="left")
        assert result.height == 3
        assert result["nd_id"].to_list() == [100, 200, None]
        # String IDs should not be nullified
        assert result["patient_id"].to_list() == ["PAT001", "PAT002", "PAT003"]

    def test_right_string_left_int_casts_to_utf8(self):
        from deid.core.ops_df.utility import join_dataframes

        left = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        right = pl.DataFrame({"id": ["1", "2"], "label": ["x", "y"]})
        result = join_dataframes(left, right, left_on="id", right_on="id", how="left")
        assert result.height == 3
        # Both sides cast to Utf8, so "1" matches "1"
        assert result["label"].to_list() == ["x", "y", None]

    def test_right_suffix_renaming(self):
        from deid.core.ops_df.utility import join_dataframes

        left = pl.DataFrame({"patient_id": [1, 2], "val": ["a", "b"]})
        right = pl.DataFrame({"patient_id": [1, 2], "nd_patient_id": [100, 200], "offset": [30, 40]})
        result = join_dataframes(
            left, right,
            left_on="patient_id", right_on="patient_id",
            how="left", right_suffix="from_patient_mapping",
            drop_right_join_column=True,
        )
        assert "nd_patient_id_from_patient_mapping" in result.columns
        assert "offset_from_patient_mapping" in result.columns
        assert result.height == 2

    def test_drop_join_columns(self):
        from deid.core.ops_df.utility import join_dataframes

        left = pl.DataFrame({"a": [1, 2], "x": [10, 20]})
        right = pl.DataFrame({"b": [1, 2], "y": [30, 40]})
        result = join_dataframes(
            left, right, left_on="a", right_on="b", how="left",
            drop_left_join_column=True,
        )
        assert "a" not in result.columns
        assert "y" in result.columns


# ---------------------------------------------------------------------------
# get_or_create with MAX-based ID generation
# ---------------------------------------------------------------------------


class TestGetOrCreateMaxBased:
    """Verify MAX-based ID generation produces correct sequential IDs."""

    @pytest.fixture
    def mapping_session(self, tmp_path):
        from deid.models.base import create_mappings_engine, create_all_mappings_tables
        from sqlalchemy.orm import Session

        db_path = tmp_path / "mappings.db"
        engine = create_mappings_engine(str(db_path))
        create_all_mappings_tables(engine)
        with Session(engine) as session:
            yield session

    def test_patient_ids_are_sequential(self, mapping_session):
        from deid.models.mappings import get_or_create_patient_mapping

        id1 = get_or_create_patient_mapping(mapping_session, "P1", id_prefix=10000000)
        id2 = get_or_create_patient_mapping(mapping_session, "P2", id_prefix=10000000)
        id3 = get_or_create_patient_mapping(mapping_session, "P3", id_prefix=10000000)
        assert id1 == 10000001
        assert id2 == 10000002
        assert id3 == 10000003

    def test_patient_idempotent(self, mapping_session):
        from deid.models.mappings import get_or_create_patient_mapping

        id1 = get_or_create_patient_mapping(mapping_session, "P1", id_prefix=10000000)
        id2 = get_or_create_patient_mapping(mapping_session, "P1", id_prefix=10000000)
        assert id1 == id2

    def test_encounter_ids_are_sequential(self, mapping_session):
        from deid.models.mappings import get_or_create_encounter_mapping

        id1 = get_or_create_encounter_mapping(mapping_session, "E1", patient_id="P1")
        id2 = get_or_create_encounter_mapping(mapping_session, "E2", patient_id="P1")
        id3 = get_or_create_encounter_mapping(mapping_session, "E3", patient_id="P2")
        assert id1 == 1
        assert id2 == 2
        assert id3 == 3

    def test_encounter_idempotent(self, mapping_session):
        from deid.models.mappings import get_or_create_encounter_mapping

        id1 = get_or_create_encounter_mapping(mapping_session, "E1", patient_id="P1")
        id2 = get_or_create_encounter_mapping(mapping_session, "E1", patient_id="P1")
        assert id1 == id2


# ---------------------------------------------------------------------------
# MappingDb with Table reflection
# ---------------------------------------------------------------------------


class TestMappingDb:
    """Verify MappingDb uses Table reflection correctly."""

    @pytest.fixture
    def mapping_db_with_data(self, tmp_path):
        """Create a SQLite mapping DB with test data using the ORM models."""
        from deid.models.base import create_mappings_engine, create_all_mappings_tables
        from deid.models.mappings import PatientMapping, EncounterMapping
        from sqlalchemy.orm import Session

        db_path = tmp_path / "mappings.db"
        engine = create_mappings_engine(str(db_path))
        create_all_mappings_tables(engine)

        with Session(engine) as session:
            session.add(PatientMapping(patient_id="PAT001", nd_patient_id=10000001, offset=30))
            session.add(PatientMapping(patient_id="PAT002", nd_patient_id=10000002, offset=45))
            session.add(EncounterMapping(patient_id="PAT001", encounter_id="ENC001", nd_encounter_id=1))
            session.add(EncounterMapping(patient_id="PAT001", encounter_id="ENC002", nd_encounter_id=2))
            session.commit()

        return {"connection_str": f"sqlite:///{db_path}"}

    def test_get_nd_patients_dict(self, mapping_db_with_data):
        from deid.core.dbPkg.mapping_loader import MappingDb

        db = MappingDb(mapping_db_with_data)
        result = db.get_nd_patients_dict([10000001, 10000002], id_column="nd_patient_id")
        assert len(result) == 2
        assert result[10000001]["patient_id"] == "PAT001"
        assert result[10000002]["offset"] == 45
        db.close_connection()

    def test_get_nd_encounter_dict(self, mapping_db_with_data):
        from deid.core.dbPkg.mapping_loader import MappingDb

        db = MappingDb(mapping_db_with_data)
        result = db.get_nd_encounter_dict(["ENC001", "ENC002"])
        assert len(result) == 2
        assert result["ENC001"]["nd_encounter_id"] == 1
        assert result["ENC001"]["patient_id"] == "PAT001"
        db.close_connection()

    def test_get_reverse_patients_dict(self, mapping_db_with_data):
        from deid.core.dbPkg.mapping_loader import MappingDb

        db = MappingDb(mapping_db_with_data)
        result = db.get_reverse_patients_dict([10000001])
        assert len(result) == 1
        assert result[10000001]["patient_id"] == "PAT001"
        assert result[10000001]["offset"] == 30
        db.close_connection()

    def test_get_reverse_encounter_dict(self, mapping_db_with_data):
        from deid.core.dbPkg.mapping_loader import MappingDb

        db = MappingDb(mapping_db_with_data)
        result = db.get_reverse_encounter_dict([1, 2])
        assert len(result) == 2
        assert result[1]["encounter_id"] == "ENC001"
        assert result[2]["patient_id"] == "PAT001"
        db.close_connection()

    def test_empty_ids_returns_empty_dict(self, mapping_db_with_data):
        from deid.core.dbPkg.mapping_loader import MappingDb

        db = MappingDb(mapping_db_with_data)
        assert db.get_nd_patients_dict([]) == {}
        assert db.get_nd_encounter_dict([]) == {}
        assert db.get_reverse_patients_dict([]) == {}
        assert db.get_reverse_encounter_dict([]) == {}
        db.close_connection()

    def test_nonexistent_ids_returns_empty_dict(self, mapping_db_with_data):
        from deid.core.dbPkg.mapping_loader import MappingDb

        db = MappingDb(mapping_db_with_data)
        assert db.get_nd_patients_dict([99999]) == {}
        assert db.get_reverse_patients_dict([99999]) == {}
        db.close_connection()
