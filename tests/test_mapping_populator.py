"""Tests for deid.core.mapping_populator module."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from deid.config.schema import TableConfig
from deid.core.mapping_populator import (
    bulk_insert_appointment_mappings,
    bulk_insert_encounter_mappings,
    bulk_insert_patient_mappings,
    populate_mappings,
    scan_rules_for_id_columns,
)
from deid.models.base import create_all_mappings_tables, create_mappings_engine
from deid.models.mappings import AppointmentMapping, EncounterMapping, PatientMapping


# ---------------------------------------------------------------------------
# TestScanRulesForIdColumns
# ---------------------------------------------------------------------------


class TestScanRulesForIdColumns:
    def test_finds_patient_id_columns(self):
        tables = [
            TableConfig(name="patients", rules={"pid": "PATIENT_ID", "name": "MASK"}),
            TableConfig(name="encounters", rules={"pat_id": "PATIENT_ID", "enc_id": "ENCOUNTER_ID"}),
        ]
        result = scan_rules_for_id_columns(tables)

        assert "patients" in result["patient_id_columns"]
        assert result["patient_id_columns"]["patients"] == ["pid"]
        assert "encounters" in result["patient_id_columns"]
        assert result["patient_id_columns"]["encounters"] == ["pat_id"]

    def test_finds_encounter_tables_with_both_columns(self):
        tables = [
            TableConfig(
                name="visit",
                rules={"pat_id": "PATIENT_ID", "enc_id": "ENCOUNTER_ID", "date": "DATE_OFFSET"},
            ),
            TableConfig(
                name="orders",
                rules={"enc_id": "ENCOUNTER_ID", "note": "MASK"},
            ),
        ]
        result = scan_rules_for_id_columns(tables)

        # visit has both ENCOUNTER_ID and PATIENT_ID -> included
        assert "visit" in result["encounter_id_tables"]
        assert result["encounter_id_tables"]["visit"] == ("enc_id", "pat_id")

        # orders has ENCOUNTER_ID but NO PATIENT_ID -> excluded
        assert "orders" not in result["encounter_id_tables"]

    def test_finds_appointment_tables(self):
        tables = [
            TableConfig(
                name="appointments",
                rules={"pid": "PATIENT_ID", "appt": "APPOINTMENT_ID"},
            ),
        ]
        result = scan_rules_for_id_columns(tables)

        assert "appointments" in result["appointment_id_tables"]
        assert result["appointment_id_tables"]["appointments"] == ("appt", "pid")


# ---------------------------------------------------------------------------
# TestBulkInsertPatientMappings
# ---------------------------------------------------------------------------


class TestBulkInsertPatientMappings:
    @pytest.fixture()
    def engine(self, tmp_path):
        eng = create_mappings_engine(str(tmp_path / "mappings.db"))
        create_all_mappings_tables(eng)
        return eng

    def test_inserts_patient_mappings_with_sequential_ids(self, engine):
        prefix = 10_000_000
        max_offset = 365

        count = bulk_insert_patient_mappings(engine, ["P001", "P002", "P003"], prefix, max_offset)

        assert count == 3

        with Session(engine) as session:
            mappings = (
                session.query(PatientMapping)
                .order_by(PatientMapping.nd_patient_id)
                .all()
            )
            assert len(mappings) == 3
            assert mappings[0].nd_patient_id == prefix + 1
            assert mappings[1].nd_patient_id == prefix + 2
            assert mappings[2].nd_patient_id == prefix + 3

            for m in mappings:
                assert 1 <= m.offset <= max_offset

    def test_is_idempotent(self, engine):
        prefix = 10_000_000
        max_offset = 365

        count1 = bulk_insert_patient_mappings(engine, ["P001", "P002"], prefix, max_offset)
        assert count1 == 2

        count2 = bulk_insert_patient_mappings(engine, ["P002", "P003"], prefix, max_offset)
        assert count2 == 1  # only P003 is new

        with Session(engine) as session:
            total = session.query(PatientMapping).count()
            assert total == 3

    def test_offsets_are_random(self, engine):
        prefix = 10_000_000
        max_offset = 365
        ids = [f"P{i:04d}" for i in range(100)]

        bulk_insert_patient_mappings(engine, ids, prefix, max_offset)

        with Session(engine) as session:
            offsets = [m.offset for m in session.query(PatientMapping).all()]
            assert len(set(offsets)) > 1, "Expected more than 1 distinct offset value"


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------


def _make_mappings_engine(tmp_path):
    eng = create_mappings_engine(str(tmp_path / "mappings.db"))
    create_all_mappings_tables(eng)
    return eng


# ---------------------------------------------------------------------------
# TestBulkInsertEncounterMappings
# ---------------------------------------------------------------------------


class TestBulkInsertEncounterMappings:
    @pytest.fixture()
    def engine(self, tmp_path):
        return _make_mappings_engine(tmp_path)

    def test_inserts_encounter_mappings(self, engine):
        pairs = [("E001", "P001"), ("E002", "P001"), ("E003", "P002")]
        count = bulk_insert_encounter_mappings(engine, pairs)

        assert count == 3

        with Session(engine) as session:
            mappings = (
                session.query(EncounterMapping)
                .order_by(EncounterMapping.nd_encounter_id)
                .all()
            )
            assert len(mappings) == 3

            assert mappings[0].encounter_id == "E001"
            assert mappings[0].patient_id == "P001"
            assert mappings[0].nd_encounter_id == 1

            assert mappings[1].encounter_id == "E002"
            assert mappings[1].patient_id == "P001"
            assert mappings[1].nd_encounter_id == 2

            assert mappings[2].encounter_id == "E003"
            assert mappings[2].patient_id == "P002"
            assert mappings[2].nd_encounter_id == 3

    def test_is_idempotent(self, engine):
        count1 = bulk_insert_encounter_mappings(engine, [("E001", "P001")])
        assert count1 == 1

        count2 = bulk_insert_encounter_mappings(engine, [("E001", "P001"), ("E002", "P002")])
        assert count2 == 1  # only E002 is new

        with Session(engine) as session:
            total = session.query(EncounterMapping).count()
            assert total == 2


# ---------------------------------------------------------------------------
# TestBulkInsertAppointmentMappings
# ---------------------------------------------------------------------------


class TestBulkInsertAppointmentMappings:
    @pytest.fixture()
    def engine(self, tmp_path):
        return _make_mappings_engine(tmp_path)

    def test_inserts_appointment_mappings(self, engine):
        pairs = [("A001", "P001"), ("A002", "P002")]
        count = bulk_insert_appointment_mappings(engine, pairs)

        assert count == 2

        with Session(engine) as session:
            mappings = (
                session.query(AppointmentMapping)
                .order_by(AppointmentMapping.nd_appointment_id)
                .all()
            )
            assert len(mappings) == 2

            assert mappings[0].appointment_id == "A001"
            assert mappings[0].patient_id == "P001"
            assert mappings[0].nd_appointment_id == 1

            assert mappings[1].appointment_id == "A002"
            assert mappings[1].patient_id == "P002"
            assert mappings[1].nd_appointment_id == 2


# ---------------------------------------------------------------------------
# TestPopulateMappings
# ---------------------------------------------------------------------------


class TestPopulateMappings:
    def test_no_id_rules_returns_zeros(self, tmp_path):
        """Tables with no ID rules should produce an all-zero summary."""
        tables = [
            TableConfig(name="notes", rules={"text": "MASK", "zip": "ZIP_CODE"}),
        ]
        source = MagicMock()
        engine = create_mappings_engine(str(tmp_path / "mappings.db"))
        create_all_mappings_tables(engine)

        summary = populate_mappings(source, tables, engine)

        assert summary == {
            "patients_found": 0,
            "patients_created": 0,
            "encounters_found": 0,
            "encounters_created": 0,
            "appointments_found": 0,
            "appointments_created": 0,
        }
        source.fetch_distinct_values.assert_not_called()
        source.fetch_distinct_pairs.assert_not_called()

    def test_end_to_end_with_mock_source(self, tmp_path):
        # ── Table configs ─────────────────────────────────────────────
        tables = [
            TableConfig(name="patients", rules={"PatientID": "PATIENT_ID", "Name": "MASK"}),
            TableConfig(
                name="encounters",
                rules={"EncounterID": "ENCOUNTER_ID", "PID": "PATIENT_ID"},
            ),
        ]

        # ── Mock source handler ───────────────────────────────────────
        source = MagicMock()

        def _fetch_distinct_values(table_name, col):
            lookup = {
                ("patients", "PatientID"): iter(["P001", "P002", "P003"]),
                ("encounters", "PID"): iter(["P001", "P002"]),
            }
            return lookup.get((table_name, col), iter([]))

        def _fetch_distinct_pairs(table_name, col_a, col_b):
            lookup = {
                ("encounters", "EncounterID", "PID"): iter([("E001", "P001"), ("E002", "P002")]),
            }
            return lookup.get((table_name, col_a, col_b), iter([]))

        source.fetch_distinct_values = MagicMock(side_effect=_fetch_distinct_values)
        source.fetch_distinct_pairs = MagicMock(side_effect=_fetch_distinct_pairs)

        # ── Mappings engine ───────────────────────────────────────────
        engine = create_mappings_engine(str(tmp_path / "mappings.db"))
        create_all_mappings_tables(engine)

        # ── Run ───────────────────────────────────────────────────────
        summary = populate_mappings(
            source, tables, engine, patient_id_prefix=10000000, max_offset=34
        )

        # ── Assert summary ────────────────────────────────────────────
        assert summary["patients_found"] == 3
        assert summary["patients_created"] == 3
        assert summary["encounters_found"] == 2
        assert summary["encounters_created"] == 2

        # ── Assert DB rows ────────────────────────────────────────────
        with Session(engine) as session:
            assert session.query(PatientMapping).count() == 3
            assert session.query(EncounterMapping).count() == 2
