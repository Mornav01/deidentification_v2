"""Exclusion-flag behaviour of CDC/MySQL/mapping_delta.py.

mapping_delta targets MySQL, but the exclusion logic is plain SQLAlchemy Core, so
it is exercised here against SQLite: `create_mysql_engine` is patched to hand back
SQLite engines and the staging file is ATTACHed under the staging schema name so
the `{staging_schema}.enc` references in the exclusion query resolve.

Covers:
  - excluded patients are mapped (not dropped) and flagged with excluded_at
  - the flag is sticky — a later window without exclusion criteria does not clear it
  - a patient flagged in an earlier window keeps their original nd_patient_id
  - patients that qualify via enc/structdemographics only (no `users` delta row)
    are still flagged, without their registration_date being wiped
  - encounters of excluded patients are mapped
  - excluded/excluded_at are added to a table that predates them
"""

from __future__ import annotations

import importlib.util
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text

MAPPING_DELTA_PATH = (
    Path(__file__).resolve().parents[1] / "CDC" / "MySQL" / "mapping_delta.py"
)
STAGING_SCHEMA = "mobiledoc_staging"


def _load_mapping_delta():
    spec = importlib.util.spec_from_file_location("mapping_delta", MAPPING_DELTA_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def mapping_delta():
    return _load_mapping_delta()


@pytest.fixture
def dbs(tmp_path, mapping_delta, monkeypatch):
    """Patch create_mysql_engine → SQLite; return (mapping_engine, staging_engine)."""
    mapping_path = tmp_path / "mapping.db"
    staging_path = tmp_path / "staging.db"

    def _staging_engine():
        # PARSE_DECLTYPES makes the TIMESTAMP columns come back as datetime
        # objects, matching what pymysql hands mapping_delta in production.
        return create_engine(
            f"sqlite:///{staging_path}",
            connect_args={"detect_types": sqlite3.PARSE_DECLTYPES},
        )

    def _engine(schema: str):
        if schema == STAGING_SCHEMA:
            eng = _staging_engine()

            # `select ... from mobiledoc_staging.enc` only resolves if the staging
            # file is also visible under the schema name.
            @event.listens_for(eng, "connect")
            def _attach(dbapi_conn, _record):
                dbapi_conn.execute(
                    f"ATTACH DATABASE '{staging_path}' AS {STAGING_SCHEMA}"
                )

            return eng
        return create_engine(f"sqlite:///{mapping_path}")

    monkeypatch.setattr(mapping_delta, "create_mysql_engine", _engine)

    staging_engine = _staging_engine()
    with staging_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (uid INTEGER, cdate TIMESTAMP, UserType INTEGER)"
        ))
        conn.execute(text(
            "CREATE TABLE enc (patientID INTEGER, date TIMESTAMP, "
            "encounterID INTEGER, visittype TEXT)"
        ))
        conn.execute(text(
            "CREATE TABLE structdemographics "
            "(patientid INTEGER, detailid INTEGER, value TEXT)"
        ))
        conn.execute(text("CREATE TABLE structdatadetail (id INTEGER)"))
        conn.execute(text("INSERT INTO structdatadetail VALUES (7062)"))

    mapping_engine = create_engine(f"sqlite:///{mapping_path}")
    # create_all would emit `id BIGINT AUTO_INCREMENT`, which SQLite only honours
    # for INTEGER PRIMARY KEY. Pre-create the table so create_all skips it.
    with mapping_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE encounter_mapping_table ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  nd_patient_id BIGINT NOT NULL,"
            "  encounter_id BIGINT NOT NULL,"
            "  nd_encounter_id BIGINT NOT NULL UNIQUE,"
            "  encounter_date DATETIME,"
            "  nd_ActiveFlag CHAR(1) NOT NULL DEFAULT 'Y',"
            "  created_at DATETIME NOT NULL,"
            "  updated_at DATETIME NOT NULL,"
            "  patientid BIGINT NOT NULL)"
        ))

    return mapping_engine, staging_engine


def _set_staging(staging_engine, users=(), encounters=()):
    """Replace the staging delta window with `users` / `encounters`."""
    with staging_engine.begin() as conn:
        conn.execute(text("DELETE FROM users"))
        conn.execute(text("DELETE FROM enc"))
        for uid, cdate in users:
            conn.execute(
                text("INSERT INTO users VALUES (:uid, :cdate, 3)"),
                {"uid": uid, "cdate": cdate},
            )
        for pid, date, encid, visittype in encounters:
            conn.execute(
                text(
                    "INSERT INTO enc VALUES (:pid, :date, :encid, :vt)"
                ),
                {"pid": pid, "date": date, "encid": encid, "vt": visittype},
            )


def _patients(mapping_engine) -> dict[int, dict]:
    with mapping_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT patientid, nd_patient_id, registration_date, excluded, excluded_at "
            "FROM patient_mapping_table"
        )).fetchall()
    return {
        r[0]: {
            "nd_patient_id": r[1],
            "registration_date": r[2],
            "excluded": r[3],
            "excluded_at": r[4],
        }
        for r in rows
    }


def _run(mapping_delta):
    mapping_delta.run_mapping_delta(
        mapping_schema="mapping_pg", staging_schema=STAGING_SCHEMA
    )


REG_1 = datetime(2026, 1, 5, 9, 0, 0)
REG_2 = datetime(2026, 1, 6, 9, 0, 0)


def test_excluded_patient_is_mapped_and_flagged(mapping_delta, dbs):
    mapping_engine, staging_engine = dbs
    # 101 has a normal visit; 102's NP visit puts them in the exclusion set.
    _set_staging(
        staging_engine,
        users=[(101, REG_1), (102, REG_1)],
        encounters=[
            (101, REG_1, 5001, "FU"),
            (102, REG_1, 5002, "NP"),
        ],
    )
    _run(mapping_delta)

    patients = _patients(mapping_engine)
    assert set(patients) == {101, 102}, "excluded patient must still be mapped"
    assert not patients[101]["excluded"]
    assert patients[101]["excluded_at"] is None
    assert patients[102]["excluded"]
    assert patients[102]["excluded_at"] is not None

    # Encounters are mapped for both, so an un-exclusion later has full history.
    with mapping_engine.connect() as conn:
        enc_pids = {
            r[0] for r in conn.execute(text(
                "SELECT patientid FROM encounter_mapping_table"
            ))
        }
    assert enc_pids == {101, 102}


def test_flag_is_sticky_and_nd_patient_id_is_reused(mapping_delta, dbs):
    mapping_engine, staging_engine = dbs
    _set_staging(
        staging_engine,
        users=[(102, REG_1)],
        encounters=[(102, REG_1, 5002, "NP")],
    )
    _run(mapping_delta)

    first = _patients(mapping_engine)[102]
    assert first["excluded"]
    original_nd_id = first["nd_patient_id"]
    original_excluded_at = first["excluded_at"]

    # Next window: same patient, ordinary visit, no exclusion criteria present.
    _set_staging(
        staging_engine,
        users=[(102, REG_2)],
        encounters=[(102, REG_2, 5003, "FU")],
    )
    _run(mapping_delta)

    after = _patients(mapping_engine)
    assert len(after) == 1, "must not mint a second row for the same patient"
    assert after[102]["nd_patient_id"] == original_nd_id
    assert after[102]["excluded"], "flag must not be cleared by the delta pipeline"
    assert after[102]["excluded_at"] == original_excluded_at, "excluded_at is 'since'"


def test_excluded_at_is_not_restamped_on_repeat_exclusion(mapping_delta, dbs):
    """excluded_at means 'excluded since' — a second qualifying window must not move it."""
    mapping_engine, staging_engine = dbs
    _set_staging(
        staging_engine,
        users=[(102, REG_1)],
        encounters=[(102, REG_1, 5002, "NP")],
    )
    _run(mapping_delta)
    first_excluded_at = _patients(mapping_engine)[102]["excluded_at"]
    assert first_excluded_at is not None

    # Window 2: the patient qualifies again, via a different criterion.
    _set_staging(
        staging_engine,
        users=[(102, REG_2)],
        encounters=[(102, REG_2, 5006, "RESU")],
    )
    _run(mapping_delta)

    assert _patients(mapping_engine)[102]["excluded_at"] == first_excluded_at


def test_already_mapped_patient_gets_flagged_later(mapping_delta, dbs):
    mapping_engine, staging_engine = dbs
    _set_staging(
        staging_engine,
        users=[(101, REG_1)],
        encounters=[(101, REG_1, 5001, "FU")],
    )
    _run(mapping_delta)
    assert not _patients(mapping_engine)[101]["excluded"]
    original_nd_id = _patients(mapping_engine)[101]["nd_patient_id"]

    _set_staging(
        staging_engine,
        users=[(101, REG_2)],
        encounters=[(101, REG_2, 5004, "RESU")],
    )
    _run(mapping_delta)

    after = _patients(mapping_engine)[101]
    assert after["excluded"]
    assert after["excluded_at"] is not None
    assert after["nd_patient_id"] == original_nd_id


def test_exclusion_only_patient_is_flagged_without_wiping_registration_date(
    mapping_delta, dbs
):
    """A patient can qualify via enc alone, with no `users` row in the window."""
    mapping_engine, staging_engine = dbs
    _set_staging(
        staging_engine,
        users=[(101, REG_1)],
        encounters=[(101, REG_1, 5001, "FU")],
    )
    _run(mapping_delta)
    stored_reg_date = _patients(mapping_engine)[101]["registration_date"]
    assert stored_reg_date is not None

    # Window 2: no users row for 101 at all, only an excluding encounter.
    _set_staging(
        staging_engine,
        users=[],
        encounters=[(101, REG_2, 5005, "Blood Draw")],
    )
    _run(mapping_delta)

    after = _patients(mapping_engine)[101]
    assert after["excluded"], "flagged even though absent from the users delta"
    assert after["registration_date"] == stored_reg_date, "COALESCE must keep the date"


def test_exclusion_columns_added_to_preexisting_table(mapping_delta, dbs):
    """A mapping schema created before the flag gets the columns added in place."""
    mapping_engine, staging_engine = dbs
    with mapping_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE patient_mapping_table ("
            "  nd_patient_id BIGINT NOT NULL PRIMARY KEY,"
            "  patientid BIGINT NOT NULL,"
            "  `offset` INT NOT NULL,"
            "  registration_date DATETIME,"
            "  created_at DATETIME NOT NULL,"
            "  updated_at DATETIME NOT NULL)"
        ))
        conn.execute(text(
            "INSERT INTO patient_mapping_table VALUES "
            "(900, 101, 30, '2026-01-01 00:00:00', '2026-01-01 00:00:00', "
            "'2026-01-01 00:00:00')"
        ))

    _set_staging(
        staging_engine,
        users=[(101, REG_2)],
        encounters=[(101, REG_2, 5001, "FU")],
    )
    _run(mapping_delta)

    patients = _patients(mapping_engine)
    assert patients[101]["nd_patient_id"] == 900, "pre-existing mapping preserved"
    assert not patients[101]["excluded"], "backfilled rows default to not-excluded"
