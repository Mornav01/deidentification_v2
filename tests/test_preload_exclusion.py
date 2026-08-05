"""Exclusion gating in the de-id mapping preload (deid/tasks/celery_app.py).

The `excluded` flag exists only in dent's mapping_pg. These tests pin both halves
of that: a schema that HAS the column gets its excluded patients gated out, and a
schema that does NOT (every other client) preloads exactly as it always did.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from deid.tasks import celery_app as ca


PATIENT_DDL_WITH_FLAG = """
CREATE TABLE patient_mapping_table (
  nd_patient_id BIGINT NOT NULL PRIMARY KEY,
  patientid     BIGINT NOT NULL,
  `offset`      INT    NOT NULL,
  excluded      TINYINT(1) NOT NULL DEFAULT 0,
  excluded_at   DATETIME
)
"""

PATIENT_DDL_NO_FLAG = """
CREATE TABLE patient_mapping_table (
  nd_patient_id BIGINT NOT NULL PRIMARY KEY,
  patientid     BIGINT NOT NULL,
  `offset`      INT    NOT NULL
)
"""

ENCOUNTER_DDL = """
CREATE TABLE encounter_mapping_table (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  nd_patient_id   BIGINT NOT NULL,
  encounter_id    BIGINT NOT NULL,
  nd_encounter_id BIGINT NOT NULL,
  nd_ActiveFlag   CHAR(1) NOT NULL DEFAULT 'Y',
  patientid       BIGINT NOT NULL
)
"""


def _build_db(path, *, with_flag: bool):
    """Two patients (1 normal, 2 excluded-if-supported), one encounter each."""
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.execute(text(PATIENT_DDL_WITH_FLAG if with_flag else PATIENT_DDL_NO_FLAG))
        conn.execute(text(ENCOUNTER_DDL))
        if with_flag:
            conn.execute(text(
                "INSERT INTO patient_mapping_table "
                "(nd_patient_id, patientid, `offset`, excluded) VALUES "
                "(1001, 101, 30, 0), (1002, 102, 31, 1)"
            ))
        else:
            conn.execute(text(
                "INSERT INTO patient_mapping_table "
                "(nd_patient_id, patientid, `offset`) VALUES "
                "(1001, 101, 30), (1002, 102, 31)"
            ))
        conn.execute(text(
            "INSERT INTO encounter_mapping_table "
            "(nd_patient_id, encounter_id, nd_encounter_id, nd_ActiveFlag, patientid) "
            "VALUES (1001, 5001, 10010001, 'Y', 101), "
            "       (1002, 5002, 10020001, 'Y', 102)"
        ))
    engine.dispose()


@pytest.fixture
def run_preload(tmp_path, monkeypatch):
    def _run(*, with_flag: bool) -> dict:
        db_path = tmp_path / f"mappings_{with_flag}.db"
        _build_db(db_path, with_flag=with_flag)

        cfg = SimpleNamespace(
            mappings_connection_string=f"sqlite:///{db_path}", pii_db=None
        )
        monkeypatch.setattr("deid.config.loader.load_config", lambda _p: cfg)

        ca._preloaded_data.clear()
        app = SimpleNamespace(
            conf=SimpleNamespace(deid_config_path=str(tmp_path / "config.yaml"))
        )
        ca._preload_mappings(app)
        return dict(ca._preloaded_data)

    yield _run
    ca._preloaded_data.clear()


def test_excluded_patients_gated_out_when_column_present(run_preload):
    preloaded = run_preload(with_flag=True)

    pat = preloaded["patient_mapping"]
    assert pat["nd_patient_id"].to_list() == [1001], "excluded patient must not preload"

    # The encounter row carries nd_patient_id itself, so it has to go too —
    # otherwise the resolver coalesces it and the excluded patient resolves anyway.
    enc = preloaded["encounter_mapping"]
    assert enc["nd_patient_id"].to_list() == [1001]


def test_no_gating_when_column_absent(run_preload):
    """Other clients' mapping schemas preload unchanged."""
    preloaded = run_preload(with_flag=False)

    pat = preloaded["patient_mapping"]
    assert sorted(pat["nd_patient_id"].to_list()) == [1001, 1002]
    assert "excluded" not in pat.columns

    enc = preloaded["encounter_mapping"]
    assert sorted(enc["nd_patient_id"].to_list()) == [1001, 1002]
