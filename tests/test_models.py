import pytest
from pathlib import Path
from datetime import datetime


def test_state_db_tables_created(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables

    db_path = tmp_path / "state.db"
    engine = create_state_engine(str(db_path))
    create_all_state_tables(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    assert "db_configs" in tables
    assert "table_states" in tables
    assert "run_logs" in tables


def test_mappings_db_tables_created(tmp_path):
    from deid.models.base import create_mappings_engine, create_all_mappings_tables

    db_path = tmp_path / "mappings.db"
    engine = create_mappings_engine(str(db_path))
    create_all_mappings_tables(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    assert "patient_mapping_table" in tables
    assert "encounter_mapping_table" in tables
    assert "appointment_mapping_table" in tables
    assert "phi_staging" in tables


def test_insert_and_query_table_state(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    from deid.models.state import DbConfig, TableState
    from sqlalchemy.orm import Session

    db_path = tmp_path / "state.db"
    engine = create_state_engine(str(db_path))
    create_all_state_tables(engine)

    with Session(engine) as session:
        db_cfg = DbConfig(
            name="test_db",
            source_conn_str="mysql://localhost/src",
            dest_conn_str="postgresql://localhost/dest",
            run_config={"pii_config": {}},
        )
        session.add(db_cfg)
        session.commit()

        ts = TableState(
            db_config_id=db_cfg.id,
            table_name="patients",
            status="pending",
            rules_config={"patient_id": "PATIENT_ID"},
        )
        session.add(ts)
        session.commit()

        result = session.query(TableState).filter_by(table_name="patients").first()
        assert result is not None
        assert result.status == "pending"
        assert result.db_config_id == db_cfg.id


def test_patient_mapping_get_or_create(tmp_path):
    from deid.models.base import create_mappings_engine, create_all_mappings_tables
    from deid.models.mappings import get_or_create_patient_mapping
    from sqlalchemy.orm import Session

    db_path = tmp_path / "mappings.db"
    engine = create_mappings_engine(str(db_path))
    create_all_mappings_tables(engine)

    with Session(engine) as session:
        nd_id_1 = get_or_create_patient_mapping(session, "PAT001", id_prefix=10000000)
        nd_id_2 = get_or_create_patient_mapping(session, "PAT001", id_prefix=10000000)
        nd_id_3 = get_or_create_patient_mapping(session, "PAT002", id_prefix=10000000)
        assert nd_id_1 == nd_id_2  # Same patient, same ID
        assert nd_id_3 != nd_id_1  # Different patient, different ID
        assert nd_id_1 >= 10000001


def test_encounter_mapping_get_or_create(tmp_path):
    from deid.models.base import create_mappings_engine, create_all_mappings_tables
    from deid.models.mappings import get_or_create_patient_mapping, get_or_create_encounter_mapping
    from sqlalchemy.orm import Session

    db_path = tmp_path / "mappings.db"
    engine = create_mappings_engine(str(db_path))
    create_all_mappings_tables(engine)

    with Session(engine) as session:
        pat_id = get_or_create_patient_mapping(session, "PAT001", id_prefix=10000000)
        enc_id_1 = get_or_create_encounter_mapping(session, "ENC001", patient_id="PAT001")
        enc_id_2 = get_or_create_encounter_mapping(session, "ENC001", patient_id="PAT001")
        assert enc_id_1 == enc_id_2
