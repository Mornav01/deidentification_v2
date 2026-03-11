"""Integration test for the 3-stage pipeline (fetch → process → write)."""
import json
import polars as pl
import pytest
from pathlib import Path
from sqlalchemy.orm import Session
from unittest.mock import patch, MagicMock


@pytest.fixture
def pipeline_env(tmp_path):
    """Set up state.db with BatchState rows and staging dir."""
    from deid.models.base import create_state_engine, create_all_state_tables
    from deid.models.state import BatchState, DbConfig, TableState

    state_db = str(tmp_path / "state.db")
    engine = create_state_engine(state_db)
    create_all_state_tables(engine)

    with Session(engine) as s:
        db_cfg = DbConfig(name="test", source_conn_str="sqlite:///s.db", dest_conn_str="sqlite:///d.db")
        s.add(db_cfg)
        s.commit()
        s.add(TableState(db_config_id=db_cfg.id, table_name="t1", status="pending", row_count=3))
        s.add(BatchState(table_name="t1", start_id=1, end_id=3, status="pending"))
        s.commit()

    return {"state_db": state_db, "engine": engine, "tmp_path": tmp_path}


@pytest.fixture(autouse=True)
def _setup_celery():
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(broker_url="memory://", result_backend="cache+memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    app.finalize()
    app.loader.import_default_modules()
    return app


def test_fetch_process_write_chain(pipeline_env):
    """A batch flows through all 3 stages end-to-end."""
    from deid.models.state import BatchState, TableState

    state_db = pipeline_env["state_db"]
    engine = pipeline_env["engine"]
    staging_root = str(pipeline_env["tmp_path"] / ".deid_staging")

    mock_df = pl.DataFrame({
        "nd_auto_increment_id": [1, 2, 3],
        "col1": ["val1", "val2", "val3"],
    })

    def mock_offset(handler, table_name, batch_size, last_id=None, id_column="nd_auto_increment_id"):
        yield mock_df

    # Build fetch config dict with extra keys that get forwarded through the chain.
    fetch_config = {
        "table_name": "t1",
        "start_id": 1,
        "end_id": 3,
        "source_conn_str": "sqlite:///s.db",
        "state_db_path": state_db,
        "staging_root": staging_root,
        # Extra keys forwarded to process_batch:
        "mapping_db_config": {"connection_str": "sqlite:///m.db"},
        "table_details": {"columns_details": []},
        "offset_days": 34,
        # Extra key forwarded to write_batch:
        "dest_conn_str": "sqlite:///d.db",
    }

    with patch("deid.tasks.fetch.NDDBHandler") as mock_src, \
         patch("deid.tasks.fetch.stream_table_keyset", side_effect=mock_offset), \
         patch("deid.tasks.process.NDDBHandler") as mock_proc_src, \
         patch("deid.tasks.process.ReferenceMappingDataFrameJoiner") as mock_ref, \
         patch("deid.tasks.process.JoinMapping") as mock_jm, \
         patch("deid.tasks.process.PatientIdentifierResolver") as mock_pir, \
         patch("deid.tasks.process.InvalidRowHandler") as mock_irh, \
         patch("deid.tasks.process.DeIdentifier") as mock_deid, \
         patch("deid.tasks.write.NDDBHandler") as mock_dest:

        # Setup fetch mocks
        mock_src.return_value = MagicMock()
        mock_src.return_value.get_columns.return_value = [
            {"name": "nd_auto_increment_id", "type": MagicMock(length=None)},
            {"name": "col1", "type": MagicMock(length=100)},
        ]

        # Setup process mocks
        mock_proc_src.return_value = MagicMock()
        mock_ref.return_value.join_dataframe.return_value = (mock_df, ([], [], [], []))
        mock_jm_inst = MagicMock()
        mock_jm.return_value = mock_jm_inst
        for m in ["_get_distinct_encounterids", "_get_distinct_patientids",
                   "_get_distinct_referencepids", "_get_distinct_appointmentids"]:
            getattr(mock_jm_inst, m).return_value = []
        for m in ["_get_encounter_mapping", "_get_patient_mapping",
                   "_get_reference_pid_mapping", "_get_appointment_mapping"]:
            getattr(mock_jm_inst, m).return_value = None
        mock_pir.return_value.transform.return_value = mock_df
        mock_irh.return_value.handle.return_value = mock_df
        mock_deid.return_value.apply_rules.return_value = mock_df

        # Setup write mocks
        mock_dest_inst = MagicMock()
        mock_dest_inst._qi = lambda x: f"`{x}`"
        mock_dest.return_value = mock_dest_inst

        # Run fetch — it chains to process → write automatically in eager mode
        from deid.tasks.fetch import fetch_batch
        fetch_batch(fetch_config)

    # Verify final state: batch done, table completed
    with Session(engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "done"

        table = s.query(TableState).first()
        assert table.status == "completed"
