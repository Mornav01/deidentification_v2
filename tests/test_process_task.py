"""Tests for process_batch task."""
import json
import polars as pl
import pyarrow.ipc as ipc
import pytest
from pathlib import Path
from sqlalchemy.orm import Session
from unittest.mock import patch, MagicMock


@pytest.fixture
def state_engine(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(engine)
    return engine


@pytest.fixture(autouse=True)
def _setup_celery():
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(broker_url="memory://", result_backend="cache+memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    app.finalize()
    app.loader.import_default_modules()
    return app


def _write_fetched_arrow(staging_root, table, start_id, end_id, df):
    """Write an Arrow IPC file with deid_column_schema metadata."""
    from deid.staging import batch_fetched_path
    path = batch_fetched_path(Path(staging_root), table, start_id, end_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    arrow_table = df.to_arrow()
    meta = {b"deid_column_schema": json.dumps({"col1": {"type": "VARCHAR", "length": 100}}).encode()}
    arrow_table = arrow_table.replace_schema_metadata(meta)
    with ipc.new_file(str(path), arrow_table.schema) as writer:
        writer.write_table(arrow_table)
    return path


def test_process_batch_deidentifies_and_writes_proc_arrow(tmp_path, state_engine):
    from deid.models.state import BatchState
    from deid.config.task_models import ProcessTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_path = str(tmp_path / "state.db")

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=5, status="fetched"))
        s.commit()

    df = pl.DataFrame({"nd_auto_increment_id": [1, 2, 3, 4, 5], "col1": ["a", "b", "c", "d", "e"]})
    _write_fetched_arrow(staging_root, "t1", 1, 5, df)

    config = ProcessTaskConfig(
        table_name="t1",
        start_id=1,
        end_id=5,
        staging_root=staging_root,
        state_db_path=state_db_path,
        mapping_db_config={"connection_str": "sqlite:///mappings.db"},
        table_details={"columns_details": []},
        source_conn_str="sqlite:///src.db",
    )

    # Mock the de-identification pipeline — it returns the df unchanged for this test
    with patch("deid.tasks.process.ReferenceMappingDataFrameJoiner") as mock_ref, \
         patch("deid.tasks.process.JoinMapping") as mock_jm, \
         patch("deid.tasks.process.PatientIdentifierResolver") as mock_pir, \
         patch("deid.tasks.process.InvalidRowHandler") as mock_irh, \
         patch("deid.tasks.process.DeIdentifier") as mock_deid, \
         patch("deid.tasks.process.NDDBHandler") as mock_handler, \
         patch("deid.tasks.write.write_batch") as mock_write:

        mock_ref.return_value.join_dataframe.return_value = (df, ([], [], [], []))
        mock_jm_inst = MagicMock()
        mock_jm.return_value = mock_jm_inst
        mock_jm_inst._get_distinct_encounterids.return_value = []
        mock_jm_inst._get_distinct_patientids.return_value = []
        mock_jm_inst._get_distinct_referencepids.return_value = []
        mock_jm_inst._get_distinct_appointmentids.return_value = []
        mock_jm_inst._get_encounter_mapping.return_value = None
        mock_jm_inst._get_patient_mapping.return_value = None
        mock_jm_inst._get_reference_pid_mapping.return_value = None
        mock_jm_inst._get_appointment_mapping.return_value = None
        mock_pir.return_value.transform.return_value = df
        mock_irh.return_value.handle.return_value = df
        mock_deid.return_value.apply_rules.return_value = df
        mock_handler.return_value = MagicMock()
        mock_write.apply_async = MagicMock()

        from deid.tasks.process import process_batch
        result = process_batch(config.model_dump())

    assert result["status"] == "processed"

    # Verify .proc.arrow exists and .arrow is deleted
    proc_path = Path(staging_root) / "t1" / "batch_1_5.proc.arrow"
    arrow_path = Path(staging_root) / "t1" / "batch_1_5.arrow"
    assert proc_path.exists()
    assert not arrow_path.exists()

    # Verify BatchState updated
    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "processed"

    # Verify write_batch dispatched
    mock_write.apply_async.assert_called_once()
