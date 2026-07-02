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
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

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
        state_db_url=state_db_url,
        mapping_db_config={"connection_str": "sqlite:///mappings.db"},
        table_details={"columns_details": [{"column_name": "col1", "de_identification_rule": "HASH", "is_phi": False}]},
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

        mock_ref.return_value.join_dataframe.return_value = (df, ([], {}, [], [], []))
        mock_jm_inst = MagicMock()
        mock_jm.return_value = mock_jm_inst
        mock_jm_inst.get_possible_patient_identifier_columns.return_value = ([], None)
        mock_jm_inst.df = df
        mock_jm_inst._get_distinct_encounterids.return_value = []
        mock_jm_inst._get_distinct_referencepids.return_value = []
        mock_jm_inst._get_distinct_appointmentids.return_value = []
        mock_jm_inst._get_distinct_chartids.return_value = []
        mock_jm_inst._get_encounter_mapping.return_value = None
        mock_jm_inst._get_reference_pid_mapping.return_value = None
        mock_jm_inst._get_appointment_mapping.return_value = None
        mock_jm_inst._get_chart_mapping.return_value = None
        mock_pir.return_value.transform.return_value = df
        mock_irh.return_value.handle.return_value = df
        mock_deid.return_value.apply_rules.return_value = df
        mock_handler.return_value = MagicMock()
        mock_write.apply_async = MagicMock()

        from deid.tasks.process import process_batch
        result = process_batch(config.model_dump())

    assert result["status"] == "processed"

    # Verify .proc.arrow exists and .arrow is deleted
    proc_path = Path(staging_root) / "default" / "t1" / "batch_1_5.proc.arrow"
    arrow_path = Path(staging_root) / "default" / "t1" / "batch_1_5.arrow"
    assert proc_path.exists()
    assert not arrow_path.exists()

    # Verify BatchState updated
    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "processed"

    # Verify write_batch dispatched
    mock_write.apply_async.assert_called_once()


def test_process_batch_columns_details_fallback_from_state_db(tmp_path, state_engine):
    """When columns_details is missing from the task message, rules are recovered from state.db."""
    from deid.models.state import BatchState, TableState
    from deid.config.task_models import ProcessTaskConfig
    from sqlalchemy.orm import Session

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    from deid.models.state import DbConfig
    with Session(state_engine) as s:
        db_cfg = DbConfig(name="default", source_conn_str="sqlite:///src.db", dest_conn_str="sqlite:///dst.db")
        s.add(db_cfg)
        s.flush()
        s.add(BatchState(table_name="t1", start_id=1, end_id=5, status="fetched"))
        # TableState carries the rules that the task message lost.
        s.add(TableState(db_config_id=db_cfg.id, table_name="t1", config_key="default",
                         status="pending", rules_config={"col1": "HASH"}))
        s.commit()

    df = pl.DataFrame({"nd_auto_increment_id": [1, 2, 3, 4, 5], "col1": ["a", "b", "c", "d", "e"]})
    _write_fetched_arrow(staging_root, "t1", 1, 5, df)

    # columns_details is None — simulates a lost/evicted task message payload.
    config = ProcessTaskConfig(
        table_name="t1",
        start_id=1,
        end_id=5,
        staging_root=staging_root,
        state_db_url=state_db_url,
        mapping_db_config={"connection_str": "sqlite:///mappings.db"},
        table_details={"columns_details": None},
        source_conn_str="sqlite:///src.db",
    )

    with patch("deid.tasks.process.ReferenceMappingDataFrameJoiner") as mock_ref, \
         patch("deid.tasks.process.JoinMapping") as mock_jm, \
         patch("deid.tasks.process.PatientIdentifierResolver") as mock_pir, \
         patch("deid.tasks.process.InvalidRowHandler") as mock_irh, \
         patch("deid.tasks.process.DeIdentifier") as mock_deid, \
         patch("deid.tasks.process.NDDBHandler") as mock_handler, \
         patch("deid.tasks.write.write_batch") as mock_write:

        mock_ref.return_value.join_dataframe.return_value = (df, ([], {}, [], [], []))
        mock_jm_inst = MagicMock()
        mock_jm.return_value = mock_jm_inst
        mock_jm_inst.get_possible_patient_identifier_columns.return_value = ([], None)
        mock_jm_inst.df = df
        for m in ["_get_distinct_encounterids", "_get_distinct_referencepids",
                  "_get_distinct_appointmentids", "_get_distinct_chartids"]:
            getattr(mock_jm_inst, m).return_value = []
        for m in ["_get_encounter_mapping", "_get_reference_pid_mapping",
                  "_get_appointment_mapping", "_get_chart_mapping"]:
            getattr(mock_jm_inst, m).return_value = None
        mock_pir.return_value.transform.return_value = df
        mock_irh.return_value.handle.return_value = df
        mock_deid.return_value.apply_rules.return_value = df
        mock_handler.return_value = MagicMock()
        mock_write.apply_async = MagicMock()

        from deid.tasks.process import process_batch
        result = process_batch(config.model_dump())

    # Batch completed — fallback recovery worked.
    assert result["status"] == "processed"


def test_process_batch_refuses_when_no_rules_anywhere(tmp_path, state_engine):
    """RuntimeError is raised when columns_details is missing and state.db has no rules either."""
    import pytest
    from deid.models.state import BatchState
    from deid.config.task_models import ProcessTaskConfig
    from sqlalchemy.orm import Session

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

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
        state_db_url=state_db_url,
        mapping_db_config={"connection_str": "sqlite:///mappings.db"},
        table_details={"columns_details": None},
        source_conn_str="sqlite:///src.db",
    )

    from deid.tasks.process import process_batch
    with pytest.raises(RuntimeError, match="No PHI rules found"):
        process_batch(config.model_dump())


def test_process_batch_falls_back_to_sql_joins_when_preload_incomplete(tmp_path, state_engine):
    """When preloaded data is missing a required mapping, JoinMapping (SQL) is used instead."""
    import polars as pl
    from deid.models.state import BatchState
    from deid.config.task_models import ProcessTaskConfig
    from sqlalchemy.orm import Session

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=5, status="fetched"))
        s.commit()

    df = pl.DataFrame({
        "nd_auto_increment_id": [1, 2, 3, 4, 5],
        "encounter_col": [10, 20, 30, 40, 50],
    })
    _write_fetched_arrow(staging_root, "t1", 1, 5, df)

    # Table has an ENCOUNTER_ID rule → requires encounter_mapping + patient_mapping.
    config = ProcessTaskConfig(
        table_name="t1",
        start_id=1,
        end_id=5,
        staging_root=staging_root,
        state_db_url=state_db_url,
        mapping_db_config={"connection_str": "sqlite:///mappings.db"},
        table_details={"columns_details": [
            {"column_name": "encounter_col", "de_identification_rule": "ENCOUNTER_ID", "is_phi": True}
        ]},
        source_conn_str="sqlite:///src.db",
    )

    # Preloaded has patient_mapping but NOT encounter_mapping → incomplete → SQL fallback.
    incomplete_preload = {"patient_mapping": pl.DataFrame({"nd_patient_id": []}), "encounter_mapping": None}

    with patch("deid.tasks.process.get_preloaded_data", return_value=incomplete_preload), \
         patch("deid.tasks.process.ReferenceMappingDataFrameJoiner") as mock_ref, \
         patch("deid.tasks.process.JoinMapping") as mock_jm, \
         patch("deid.tasks.process.PatientIdentifierResolver") as mock_pir, \
         patch("deid.tasks.process.InvalidRowHandler") as mock_irh, \
         patch("deid.tasks.process.DeIdentifier") as mock_deid, \
         patch("deid.tasks.process.NDDBHandler") as mock_handler, \
         patch("deid.tasks.write.write_batch") as mock_write:

        mock_ref.return_value.join_dataframe.return_value = (df, (["encounter_col"], {}, [], [], []))
        mock_jm_inst = MagicMock()
        mock_jm.return_value = mock_jm_inst
        mock_jm_inst.get_possible_patient_identifier_columns.return_value = ([], None)
        mock_jm_inst.df = df
        for m in ["_get_distinct_encounterids", "_get_distinct_referencepids",
                  "_get_distinct_appointmentids", "_get_distinct_chartids"]:
            getattr(mock_jm_inst, m).return_value = []
        for m in ["_get_encounter_mapping", "_get_reference_pid_mapping",
                  "_get_appointment_mapping", "_get_chart_mapping"]:
            getattr(mock_jm_inst, m).return_value = None
        mock_pir.return_value.transform.return_value = df
        mock_irh.return_value.handle.return_value = df
        mock_deid.return_value.apply_rules.return_value = df
        mock_handler.return_value = MagicMock()
        mock_write.apply_async = MagicMock()

        from deid.tasks.process import process_batch
        process_batch(config.model_dump())

    # JoinMapping (SQL fallback) must have been instantiated — not the preloaded path.
    mock_jm.assert_called_once()
