"""Tests for write_batch task."""
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


def _write_processed_arrow(staging_root, table, start_id, end_id, df):
    from deid.staging import batch_processed_path
    path = batch_processed_path(Path(staging_root), table, start_id, end_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    arrow_table = df.to_arrow()
    meta = {b"deid_column_schema": json.dumps({
        "nd_auto_increment_id": {"type": "INTEGER", "length": None},
        "col1": {"type": "VARCHAR(100)", "length": 100},
    }).encode()}
    arrow_table = arrow_table.replace_schema_metadata(meta)
    with ipc.new_file(str(path), arrow_table.schema) as writer:
        writer.write_table(arrow_table)
    return path


def test_write_batch_inserts_and_updates_state(tmp_path, state_engine):
    from deid.models.state import BatchState
    from deid.config.task_models import WriteTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=5, status="processed"))
        s.commit()

    df = pl.DataFrame({"nd_auto_increment_id": [1, 2, 3, 4, 5], "col1": ["a", "b", "c", "d", "e"]})
    _write_processed_arrow(staging_root, "t1", 1, 5, df)

    config = WriteTaskConfig(
        table_name="t1",
        start_id=1,
        end_id=5,
        staging_root=staging_root,
        state_db_url=state_db_url,
        dest_conn_str="sqlite:///dest.db",
    )

    with patch("deid.tasks.write.NDDBHandler") as mock_handler_cls:
        mock_handler = MagicMock()
        mock_handler._qi = lambda x: f"`{x}`"
        mock_handler_cls.return_value = mock_handler

        from deid.tasks.write import write_batch
        result = write_batch(config.model_dump())

    assert result["status"] == "done"

    # Verify .proc.arrow deleted
    proc_path = Path(staging_root) / "t1" / "batch_1_5.proc.arrow"
    assert not proc_path.exists()

    # Verify BatchState updated
    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "done"

    # Verify dest DB handler was called
    mock_handler.engine.begin.assert_called()


def test_write_batch_failure_resets_batch_via_reset_or_fail(tmp_path, state_engine):
    from deid.models.state import BatchState
    from deid.config.task_models import WriteTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    with Session(state_engine) as s:
        s.add(BatchState(table_name="wfail", start_id=1, end_id=5, status="processed"))
        s.commit()

    df = pl.DataFrame({"nd_auto_increment_id": [1, 2, 3, 4, 5], "col1": ["a", "b", "c", "d", "e"]})
    _write_processed_arrow(staging_root, "wfail", 1, 5, df)

    config = WriteTaskConfig(
        table_name="wfail",
        start_id=1,
        end_id=5,
        staging_root=staging_root,
        state_db_url=state_db_url,
        dest_conn_str="sqlite:///dest.db",
        run_config={"max_batch_retries": 1},
    )

    with patch("deid.tasks.write._write_batch_inner", side_effect=RuntimeError("simulated write failure")):
        from deid.tasks.write import write_batch
        with pytest.raises(RuntimeError, match="simulated write failure"):
            write_batch(config.model_dump())

    with Session(state_engine) as s:
        batch = s.query(BatchState).filter_by(table_name="wfail").first()
        assert batch.status == "failed"
        assert batch.retry_count == 1
        assert "simulated write failure" in (batch.last_failed_reason or "")
