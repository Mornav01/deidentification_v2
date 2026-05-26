"""Tests for fetch_batch task."""
import json
import polars as pl
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


def test_fetch_batch_writes_arrow_and_updates_state(tmp_path, state_engine):
    from deid.models.state import BatchState
    from deid.config.task_models import FetchTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    # Pre-create BatchState row
    with Session(state_engine) as s:
        s.add(BatchState(table_name="patients", start_id=1, end_id=5, status="pending"))
        s.commit()

    config = FetchTaskConfig(
        table_name="patients",
        start_id=1,
        end_id=5,
        source_conn_str="sqlite:///test.db",
        state_db_url=state_db_url,
        staging_root=staging_root,
    )

    mock_df = pl.DataFrame({
        "nd_auto_increment_id": [1, 2, 3, 4, 5],
        "name": ["a", "b", "c", "d", "e"],
    })

    def mock_keyset(handler, table_name, batch_size, last_id=None, id_column="nd_auto_increment_id"):
        yield mock_df

    with patch("deid.tasks.fetch.NDDBHandler") as mock_handler_cls, \
         patch("deid.tasks.fetch.stream_table_keyset", side_effect=mock_keyset), \
         patch("deid.tasks.process.process_batch") as mock_process:
        mock_handler = MagicMock()
        mock_handler.get_columns.return_value = [
            {"name": "nd_auto_increment_id", "type": MagicMock(length=None)},
            {"name": "name", "type": MagicMock(length=100)},
        ]
        mock_handler_cls.return_value = mock_handler
        mock_process.apply_async = MagicMock()

        from deid.tasks.fetch import fetch_batch
        result = fetch_batch(config.model_dump())

    assert result["status"] == "fetched"

    # Verify arrow file was written
    arrow_path = Path(staging_root) / "default" / "patients" / "batch_1_5.arrow"
    assert arrow_path.exists()
    df = pl.read_ipc(arrow_path)
    assert df.height == 5

    # Verify column schema in metadata
    import pyarrow.ipc as ipc
    reader = ipc.open_file(str(arrow_path))
    schema_json = reader.schema.metadata[b"deid_column_schema"]
    schema = json.loads(schema_json)
    assert "name" in schema

    # Verify BatchState updated
    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "fetched"

    # Verify process_batch was dispatched
    mock_process.apply_async.assert_called_once()


def test_fetch_batch_empty_range_marks_done(tmp_path, state_engine):
    from deid.models.state import BatchState
    from deid.config.task_models import FetchTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    with Session(state_engine) as s:
        s.add(BatchState(table_name="empty_t", start_id=1, end_id=5, status="pending"))
        s.commit()

    config = FetchTaskConfig(
        table_name="empty_t",
        start_id=1,
        end_id=5,
        source_conn_str="sqlite:///test.db",
        state_db_url=state_db_url,
        staging_root=staging_root,
    )

    def mock_keyset_empty(handler, table_name, batch_size, last_id=None, id_column="nd_auto_increment_id"):
        return iter([])  # no data

    with patch("deid.tasks.fetch.NDDBHandler") as mock_handler_cls, \
         patch("deid.tasks.fetch.stream_table_keyset", side_effect=mock_keyset_empty):
        mock_handler = MagicMock()
        mock_handler.get_columns.return_value = []
        mock_handler_cls.return_value = mock_handler

        from deid.tasks.fetch import fetch_batch
        result = fetch_batch(config.model_dump())

    assert result["status"] == "done"
    assert result["rows"] == 0

    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "done"


def test_fetch_batch_failure_resets_batch_via_reset_or_fail(tmp_path, state_engine):
    from deid.models.state import BatchState
    from deid.config.task_models import FetchTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    with Session(state_engine) as s:
        s.add(BatchState(table_name="fail_t", start_id=1, end_id=5, status="pending"))
        s.commit()

    config = FetchTaskConfig(
        table_name="fail_t",
        start_id=1,
        end_id=5,
        source_conn_str="sqlite:///test.db",
        state_db_url=state_db_url,
        staging_root=staging_root,
        run_config={"max_batch_retries": 1},
    )

    with patch("deid.tasks.fetch._fetch_batch_inner", side_effect=RuntimeError("simulated fetch failure")):
        from deid.tasks.fetch import fetch_batch
        with pytest.raises(RuntimeError, match="simulated fetch failure"):
            fetch_batch(config.model_dump())

    with Session(state_engine) as s:
        batch = s.query(BatchState).filter_by(table_name="fail_t").first()
        assert batch.status == "failed"
        assert batch.retry_count == 1
        assert "simulated fetch failure" in (batch.last_failed_reason or "")


def test_fetch_batch_connection_error_gets_more_retries(tmp_path, state_engine):
    """pymssql EOF errors use max_retries=10, not 3 — batch stays pending after first hit."""
    from deid.models.state import BatchState
    from deid.config.task_models import FetchTaskConfig

    staging_root = str(tmp_path / ".deid_staging")
    state_db_url = f"sqlite:///{tmp_path / 'state.db'}"

    with Session(state_engine) as s:
        s.add(BatchState(table_name="conn_err_t", start_id=1, end_id=5, status="pending"))
        s.commit()

    config = FetchTaskConfig(
        table_name="conn_err_t",
        start_id=1,
        end_id=5,
        source_conn_str="sqlite:///test.db",
        state_db_url=state_db_url,
        staging_root=staging_root,
        # default max_batch_retries not set → default 3, but connection error bumps to 10
    )

    conn_exc = RuntimeError(
        "(20017, b'DB-Lib error message 20017, severity 9:\\nUnexpected EOF from the server\\n')"
    )

    with patch("deid.tasks.fetch._fetch_batch_inner", side_effect=conn_exc):
        from deid.tasks.fetch import fetch_batch
        with pytest.raises(RuntimeError):
            fetch_batch(config.model_dump())

    with Session(state_engine) as s:
        batch = s.query(BatchState).filter_by(table_name="conn_err_t").first()
        # With max_retries bumped to 10, first failure → still "pending" (retry_count=1 < 10)
        assert batch.status == "pending"
        assert batch.retry_count == 1
