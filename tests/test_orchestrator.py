"""Tests for orchestrator async_runner."""
import pytest


def _make_config(**overrides):
    from deid.config.schema import (
        DeidConfig, DbConfig, DeidentificationSettings,
        TableConfig, WorkerSettings, QCSettings,
    )
    defaults = dict(
        source_db=DbConfig(type="mysql", host="localhost", port=3306, database="src", username="u", password="p"),
        destination_db=DbConfig(type="postgresql", host="localhost", port=5432, database="dest", username="u", password="p"),
        tables=[TableConfig(name="small_table", rules={"col1": "MASK"})],
        mapping_tables={},
        workers=WorkerSettings(),
        qc=QCSettings(),
    )
    defaults.update(overrides)
    return DeidConfig(**defaults)


@pytest.fixture(autouse=True)
def _setup_celery():
    """Ensure a Celery app exists so shared_task can bind."""
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(broker_url="memory://", result_backend="cache+memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    app.finalize()
    app.loader.import_default_modules()
    return app


def test_setup_phase_populates_mappings(tmp_path):
    """_setup_phase should call populate_mappings at the end."""
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[TableConfig(name="patients", rules={"PID": "PATIENT_ID", "Name": "MASK"})],
    )

    mock_handler = MagicMock()
    mock_handler.get_rows_count.return_value = 100
    mock_handler.get_min_max_id.return_value = None

    mock_populate = MagicMock(return_value={
        "patients_found": 10, "patients_created": 10,
        "encounters_found": 0, "encounters_created": 0,
        "appointments_found": 0, "appointments_created": 0,
    })

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler), \
         patch("deid.core.mapping_populator.populate_mappings", mock_populate):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    mock_populate.assert_called_once()
    call_kwargs = mock_populate.call_args.kwargs
    assert call_kwargs["patient_id_prefix"] == config.deidentification.patient_id_prefix
    assert call_kwargs["max_offset"] == config.deidentification.date_offset_days


def test_setup_phase_creates_batch_states(tmp_path):
    """_setup_phase should create BatchState rows for all tables."""
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[TableConfig(name="patients", rules={"PID": "PATIENT_ID"})],
    )

    mock_handler = MagicMock()
    mock_handler.get_rows_count.return_value = 5000
    mock_handler.get_min_max_id.return_value = (1, 5000)

    mock_populate = MagicMock(return_value={
        "patients_found": 10, "patients_created": 10,
        "encounters_found": 0, "encounters_created": 0,
        "appointments_found": 0, "appointments_created": 0,
    })

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler), \
         patch("deid.core.mapping_populator.populate_mappings", mock_populate):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        batches = s.query(BatchState).filter_by(table_name="patients").order_by(BatchState.start_id).all()
        # 5000 rows / 1000 batch_size = 5 batches
        assert len(batches) == 5
        assert all(b.status == "pending" for b in batches)
        assert batches[0].start_id == 1
        assert batches[0].end_id == 1000
        assert batches[-1].start_id == 4001
        assert batches[-1].end_id == 5000


def test_setup_phase_sentinel_for_no_id_tables(tmp_path):
    """Tables without integer IDs get a single sentinel BatchState."""
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[TableConfig(name="no_id_table", rules={"col": "MASK"})],
    )

    mock_handler = MagicMock()
    mock_handler.get_rows_count.return_value = 100
    mock_handler.get_min_max_id.return_value = None

    mock_populate = MagicMock(return_value={
        "patients_found": 0, "patients_created": 0,
        "encounters_found": 0, "encounters_created": 0,
        "appointments_found": 0, "appointments_created": 0,
    })

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler), \
         patch("deid.core.mapping_populator.populate_mappings", mock_populate):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        batches = s.query(BatchState).filter_by(table_name="no_id_table").all()
        assert len(batches) == 1
        assert batches[0].start_id == -1
        assert batches[0].end_id == -1
