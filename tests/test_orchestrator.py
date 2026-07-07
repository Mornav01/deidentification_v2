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
    mock_handler.get_exact_row_count.return_value = 50000
    mock_handler.get_min_max_id.return_value = (1, 50000)

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        batches = s.query(BatchState).filter_by(table_name="patients").order_by(BatchState.start_id).all()
        # 50000 rows / 10000 batch_size = 5 batches
        assert len(batches) == 5
        assert all(b.status == "pending" for b in batches)
        assert batches[0].start_id == 0
        assert batches[0].end_id == 9999
        assert batches[-1].start_id == 40000
        assert batches[-1].end_id == 49999


def test_setup_phase_small_table(tmp_path):
    """Tables smaller than batch_size get a single BatchState row."""
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[TableConfig(name="small_table", rules={"col": "MASK"})],
    )

    mock_handler = MagicMock()
    mock_handler.get_exact_row_count.return_value = 100  # less than batch_size=10000

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        batches = s.query(BatchState).filter_by(table_name="small_table").all()
        # 100 rows < batch_size=10000 → one batch covering offset 0..9999
        assert len(batches) == 1
        assert batches[0].start_id == 0
        assert batches[0].end_id == 9999


def test_setup_phase_mixed_tables_passthrough_and_phi(tmp_path):
    """
    Verify table_batch_size=5 scenario: within one batch that contains both pass-through
    (no PHI) and regular (PHI) tables, _setup_phase completes the pass-through tables
    immediately (TableState=completed, no BatchState) and registers BatchState rows only
    for the PHI tables.  The batch pipeline then runs only on the PHI tables.

    Real scenario: 100 tables, table_batch_size=5.
    cli/run.py splits all 100 into 20 sequential rounds of 5.
    Each round calls _setup_phase → _deidentify_phase on exactly 5 tables.
    Within a round:
      - Tables with rules={} → copied in _setup_phase, never reach _deidentify_phase.
      - Tables with rules    → BatchState rows created, dispatched through Celery pipeline.
    No cross-round interleaving: round N+1 only starts after round N finishes.
    """
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    # One batch of 5 tables (as cli/run.py would slice): 2 pass-through + 3 PHI.
    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[
            TableConfig(name="lookup_codes",   rules={}),          # pass-through
            TableConfig(name="ref_providers",  rules={}),          # pass-through
            TableConfig(name="patient_visits", rules={"pid": "PATIENT_PATIENTID"}),
            TableConfig(name="orders",         rules={"enc": "ENCOUNTER_ID"}),
            TableConfig(name="notes",          rules={"note": "NLP"}),
        ],
    )

    mock_handler = MagicMock()
    mock_handler.get_exact_row_count.return_value = 500
    mock_handler.stream_table_as_dataframes.return_value = iter([])

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState, TableState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        # Pass-through tables: completed immediately, no BatchState rows.
        for tname in ("lookup_codes", "ref_providers"):
            ts = s.query(TableState).filter_by(table_name=tname).first()
            assert ts is not None, f"{tname}: TableState missing"
            assert ts.status == "completed", f"{tname}: expected completed, got {ts.status}"
            batches = s.query(BatchState).filter_by(table_name=tname).all()
            assert len(batches) == 0, f"{tname}: should have 0 BatchState rows"

        # PHI tables: BatchState rows created (pending), TableState is pending.
        for tname in ("patient_visits", "orders", "notes"):
            ts = s.query(TableState).filter_by(table_name=tname).first()
            assert ts is not None, f"{tname}: TableState missing"
            assert ts.status == "pending", f"{tname}: expected pending, got {ts.status}"
            batches = s.query(BatchState).filter_by(table_name=tname).all()
            assert len(batches) > 0, f"{tname}: should have BatchState rows for Celery pipeline"


def test_setup_phase_passthrough_table_skips_batch_pipeline(tmp_path):
    """A table with no PHI rules is copied directly in setup; no BatchState rows are created."""
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    # Cross-server topology: source MySQL 3306, dest PostgreSQL 5432 (different ports).
    # This triggers the stream-copy path in _setup_phase.
    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[TableConfig(name="ref_data", rules={})],  # no PHI rules
    )

    mock_handler = MagicMock()
    mock_handler.get_exact_row_count.return_value = 200
    mock_handler.stream_table_as_dataframes.return_value = iter([])

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState, TableState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        # No batches — the batch pipeline was bypassed entirely.
        batches = s.query(BatchState).filter_by(table_name="ref_data").all()
        assert len(batches) == 0

        # TableState is marked completed (not pending).
        ts = s.query(TableState).filter_by(table_name="ref_data").first()
        assert ts is not None
        assert ts.status == "completed"


def test_setup_phase_passthrough_copy_failure_marks_failed_not_celery(tmp_path):
    """
    When the direct copy of a no-PHI table fails, the table must be marked 'failed'
    in state.db with no BatchState rows — it must NOT fall through to the Celery
    batch pipeline.

    Scenario: 3 PHI tables + 2 no-PHI tables.  One no-PHI table raises during copy.
    Expected:
      - failed no-PHI table  → TableState.status='failed', 0 BatchState rows
      - passing no-PHI table → TableState.status='completed', 0 BatchState rows
      - all 3 PHI tables     → TableState.status='pending', BatchState rows present
    """
    import asyncio
    from unittest.mock import patch, MagicMock
    from deid.config.schema import TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        mappings_db_path=str(tmp_path / "mappings.db"),
        tables=[
            TableConfig(name="ref_ok",     rules={}),              # pass-through, succeeds
            TableConfig(name="ref_broken", rules={}),              # pass-through, fails
            TableConfig(name="patients",   rules={"pid": "PATIENT_PATIENTID"}),
            TableConfig(name="orders",     rules={"enc": "ENCOUNTER_ID"}),
            TableConfig(name="notes",      rules={"note": "NLP"}),
        ],
    )

    call_count = {"stream": 0}

    def _stream_side_effect(table_name, batch_size):
        call_count["stream"] += 1
        if table_name == "ref_broken":
            raise ConnectionError("simulated DB failure")
        return iter([])

    mock_handler = MagicMock()
    mock_handler.get_exact_row_count.return_value = 100
    mock_handler.stream_table_as_dataframes.side_effect = _stream_side_effect

    from deid.models.base import create_state_engine, create_all_state_tables
    state_engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(state_engine)

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler):
        from deid.orchestrator.async_runner import _setup_phase
        asyncio.run(_setup_phase(config, state_engine))

    from deid.models.state import BatchState, TableState
    from sqlalchemy.orm import Session
    with Session(state_engine) as s:
        # ref_ok: pass-through succeeded
        ts_ok = s.query(TableState).filter_by(table_name="ref_ok").first()
        assert ts_ok is not None
        assert ts_ok.status == "completed"
        assert s.query(BatchState).filter_by(table_name="ref_ok").count() == 0

        # ref_broken: copy failed → marked failed, NOT in Celery pipeline
        ts_broken = s.query(TableState).filter_by(table_name="ref_broken").first()
        assert ts_broken is not None
        assert ts_broken.status == "failed", f"expected failed, got {ts_broken.status}"
        assert ts_broken.failure_remarks is not None
        assert s.query(BatchState).filter_by(table_name="ref_broken").count() == 0

        # PHI tables: unaffected — still have BatchState rows
        for tname in ("patients", "orders", "notes"):
            ts = s.query(TableState).filter_by(table_name=tname).first()
            assert ts is not None
            assert ts.status == "pending", f"{tname}: expected pending, got {ts.status}"
            assert s.query(BatchState).filter_by(table_name=tname).count() > 0
