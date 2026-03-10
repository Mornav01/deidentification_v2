"""Tests for orchestrator task graph builder."""
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
        workers=WorkerSettings(concurrency=2),
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


def test_build_task_graph_single_small_table():
    from deid.orchestrator.task_graph import build_task_graph

    config = _make_config()
    table_row_counts = {"small_table": 1000}
    graph = build_task_graph(config, table_row_counts)
    assert graph is not None


def test_build_task_graph_large_table_splits():
    from deid.orchestrator.task_graph import build_task_graph
    from deid.config.schema import DeidentificationSettings, TableConfig

    config = _make_config(
        deidentification=DeidentificationSettings(large_table_threshold=500, parallel_tasks_per_table=2),
        tables=[TableConfig(name="big_table", rules={"col1": "MASK"})],
    )
    table_row_counts = {"big_table": 10000}
    table_id_ranges = {"big_table": (1, 10000)}
    graph = build_task_graph(config, table_row_counts, table_id_ranges)
    assert graph is not None


def test_cache_large_tables_creates_ipc_files(tmp_path):
    """_cache_large_tables should dump large tables to Arrow IPC files."""
    import asyncio
    import os
    from unittest.mock import patch, MagicMock
    import polars as pl
    from deid.config.schema import DeidentificationSettings, TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        deidentification=DeidentificationSettings(
            large_table_threshold=500,
            batch_size=5,
        ),
        tables=[TableConfig(name="big_table", rules={"col1": "MASK"})],
    )

    table_id_ranges = {"big_table": (1, 1000)}

    def mock_paginated_stream(handler, table_name, min_id, max_id, page_size, id_column="nd_auto_increment_id"):
        for i in range(3):
            start = i * 5 + 1
            yield pl.DataFrame({
                "nd_auto_increment_id": list(range(start, start + 5)),
                "col1": [f"val_{x}" for x in range(start, start + 5)],
            })

    mock_handler = MagicMock()
    mock_handler.close = MagicMock()

    with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler), \
         patch("deid.core.dbPkg.dbhandler.stream_table_paginated", side_effect=mock_paginated_stream):
        from deid.orchestrator.async_runner import _cache_large_tables
        cache_paths = asyncio.run(_cache_large_tables(config, table_id_ranges))

    assert "big_table" in cache_paths
    cache_dir = cache_paths["big_table"]
    assert os.path.isdir(cache_dir)

    files = sorted(os.listdir(cache_dir))
    assert len(files) == 3
    assert all(f.endswith(".arrow") for f in files)

    import shutil
    shutil.rmtree(os.path.dirname(cache_dir))


def test_cache_large_tables_skips_small_tables(tmp_path):
    """Tables below threshold should not be cached."""
    import asyncio
    from deid.config.schema import DeidentificationSettings, TableConfig

    config = _make_config(
        state_db_path=str(tmp_path / "state.db"),
        deidentification=DeidentificationSettings(large_table_threshold=500),
        tables=[TableConfig(name="small_table", rules={"col1": "MASK"})],
    )

    from deid.orchestrator.async_runner import _cache_large_tables
    cache_paths = asyncio.run(_cache_large_tables(config, {}))
    assert cache_paths == {}


def test_build_task_graph_passes_cache_dir_to_range_tasks():
    from unittest.mock import patch, MagicMock
    from deid.orchestrator.task_graph import build_task_graph
    from deid.config.schema import DeidentificationSettings, TableConfig

    config = _make_config(
        deidentification=DeidentificationSettings(large_table_threshold=500, parallel_tasks_per_table=2),
        tables=[TableConfig(name="big_table", rules={"col1": "MASK"})],
    )
    table_row_counts = {"big_table": 10000}
    table_id_ranges = {"big_table": (1, 10000)}
    cache_paths = {"big_table": "/tmp/.deid_cache/big_table"}

    captured_configs = []

    def capture_s(config_dict, start, end):
        captured_configs.append(config_dict)
        return MagicMock()

    with patch("deid.tasks.deidentify.deidentify_table_range") as mock_task:
        mock_task.s = capture_s
        with patch("deid.tasks.deidentify.deidentify_table") as mock_single:
            mock_single.s = lambda x: MagicMock()
            build_task_graph(config, table_row_counts, table_id_ranges, cache_paths)

    assert len(captured_configs) == 2
    for cfg in captured_configs:
        assert cfg["cache_dir"] == "/tmp/.deid_cache/big_table"


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
