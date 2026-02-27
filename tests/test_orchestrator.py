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
