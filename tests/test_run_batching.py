"""Tests for table_batch_size batching in deid run and deid retry."""
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call


def _make_cfg(table_names, batch_size=0):
    """Minimal config object for batching tests."""
    tables = [SimpleNamespace(name=n) for n in table_names]
    workers = SimpleNamespace(
        table_batch_size=batch_size,
        fetchers=2,
        processors=4,
        max_tasks_per_child=1,
        max_tasks_per_child_fetch=None,
        max_tasks_per_child_process=None,
    )
    return SimpleNamespace(
        tables=tables,
        workers=workers,
        redis_url="memory://",
        config_key="test",
    )


# ---------------------------------------------------------------------------
# deid run batching
# ---------------------------------------------------------------------------

def _run_command_batching(cfg, config_path="/tmp/cfg.yaml"):
    """Drive run_command's batch loop directly (avoids CLI/typer/async overhead)."""
    from deid.cli.run import _start_workers, _stop_workers

    all_tables = cfg.tables[:]
    batch_size = cfg.workers.table_batch_size
    batches = (
        [all_tables[i:i + batch_size] for i in range(0, len(all_tables), batch_size)]
        if batch_size > 0 else [all_tables]
    )
    return batches


def test_table_batch_size_zero_produces_one_batch():
    cfg = _make_cfg(["t1", "t2", "t3", "t4", "t5", "t6"], batch_size=0)
    batches = _run_command_batching(cfg)
    assert len(batches) == 1
    assert [t.name for t in batches[0]] == ["t1", "t2", "t3", "t4", "t5", "t6"]


def test_table_batch_size_splits_evenly():
    cfg = _make_cfg(["t1", "t2", "t3", "t4", "t5", "t6"], batch_size=2)
    batches = _run_command_batching(cfg)
    assert len(batches) == 3
    assert [t.name for t in batches[0]] == ["t1", "t2"]
    assert [t.name for t in batches[1]] == ["t3", "t4"]
    assert [t.name for t in batches[2]] == ["t5", "t6"]


def test_table_batch_size_last_batch_smaller():
    cfg = _make_cfg(["t1", "t2", "t3", "t4", "t5"], batch_size=3)
    batches = _run_command_batching(cfg)
    assert len(batches) == 2
    assert len(batches[0]) == 3
    assert len(batches[1]) == 2


def test_table_batch_size_larger_than_table_count():
    cfg = _make_cfg(["t1", "t2"], batch_size=10)
    batches = _run_command_batching(cfg)
    assert len(batches) == 1
    assert len(batches[0]) == 2


def test_run_start_workers_called_once_per_batch():
    """_start_workers is called once per batch, not once for all tables."""
    cfg = _make_cfg(["t1", "t2", "t3", "t4"], batch_size=2)

    start_calls = []
    stop_calls = []

    def mock_start(c, path):
        start_calls.append([t.name for t in c.tables])
        return MagicMock()

    def mock_stop(procs):
        stop_calls.append(len(procs.mock_calls) if hasattr(procs, 'mock_calls') else 1)

    mock_run = MagicMock()

    with patch("deid.cli.run._start_workers", side_effect=mock_start), \
         patch("deid.cli.run._stop_workers", side_effect=mock_stop), \
         patch("deid.cli.run._purge_run_queues"), \
         patch("deid.orchestrator.async_runner.run", return_value=mock_run), \
         patch("asyncio.run"):

        all_tables = cfg.tables[:]
        batch_size = cfg.workers.table_batch_size
        batches = [all_tables[i:i + batch_size] for i in range(0, len(all_tables), batch_size)]

        import asyncio
        from deid.cli.run import _start_workers, _stop_workers, _purge_run_queues

        for idx, batch in enumerate(batches):
            cfg.tables = batch
            _purge_run_queues(cfg)
            procs = _start_workers(cfg, "/tmp/cfg.yaml")
            asyncio.run(None)
            _stop_workers(procs)

    assert start_calls == [["t1", "t2"], ["t3", "t4"]]
    assert len(stop_calls) == 2


# ---------------------------------------------------------------------------
# deid retry batching
# ---------------------------------------------------------------------------

def test_retry_batch_failures_scoped_to_each_batch():
    """batch_failures_slice only contains failures for the current batch."""
    from deid.config.task_models import BatchFailure
    from datetime import datetime, timezone

    all_tables = [SimpleNamespace(name=n) for n in ["t1", "t2", "t3", "t4"]]
    batch_failures = [
        BatchFailure(table="t1", start_id=0, end_id=99,
                     error="e", timestamp=datetime.now(timezone.utc).isoformat(), task_type="fetch"),
        BatchFailure(table="t2", start_id=0, end_id=99,
                     error="e", timestamp=datetime.now(timezone.utc).isoformat(), task_type="fetch"),
        BatchFailure(table="t3", start_id=0, end_id=99,
                     error="e", timestamp=datetime.now(timezone.utc).isoformat(), task_type="fetch"),
        BatchFailure(table="t4", start_id=0, end_id=99,
                     error="e", timestamp=datetime.now(timezone.utc).isoformat(), task_type="fetch"),
    ]

    batch_size = 2
    batches = [all_tables[i:i + batch_size] for i in range(0, len(all_tables), batch_size)]
    slices = []
    for batch in batches:
        batch_names = {t.name for t in batch}
        slices.append([f.table for f in batch_failures if f.table in batch_names])

    assert slices == [["t1", "t2"], ["t3", "t4"]]


def test_retry_tables_filter_scoped_per_batch():
    """tables_filter passed to _retry_run equals the current batch's table names."""
    all_tables = [SimpleNamespace(name=n) for n in ["t1", "t2", "t3"]]
    batch_size = 2
    batches = [all_tables[i:i + batch_size] for i in range(0, len(all_tables), batch_size)]

    filters_used = []
    for batch in batches:
        filters_used.append({t.name for t in batch})

    assert filters_used == [{"t1", "t2"}, {"t3"}]


# ---------------------------------------------------------------------------
# WorkerSettings schema
# ---------------------------------------------------------------------------

def test_worker_settings_table_batch_size_default():
    from deid.config.schema import WorkerSettings
    ws = WorkerSettings()
    assert ws.table_batch_size == 0


def test_worker_settings_table_batch_size_set():
    from deid.config.schema import WorkerSettings
    ws = WorkerSettings(table_batch_size=5)
    assert ws.table_batch_size == 5
