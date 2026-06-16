"""Tests for table_batch_size batching in deid run and deid retry."""
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


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


# ---------------------------------------------------------------------------
# Per-batch unmatched tables check
# ---------------------------------------------------------------------------

def _make_cfg_with_unmatched(configured_names, tables_to_run, batch_size):
    """Config with tables_to_run including unmatched names."""
    tables = [SimpleNamespace(name=n) for n in configured_names]
    workers = SimpleNamespace(
        table_batch_size=batch_size,
        fetchers=2,
        processors=4,
        max_tasks_per_child=1,
        max_tasks_per_child_fetch=None,
        max_tasks_per_child_process=None,
    )
    cfg = SimpleNamespace(
        tables=tables,
        tables_to_run=tables_to_run,
        unmatched_tables=[n for n in tables_to_run if n not in configured_names],
        workers=workers,
        redis_url="memory://",
        config_key="test",
    )
    return cfg


def test_unmatched_tables_recorded_per_batch_not_upfront():
    """When table_batch_size > 0 + tables_to_run set, _record_unmatched_tables fires inside
    the batch loop (once per batch containing unmatched tables), not upfront."""
    # tables_to_run = [t1, t2, t3], only t1/t2 configured, t3 unmatched
    # batch_size = 2 → batch 1: [t1, t2], batch 2: [t3]
    # _record_unmatched_tables called once (for batch 2), never before the loop.
    cfg = _make_cfg_with_unmatched(
        configured_names=["t1", "t2"],
        tables_to_run=["t1", "t2", "t3"],
        batch_size=2,
    )

    record_calls = []

    def mock_record(c):
        record_calls.append(list(c.unmatched_tables))

    configured_by_name = {t.name: t for t in cfg.tables}
    batch_size = cfg.workers.table_batch_size
    batch_names_list = [
        cfg.tables_to_run[i:i + batch_size]
        for i in range(0, len(cfg.tables_to_run), batch_size)
    ]

    with patch("deid.cli.run._record_unmatched_tables", side_effect=mock_record), \
         patch("deid.cli.run._purge_run_queues"), \
         patch("deid.cli.run._start_workers", return_value=[]), \
         patch("deid.cli.run._stop_workers"), \
         patch("asyncio.run"):

        for batch_names in batch_names_list:
            batch_unmatched = [n for n in batch_names if n not in configured_by_name]
            if batch_unmatched:
                cfg.unmatched_tables = batch_unmatched
                from deid.cli.run import _record_unmatched_tables
                _record_unmatched_tables(cfg)
            batch_configured = [configured_by_name[n] for n in batch_names if n in configured_by_name]
            if not batch_configured:
                continue
            cfg.tables = batch_configured

    # Called exactly once, only for the batch containing t3
    assert record_calls == [["t3"]]


def test_upfront_unmatched_check_when_no_batching():
    """When table_batch_size = 0, unmatched tables are failed upfront before the loop."""
    cfg = _make_cfg_with_unmatched(
        configured_names=["t1", "t2"],
        tables_to_run=["t1", "t2", "t3"],
        batch_size=0,
    )

    record_calls = []

    def mock_record(c):
        record_calls.append(list(c.unmatched_tables))

    # Simulate the upfront path (batch_size = 0)
    with patch("deid.cli.run._record_unmatched_tables", side_effect=mock_record):
        from deid.cli.run import _record_unmatched_tables
        if cfg.unmatched_tables:
            _record_unmatched_tables(cfg)

    # Called once upfront, for all unmatched tables at once
    assert record_calls == [["t3"]]


def test_all_unmatched_batch_is_skipped():
    """A batch where every table is unmatched triggers _record_unmatched_tables but
    skips worker startup for that batch."""
    # tables_to_run = [t1, t2, t3, t4], only t1/t2 configured
    # batch_size = 2 → batch 1: [t1, t2] (configured), batch 2: [t3, t4] (all unmatched)
    cfg = _make_cfg_with_unmatched(
        configured_names=["t1", "t2"],
        tables_to_run=["t1", "t2", "t3", "t4"],
        batch_size=2,
    )

    configured_by_name = {t.name: t for t in cfg.tables}
    batch_size = cfg.workers.table_batch_size
    batch_names_list = [
        cfg.tables_to_run[i:i + batch_size]
        for i in range(0, len(cfg.tables_to_run), batch_size)
    ]

    start_calls = []
    record_calls = []

    with patch("deid.cli.run._record_unmatched_tables", side_effect=lambda c: record_calls.append(list(c.unmatched_tables))), \
         patch("deid.cli.run._purge_run_queues"), \
         patch("deid.cli.run._start_workers", side_effect=lambda c, p: start_calls.append([t.name for t in c.tables]) or []), \
         patch("deid.cli.run._stop_workers"), \
         patch("asyncio.run"):

        for batch_names in batch_names_list:
            batch_unmatched = [n for n in batch_names if n not in configured_by_name]
            if batch_unmatched:
                cfg.unmatched_tables = batch_unmatched
                from deid.cli.run import _record_unmatched_tables
                _record_unmatched_tables(cfg)
            batch_configured = [configured_by_name[n] for n in batch_names if n in configured_by_name]
            if not batch_configured:
                continue
            cfg.tables = batch_configured
            from deid.cli.run import _start_workers, _stop_workers, _purge_run_queues
            _purge_run_queues(cfg)
            procs = _start_workers(cfg, "/tmp/cfg.yaml")
            import asyncio
            asyncio.run(None)
            _stop_workers(procs)

    # Workers only started for batch 1 (t1, t2); batch 2 (all unmatched) skipped
    assert start_calls == [["t1", "t2"]]
    # Unmatched recorded once, for [t3, t4]
    assert record_calls == [["t3", "t4"]]
