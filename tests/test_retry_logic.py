"""Tests for _retry_run in deid/cli/retry.py."""
import asyncio
import pytest
from types import SimpleNamespace
from sqlalchemy.orm import Session
from unittest.mock import MagicMock, patch, AsyncMock


@pytest.fixture
def state_engine(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(engine)
    return engine


@pytest.fixture
def retry_cfg(tmp_path):
    return SimpleNamespace(
        config_key="test",
        resolved_state_db_url=f"sqlite:///{tmp_path / 'state.db'}",
        state_db_path=str(tmp_path / "state.db"),
        tables=[SimpleNamespace(name="tbl_a"), SimpleNamespace(name="tbl_b")],
        mappings_connection_string=f"sqlite:///{tmp_path / 'mappings.db'}",
    )


def _add_batch(state_engine, table="tbl_a", start=0, end=999, status="pending",
               retry_count=0, last_failed_reason=None):
    from deid.models.state import BatchState
    with Session(state_engine) as s:
        s.add(BatchState(
            table_name=table, start_id=start, end_id=end,
            status=status, config_key="test",
            retry_count=retry_count,
            last_failed_reason=last_failed_reason,
        ))
        s.commit()


def _run_retry(cfg, state_engine, batch_failures=None, tables_filter=None):
    """
    Run _retry_run with:
    - asyncio.sleep mocked out (instant)
    - fetch_batch.apply_async mocked — each dispatch marks the batch 'done' so the
      poll loop terminates without waiting
    - _build_fetch_config returns a minimal dict
    """
    dispatched = []

    def mock_build_fetch_config(config, batch_obj, staging_root, mappings_conn_str):
        return {
            "table_name": batch_obj.table_name,
            "start_id": batch_obj.start_id,
            "end_id": batch_obj.end_id,
            "config_key": config.config_key,
        }

    def mock_apply_async(args=None, queue=None, **kwargs):
        raw = (args or [{}])[0]
        dispatched.append(raw)
        # Mark done immediately so the poll loop exits on the first iteration
        from deid.models.state import BatchState
        with Session(state_engine) as s:
            b = s.query(BatchState).filter_by(
                table_name=raw.get("table_name"),
                start_id=raw.get("start_id"),
                end_id=raw.get("end_id"),
                config_key=raw.get("config_key", "test"),
            ).first()
            if b:
                b.status = "done"
                s.commit()

    mock_task = MagicMock()
    mock_task.apply_async = mock_apply_async

    with patch("deid.tasks.fetch.fetch_batch", mock_task), \
         patch("deid.orchestrator.async_runner._build_fetch_config", side_effect=mock_build_fetch_config), \
         patch("asyncio.sleep", new=AsyncMock(return_value=None)):
        from deid.cli.retry import _retry_run
        asyncio.run(_retry_run(cfg, batch_failures or [], tables_filter=tables_filter))

    return dispatched


def test_retry_run_picks_up_pending_batches(tmp_path, state_engine, retry_cfg):
    _add_batch(state_engine, table="tbl_a", status="pending")
    dispatched = _run_retry(retry_cfg, state_engine)
    assert len(dispatched) == 1
    assert dispatched[0]["table_name"] == "tbl_a"


def test_retry_run_picks_up_dispatched_batches(tmp_path, state_engine, retry_cfg):
    _add_batch(state_engine, table="tbl_a", status="dispatched")
    dispatched = _run_retry(retry_cfg, state_engine)
    assert len(dispatched) == 1
    assert dispatched[0]["table_name"] == "tbl_a"


def test_retry_run_picks_up_failed_batches_and_resets_retry_count(tmp_path, state_engine, retry_cfg):
    _add_batch(state_engine, table="tbl_a", status="failed",
               retry_count=3, last_failed_reason="previous error")
    dispatched = _run_retry(retry_cfg, state_engine)
    assert len(dispatched) == 1

    from deid.models.state import BatchState
    with Session(state_engine) as s:
        row = s.query(BatchState).filter_by(table_name="tbl_a", config_key="test").first()
        assert row.retry_count == 0
        assert row.last_failed_reason is None


def test_retry_run_skips_done_batches(tmp_path, state_engine, retry_cfg):
    _add_batch(state_engine, table="tbl_a", status="done")
    dispatched = _run_retry(retry_cfg, state_engine)
    assert dispatched == []


def test_retry_run_tables_filter_respected(tmp_path, state_engine, retry_cfg):
    _add_batch(state_engine, table="tbl_a", status="pending")
    _add_batch(state_engine, table="tbl_b", status="pending")
    dispatched = _run_retry(retry_cfg, state_engine, tables_filter={"tbl_a"})
    names = [d["table_name"] for d in dispatched]
    assert "tbl_a" in names
    assert "tbl_b" not in names


def test_retry_run_no_batches_returns_cleanly(tmp_path, state_engine, retry_cfg):
    # No rows in state.db → should complete without error, 0 dispatches
    dispatched = _run_retry(retry_cfg, state_engine)
    assert dispatched == []


def test_retry_run_multiple_batches_all_dispatched(tmp_path, state_engine, retry_cfg):
    _add_batch(state_engine, table="tbl_a", start=0, end=999, status="pending")
    _add_batch(state_engine, table="tbl_a", start=1000, end=1999, status="failed")
    _add_batch(state_engine, table="tbl_b", start=0, end=999, status="dispatched")
    dispatched = _run_retry(retry_cfg, state_engine)
    assert len(dispatched) == 3
    table_names = {d["table_name"] for d in dispatched}
    assert table_names == {"tbl_a", "tbl_b"}
