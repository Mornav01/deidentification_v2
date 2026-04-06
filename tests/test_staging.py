"""Tests for staging directory helpers."""
import os
import polars as pl
import pytest
from pathlib import Path
from sqlalchemy.orm import Session


@pytest.fixture
def state_engine(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(engine)
    return engine


@pytest.fixture
def staging_root(tmp_path):
    root = tmp_path / ".deid_staging"
    root.mkdir()
    return root


def test_staging_root_path():
    from deid.staging import get_staging_root
    root = get_staging_root("/some/path/state.db")
    assert root == Path("/some/path/.deid_staging")


def test_batch_fetched_path(staging_root):
    from deid.staging import batch_fetched_path
    p = batch_fetched_path(staging_root, "patients", 1, 1000)
    assert p == staging_root / "default" / "patients" / "batch_1_1000.arrow"


def test_batch_processed_path(staging_root):
    from deid.staging import batch_processed_path
    p = batch_processed_path(staging_root, "patients", 1, 1000)
    assert p == staging_root / "default" / "patients" / "batch_1_1000.proc.arrow"


def test_atomic_write_arrow(staging_root):
    from deid.staging import atomic_write_arrow
    target = staging_root / "patients" / "batch_1_1000.arrow"
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    atomic_write_arrow(df, target)

    assert target.exists()
    assert not target.with_suffix(".arrow.tmp").exists()
    result = pl.read_ipc(target)
    assert result.height == 3


def test_atomic_write_arrow_creates_parent_dirs(staging_root):
    from deid.staging import atomic_write_arrow
    target = staging_root / "new_table" / "batch_1_100.arrow"
    df = pl.DataFrame({"id": [1]})

    atomic_write_arrow(df, target)
    assert target.exists()


def test_cleanup_tmp_files(staging_root):
    from deid.staging import cleanup_tmp_files

    # Create valid and tmp files
    table_dir = staging_root / "patients"
    table_dir.mkdir()
    (table_dir / "batch_1_1000.arrow").write_bytes(b"valid")
    (table_dir / "batch_1_1000.arrow.tmp").write_bytes(b"stale")
    (table_dir / "batch_2001_3000.proc.arrow.tmp").write_bytes(b"stale")

    cleanup_tmp_files(staging_root)

    assert (table_dir / "batch_1_1000.arrow").exists()
    assert not (table_dir / "batch_1_1000.arrow.tmp").exists()
    assert not (table_dir / "batch_2001_3000.proc.arrow.tmp").exists()


def test_reconcile_fetched_with_both_files(state_engine, staging_root):
    """fetched + .arrow + .proc.arrow -> delete .arrow, advance to processed."""
    from deid.models.state import BatchState
    from deid.staging import reconcile, batch_fetched_path, batch_processed_path

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=100, status="fetched"))
        s.commit()

    # Both files exist (kill between proc rename and .arrow delete)
    fetched = batch_fetched_path(staging_root, "t1", 1, 100)
    processed = batch_processed_path(staging_root, "t1", 1, 100)
    fetched.parent.mkdir(parents=True, exist_ok=True)
    fetched.write_bytes(b"data")
    processed.write_bytes(b"data")

    reconcile(state_engine, staging_root)

    assert not fetched.exists()
    assert processed.exists()
    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "processed"


def test_reconcile_fetched_no_arrow_has_proc(state_engine, staging_root):
    """fetched + no .arrow + has .proc.arrow -> advance to processed."""
    from deid.models.state import BatchState
    from deid.staging import reconcile, batch_processed_path

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=100, status="fetched"))
        s.commit()

    proc = batch_processed_path(staging_root, "t1", 1, 100)
    proc.parent.mkdir(parents=True, exist_ok=True)
    proc.write_bytes(b"data")

    reconcile(state_engine, staging_root)

    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "processed"


def test_reconcile_fetched_no_files(state_engine, staging_root):
    """fetched + no .arrow + no .proc.arrow -> reset to pending."""
    from deid.models.state import BatchState
    from deid.staging import reconcile

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=100, status="fetched"))
        s.commit()

    reconcile(state_engine, staging_root)

    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "pending"


def test_reconcile_processed_no_proc_file(state_engine, staging_root):
    """processed + no .proc.arrow -> reset to pending."""
    from deid.models.state import BatchState
    from deid.staging import reconcile

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=100, status="processed"))
        s.commit()

    reconcile(state_engine, staging_root)

    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "pending"


def test_reconcile_skips_done(state_engine, staging_root):
    """done batches are not touched."""
    from deid.models.state import BatchState
    from deid.staging import reconcile

    with Session(state_engine) as s:
        s.add(BatchState(table_name="t1", start_id=1, end_id=100, status="done"))
        s.commit()

    reconcile(state_engine, staging_root)

    with Session(state_engine) as s:
        batch = s.query(BatchState).first()
        assert batch.status == "done"
