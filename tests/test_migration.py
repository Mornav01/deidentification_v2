"""Tests for _migrate_batch_state_columns additive migration in deid/models/base.py."""
import pytest


@pytest.fixture
def state_engine(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(engine)
    return engine


def test_migration_adds_missing_columns(tmp_path):
    from deid.models.base import create_state_engine, _migrate_batch_state_columns
    from sqlalchemy import text, inspect as _inspect

    engine = create_state_engine(str(tmp_path / "legacy.db"))
    # Simulate a pre-migration DB: create batch_states without the new columns
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE batch_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name VARCHAR(255) NOT NULL,
                config_key VARCHAR(100) DEFAULT 'default',
                start_id INTEGER NOT NULL,
                end_id INTEGER NOT NULL,
                status VARCHAR(50) DEFAULT 'pending'
            )
        """))

    _migrate_batch_state_columns(engine)

    insp = _inspect(engine)
    col_names = {c["name"] for c in insp.get_columns("batch_states")}
    assert "retry_count" in col_names
    assert "last_failed_reason" in col_names


def test_migration_idempotent(state_engine):
    from deid.models.base import _migrate_batch_state_columns
    from sqlalchemy import inspect as _inspect

    # Fresh DB already has both columns from create_all_state_tables; run again — no error
    _migrate_batch_state_columns(state_engine)
    _migrate_batch_state_columns(state_engine)

    insp = _inspect(state_engine)
    col_names = {c["name"] for c in insp.get_columns("batch_states")}
    assert "retry_count" in col_names
    assert "last_failed_reason" in col_names


def test_migration_skips_if_no_batch_states_table(tmp_path):
    from deid.models.base import create_state_engine, _migrate_batch_state_columns

    engine = create_state_engine(str(tmp_path / "empty.db"))
    # batch_states table does not exist; migration must be a no-op
    _migrate_batch_state_columns(engine)
