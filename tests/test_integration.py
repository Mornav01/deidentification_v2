"""
Integration smoke test — verifies the full pipeline wiring.
Uses eager Celery (no Redis) and SQLite source/destination DBs.
"""
import pytest
from pathlib import Path

import yaml


@pytest.fixture
def test_config_path(tmp_path):
    config = {
        "source_db": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_src",
            "username": "test",
            "password": "test",
        },
        "destination_db": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_dest",
            "username": "test",
            "password": "test",
        },
        "state_db_path": str(tmp_path / "state.db"),
        "mappings_db_path": str(tmp_path / "mappings.db"),
        "redis_url": "redis://localhost:6379/0",
        "deidentification": {
            "batch_size": 100,
            "date_offset_days": 34,
            "patient_id_prefix": 10000000,
        },
        "tables": [
            {"name": "test_patients", "rules": {"name": "MASK", "dob": "DATE_OFFSET"}}
        ],
        "mapping_tables": {},
        "phases": ["setup"],
        "workers": {"concurrency": 1},
        "qc": {"sample_size": 10},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(config))
    return p


def test_config_loads_and_validates(test_config_path):
    from deid.config.loader import load_config
    config = load_config(test_config_path)
    assert config.source_db.type.value == "postgresql"
    assert len(config.tables) == 1


def test_state_db_initialized(test_config_path):
    from deid.config.loader import load_config
    from deid.models.base import create_state_engine, create_all_state_tables

    config = load_config(test_config_path)
    engine = create_state_engine(config.state_db_path)
    create_all_state_tables(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    assert "table_states" in inspector.get_table_names()
    assert "run_logs" in inspector.get_table_names()


def test_celery_task_callable():
    """Verify Celery tasks can be called in eager mode."""
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(broker_url="memory://", result_backend="cache+memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    app.finalize()
    app.loader.import_default_modules()

    from deid.tasks.deidentify import deidentify_table
    # Can't run de-identification without a real DB,
    # but verify the task is callable and raises appropriately
    with pytest.raises(Exception):
        deidentify_table({
            "table_name": "test",
            "source_conn_str": "sqlite:///nonexistent.db",
            "dest_conn_str": "sqlite:///nonexistent.db",
            "batch_size": 100,
            "offset_days": 34,
        })
