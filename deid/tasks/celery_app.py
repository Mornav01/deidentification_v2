"""Celery application factory."""
from __future__ import annotations

import logging
import os

from celery import Celery
from celery.signals import worker_process_init
from pydantic import validate_call

_TASK_MODULES = [
    "deid.tasks.fetch",
    "deid.tasks.process",
    "deid.tasks.write",
    "deid.tasks.qc",
]

# Module-level app instance (lazy-configured)
_app: Celery | None = None


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_celery_app(
    broker_url: str = "redis://localhost:6379/0",
    result_backend: str | None = None,
) -> Celery:
    global _app
    app = Celery("deid", include=_TASK_MODULES)
    app.conf.update(
        broker_url=broker_url,
        result_backend=result_backend or broker_url,
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
    )
    _app = app
    return app


@validate_call(config=dict(arbitrary_types_allowed=True))
def get_celery_app() -> Celery:
    global _app
    if _app is None:
        _app = create_celery_app()
    return _app


# Module-level instance so `celery -A deid.tasks.celery_app worker` works.
# Celery auto-discovers an attribute named `celery` or `app`.
celery = get_celery_app()


# ---------------------------------------------------------------------------
# Worker-level preloaded data (process workers only)
# ---------------------------------------------------------------------------

_preloaded_data: dict = {}
_preload_logger = logging.getLogger("deid.tasks.preload")


def _preload_mappings(app: Celery) -> None:
    """Load mapping + PII tables into memory for process workers."""
    config_path = getattr(app.conf, "deid_config_path", None)
    if not config_path:
        config_path = os.environ.get("DEID_CONFIG_PATH")
    if not config_path:
        _preload_logger.warning("No config path available for preloading")
        return

    from pathlib import Path

    import polars as pl
    from sqlalchemy import create_engine, text

    from deid.config.loader import load_config

    cfg = load_config(Path(config_path))
    mappings_conn_str = f"sqlite:///{cfg.mappings_db_path}"
    engine = create_engine(mappings_conn_str)

    try:
        with engine.connect() as conn:
            for table_key, table_name in [
                ("patient_mapping", "patient_mapping_table"),
                ("encounter_mapping", "encounter_mapping_table"),
                ("appointment_mapping", "appointment_mapping_table"),
            ]:
                try:
                    result = conn.execute(text(f"SELECT * FROM {table_name}"))
                    cols = list(result.keys())
                    rows = result.fetchall()
                    if rows:
                        _preloaded_data[table_key] = pl.DataFrame(
                            [list(r) for r in rows], schema=cols, orient="row",
                            infer_schema_length=len(rows),
                        )
                        _preload_logger.info("Preloaded %s: %d rows", table_key, len(rows))
                except Exception as e:
                    _preload_logger.warning("Failed to preload %s: %s", table_name, e)
    finally:
        engine.dispose()

    # Load PII table if configured
    if getattr(cfg, "pii_db", None):
        try:
            pii_engine = create_engine(cfg.pii_db["master_connection_str"])
            with pii_engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM pii_data_table"))
                cols = list(result.keys())
                rows = result.fetchall()
                if rows:
                    _preloaded_data["pii_data_table"] = pl.DataFrame(
                        [list(r) for r in rows], schema=cols, orient="row",
                        infer_schema_length=len(rows),
                    )
                    _preload_logger.info("Preloaded pii_data_table: %d rows", len(rows))
            pii_engine.dispose()
        except Exception as e:
            _preload_logger.warning("Failed to preload PII table: %s", e)


def get_preloaded_data() -> dict:
    """Return the preloaded data dict (empty if not a process worker or not yet loaded)."""
    return _preloaded_data


@worker_process_init.connect
def _on_worker_process_init(**kwargs):
    queue = os.environ.get("DEID_WORKER_QUEUE", "")
    if queue == "deid-process":
        _preload_logger.info("Process worker starting — preloading mapping tables...")
        _preload_mappings(get_celery_app())
