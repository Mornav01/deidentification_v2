"""Celery application factory."""
from __future__ import annotations

import logging
import os
import threading

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
        # Recycle a worker process once it exceeds 3 GB RSS.
        # Memory-based recycling checks happen *between* tasks (not during),
        # so it avoids the BrokenPipeError race that task-count recycling causes.
        worker_max_memory_per_child=10_000_000,  # 10 GB in KB
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
_preload_ready = threading.Event()   # set once _preload_mappings() finishes
_preload_thread: threading.Thread | None = None
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
    from sqlalchemy import text

    from deid.config.loader import load_config
    from deid.models.base import create_read_only_mappings_engine

    cfg = load_config(Path(config_path))
    engine = create_read_only_mappings_engine(cfg.mappings_connection_string)

    # Tables that should filter by nd_ActiveFlag = 'Y' when that column exists.
    _active_flag_tables = {"encounter_mapping_table", "appointment_mapping_table"}

    def _fetch_mapping_table(conn, table_name: str) -> tuple[list, list]:
        """Return (cols, rows) for a mapping table.

        For encounter/appointment tables, filters WHERE nd_ActiveFlag = 'Y'
        if that column exists; otherwise falls back to a full SELECT.
        """
        if table_name in _active_flag_tables:
            try:
                probe = conn.execute(text(
                    f"SELECT * FROM {table_name} WHERE nd_ActiveFlag = 'Y'"
                ))
                return [c.lower() for c in probe.keys()], probe.fetchall()
            except Exception:
                _preload_logger.info(
                    "nd_ActiveFlag not found in %s — loading without filter", table_name
                )
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        return [c.lower() for c in result.keys()], result.fetchall()

    from deid.core.dbPkg.dbhandler import _normalize_rows

    def _to_df(cols: list, rows: list) -> "pl.DataFrame":
        return pl.DataFrame(
            _normalize_rows(rows),
            schema=cols,
            orient="row",
            infer_schema_length=len(rows),
        )

    try:
        with engine.connect() as conn:
            for table_key, table_name in [
                ("patient_mapping", "patient_mapping_table"),
                ("encounter_mapping", "encounter_mapping_table"),
                ("appointment_mapping", "appointment_mapping_table"),
            ]:
                try:
                    cols, rows = _fetch_mapping_table(conn, table_name)
                    if rows:
                        _preloaded_data[table_key] = _to_df(cols, rows)
                        _preload_logger.info("Preloaded %s: %d rows", table_key, len(rows))
                except Exception as e:
                    _preload_logger.warning("Failed to preload %s: %s", table_name, e)
    finally:
        engine.dispose()

    # Load PII table if configured
    if getattr(cfg, "pii_db", None):
        try:
            from deid.core.dbPkg.dbhandler import create_read_only_engine
            pii_engine = create_read_only_engine(cfg.pii_db["master_connection_str"])
            with pii_engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM pii_data_table"))
                cols = [c.lower() for c in result.keys()]
                rows = result.fetchall()
                if rows:
                    _preloaded_data["pii_data_table"] = _to_df(cols, rows)
                    _preload_logger.info("Preloaded pii_data_table: %d rows", len(rows))
            pii_engine.dispose()
        except Exception as e:
            _preload_logger.warning("Failed to preload PII table: %s", e)


def get_preloaded_data(timeout: float = 60.0) -> dict:
    """Return the preloaded data dict.

    If a background preload thread is running, waits up to *timeout* seconds
    for it to finish.  Returns an empty dict on timeout so callers fall back
    to the SQL path.
    """
    if _preload_thread is not None and not _preload_ready.is_set():
        _preload_ready.wait(timeout=timeout)
    return dict(_preloaded_data)


def _run_preload_in_background(app: Celery) -> None:
    """Start _preload_mappings in a daemon thread and set _preload_ready when done."""
    global _preload_thread

    def _target():
        try:
            _preload_mappings(app)
        finally:
            _preload_ready.set()

    _preload_thread = threading.Thread(target=_target, daemon=True, name="deid-preload")
    _preload_thread.start()


@worker_process_init.connect
def _on_worker_process_init(**kwargs):
    queue = os.environ.get("DEID_WORKER_QUEUE", "")
    # Matches both the legacy "deid-process" queue and the per-config_key
    # "deid-process-<config_key>" queues introduced to isolate concurrent runs.
    if queue.startswith("deid-process"):
        _preload_logger.info("Process worker starting — preloading mapping tables in background...")
        _run_preload_in_background(get_celery_app())
