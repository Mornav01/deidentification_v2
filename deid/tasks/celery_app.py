"""Celery application factory."""
from __future__ import annotations

import logging
import os

from celery import Celery
from celery.signals import worker_init
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
    broker_url: str | None = None,
    result_backend: str | None = None,
) -> Celery:
    import os
    if broker_url is None:
        broker_url = os.environ.get("DEID_BROKER_URL", "redis://localhost:6379/0")
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
        # ── Broker/Redis connection resilience (backported from dent) ──────────
        # Keep broker connections alive across long-running DEID jobs so Redis
        # doesn't silently drop idle sockets (common on NAT/firewall setups),
        # and reconnect indefinitely instead of failing the run.
        broker_connection_retry=True,
        broker_connection_retry_on_startup=True,
        broker_connection_max_retries=None,  # unlimited — let Celery reconnect forever
        broker_transport_options={
            "socket_timeout": 30,
            "socket_connect_timeout": 30,
            "socket_keepalive": True,
            "retry_policy": {"timeout": 30},
        },
        redis_socket_keepalive=True,
        redis_socket_timeout=30,
        redis_socket_connect_timeout=30,
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

# Loaded ONCE in the parent Celery master process (worker_init signal, before
# any worker is forked). All forked children inherit the pages via OS
# copy-on-write — workers only READ this dict, so the pages are never copied
# and all children share the same physical RAM (faster startup + lower RAM).
# (Backported from dent.)
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
    from sqlalchemy import text

    from deid.config.loader import load_config
    from deid.models.base import create_read_only_mappings_engine

    cfg = load_config(Path(config_path))
    engine = create_read_only_mappings_engine(cfg.mappings_connection_string)

    # Tables that should filter by nd_ActiveFlag = 'Y' when that column exists.
    _active_flag_tables = {"encounter_mapping_table", "appointment_mapping_table", "chart_mapping_table"}

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
                ("chart_mapping", "chart_mapping_table"),
            ]:
                try:
                    cols, rows = _fetch_mapping_table(conn, table_name)
                    if rows:
                        _preloaded_data[table_key] = _to_df(cols, rows)
                        _preload_logger.info("Preloaded %s: %d rows", table_key, len(rows))
                    else:
                        _preload_logger.warning(
                            "Mapping table %s (%s) is empty — joins for this type will be skipped",
                            table_name, table_key,
                        )
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

    Data is loaded once in the parent process before workers fork (worker_init),
    so children inherit it via copy-on-write and this returns the complete set
    with no waiting. The *timeout* arg is kept for backwards-compatible callers
    and is ignored.
    """
    return dict(_preloaded_data)


@worker_init.connect
def _on_worker_init(**kwargs):
    """Load mapping + master tables ONCE in the parent before forking workers.

    Celery's prefork pool uses os.fork(), so child workers inherit the parent's
    _preloaded_data via copy-on-write. Workers only READ it, so the pages are
    never copied — loaded exactly once and shared across all children, instead
    of each worker re-loading its own copy. (Backported from dent.)
    """
    queue = os.environ.get("DEID_WORKER_QUEUE", "")
    # Only process workers consume the mapping tables; matches both the legacy
    # "deid-process" queue and per-config_key "deid-process-<config_key>" queues.
    if queue.startswith("deid-process"):
        _preload_logger.info(
            "Parent process: loading mapping + master tables once before forking workers..."
        )
        try:
            _preload_mappings(get_celery_app())
            _preload_logger.info(
                "Parent preload complete — %d table(s) shared with workers via fork.",
                len(_preloaded_data),
            )
        except Exception as exc:
            # A serious preload failure must not crash worker startup. Clear any
            # partial data so workers fall back to the per-batch SQL join path.
            _preloaded_data.clear()
            _preload_logger.error(
                "Parent preload failed (%s) — workers will use the SQL join fallback.",
                exc, exc_info=True,
            )
