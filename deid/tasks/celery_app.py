"""Celery application factory."""
from __future__ import annotations

from celery import Celery
from pydantic import validate_call

_TASK_MODULES = [
    "deid.tasks.deidentify",
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
