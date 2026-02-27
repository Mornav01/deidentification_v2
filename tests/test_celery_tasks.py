"""Verify Celery app creation and task registration."""
import pytest


@pytest.fixture
def celery_config():
    """Configure Celery for testing — eager mode, no broker needed."""
    return {
        "broker_url": "memory://",
        "result_backend": "cache+memory://",
        "task_always_eager": True,
        "task_eager_propagates": True,
    }


@pytest.fixture
def celery_app_fixture(celery_config):
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(
        broker_url=celery_config["broker_url"],
        result_backend=celery_config["result_backend"],
    )
    app.conf.update(celery_config)
    app.finalize()
    app.loader.import_default_modules()
    return app


def test_celery_app_creates(celery_app_fixture):
    assert celery_app_fixture.main == "deid"


def test_deidentify_task_registered(celery_app_fixture):
    assert "deid.tasks.deidentify.deidentify_table" in celery_app_fixture.tasks


def test_deidentify_range_task_registered(celery_app_fixture):
    assert "deid.tasks.deidentify.deidentify_table_range" in celery_app_fixture.tasks


def test_qc_task_registered(celery_app_fixture):
    assert "deid.tasks.qc.run_qc" in celery_app_fixture.tasks
