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


def test_deidentify_table_publishes_start_and_complete_logs():
    """Task should publish log records for start and completion."""
    from unittest.mock import patch
    from deid.config.task_models import DeidentifyTaskConfig

    config = DeidentifyTaskConfig(
        table_name="patients",
        source_conn_str="sqlite:///test.db",
        dest_conn_str="sqlite:///dest.db",
        table_details_for_ui={"columns_details": [], "ignore_rows": {}},
        redis_url="redis://localhost:6379/0",
        run_config={"redis_url": "redis://localhost:6379/0", "log_verbosity": "standard"},
    )

    published_logs = []

    def capture_log(redis_url, record):
        published_logs.append(record)

    with patch("deid.tasks.deidentify.start_de_identification_for_table", return_value={"table_name": "patients", "batches_processed": 3}):
        with patch("deid.tasks.deidentify._publish_progress"):
            with patch("deid.tasks.deidentify.publish_log", side_effect=capture_log):
                from deid.tasks.deidentify import deidentify_table
                deidentify_table(config.model_dump())

    assert len(published_logs) >= 2
    assert published_logs[0].level.value == "INFO"
    assert "started" in published_logs[0].message.lower()
    assert published_logs[-1].level.value == "INFO"
    assert "completed" in published_logs[-1].message.lower() or "complete" in published_logs[-1].message.lower()


def test_deidentify_table_publishes_error_on_failure():
    """Task should publish error log and progress on exception."""
    from unittest.mock import patch
    from deid.config.task_models import DeidentifyTaskConfig

    config = DeidentifyTaskConfig(
        table_name="patients",
        source_conn_str="sqlite:///test.db",
        dest_conn_str="sqlite:///dest.db",
        table_details_for_ui={"columns_details": [], "ignore_rows": {}},
        redis_url="redis://localhost:6379/0",
        run_config={"redis_url": "redis://localhost:6379/0", "log_verbosity": "standard"},
    )

    published_logs = []

    def capture_log(redis_url, record):
        published_logs.append(record)

    with patch("deid.tasks.deidentify.start_de_identification_for_table", side_effect=RuntimeError("connection timeout")):
        with patch("deid.tasks.deidentify._publish_progress") as mock_progress:
            with patch("deid.tasks.deidentify.publish_log", side_effect=capture_log):
                from deid.tasks.deidentify import deidentify_table
                with pytest.raises(RuntimeError, match="connection timeout"):
                    deidentify_table(config.model_dump())

    # Should have published a failed progress event.
    progress_calls = mock_progress.call_args_list
    failed_calls = [c for c in progress_calls if c[0][2] == "failed"]
    assert len(failed_calls) >= 1

    # Should have published an error log record.
    error_logs = [r for r in published_logs if r.level.value == "ERROR"]
    assert len(error_logs) >= 1
    assert "connection timeout" in error_logs[0].error
