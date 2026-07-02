import json
from unittest.mock import patch, MagicMock

from deid.config.task_models import LogRecord, LogLevel


def _make_record(**overrides):
    defaults = dict(
        timestamp="2026-03-09T14:30:05.123Z",
        level=LogLevel.INFO,
        table="patients",
        phase="deidentify",
        message="test",
    )
    defaults.update(overrides)
    return LogRecord(**defaults)


def test_publish_log_publishes_to_redis():
    from deid.core.log_publisher import publish_log, _get_pool

    mock_redis = MagicMock()
    mock_pool = MagicMock()
    _get_pool.cache_clear()
    with patch("deid.core.log_publisher._get_pool", return_value=mock_pool), \
         patch("deid.core.log_publisher.redis_lib.Redis", return_value=mock_redis):
        record = _make_record()
        publish_log("redis://localhost:6379/0", record)
        mock_redis.publish.assert_called_once()
        channel, data = mock_redis.publish.call_args[0]
        assert channel == "deid:logs"
        parsed = json.loads(data)
        assert parsed["table"] == "patients"


def test_maybe_log_standard_skips_debug():
    from deid.core.log_publisher import maybe_log

    record = _make_record(level=LogLevel.DEBUG, batch=1)
    run_config = {"redis_url": "redis://localhost", "log_verbosity": "standard"}
    with patch("deid.core.log_publisher.publish_log") as mock:
        maybe_log(run_config, record)
        mock.assert_not_called()


def test_maybe_log_standard_allows_warning():
    from deid.core.log_publisher import maybe_log

    record = _make_record(level=LogLevel.WARNING, batch=1, row_id="123")
    run_config = {"redis_url": "redis://localhost", "log_verbosity": "standard"}
    with patch("deid.core.log_publisher.publish_log") as mock:
        maybe_log(run_config, record)
        mock.assert_called_once()


def test_maybe_log_minimal_skips_batch():
    from deid.core.log_publisher import maybe_log

    record = _make_record(level=LogLevel.INFO, batch=3)
    run_config = {"redis_url": "redis://localhost", "log_verbosity": "minimal"}
    with patch("deid.core.log_publisher.publish_log") as mock:
        maybe_log(run_config, record)
        mock.assert_not_called()


def test_maybe_log_minimal_allows_table_level():
    from deid.core.log_publisher import maybe_log

    record = _make_record(level=LogLevel.INFO)
    run_config = {"redis_url": "redis://localhost", "log_verbosity": "minimal"}
    with patch("deid.core.log_publisher.publish_log") as mock:
        maybe_log(run_config, record)
        mock.assert_called_once()


def test_maybe_log_verbose_allows_debug():
    from deid.core.log_publisher import maybe_log

    record = _make_record(level=LogLevel.DEBUG, batch=1, row_id="123")
    run_config = {"redis_url": "redis://localhost", "log_verbosity": "verbose"}
    with patch("deid.core.log_publisher.publish_log") as mock:
        maybe_log(run_config, record)
        mock.assert_called_once()


def test_maybe_log_errors_always_published():
    """Errors are never filtered, even in minimal mode."""
    from deid.core.log_publisher import maybe_log

    record = _make_record(level=LogLevel.ERROR, batch=5)
    run_config = {"redis_url": "redis://localhost", "log_verbosity": "minimal"}
    with patch("deid.core.log_publisher.publish_log") as mock:
        maybe_log(run_config, record)
        mock.assert_called_once()
