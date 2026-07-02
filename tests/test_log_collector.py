import json
from pathlib import Path

from deid.config.task_models import LogRecord, LogLevel, BatchFailure


def _make_record(**overrides):
    defaults = dict(
        timestamp="2026-03-09T14:30:05.123Z",
        level="INFO",
        table="patients",
        phase="process",
        message="batch 1: 1000/1000 rows OK",
        batch=1,
        rows_in_batch=1000,
        rows_succeeded=1000,
        rows_failed=0,
        duration_ms=2300,
    )
    defaults.update(overrides)
    return defaults


def test_log_collector_writes_log_line(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    collector.handle_record(_make_record())
    collector.close()

    log_file = tmp_path / "deid_2026-03-09_14-30-00.log"
    assert log_file.exists()
    content = log_file.read_text()
    assert "patients" in content
    assert "batch 1" in content


def test_log_collector_accumulates_stats(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    collector.handle_record(_make_record(
        table="patients", batch=1,
        rows_in_batch=1000, rows_succeeded=990, rows_failed=10,
    ))
    collector.handle_record(_make_record(
        table="patients", batch=2,
        rows_in_batch=500, rows_succeeded=500, rows_failed=0,
    ))
    collector.handle_record(_make_record(
        table="vitals", batch=1,
        rows_in_batch=200, rows_succeeded=200, rows_failed=0,
    ))

    stats = collector.get_stats()
    assert stats["totals"]["rows_succeeded"] == 1690
    assert stats["totals"]["rows_failed"] == 10
    assert stats["tables"]["patients"]["rows_succeeded"] == 1490
    assert stats["tables"]["vitals"]["rows_failed"] == 0
    collector.close()


def test_log_collector_writes_failure(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    collector.handle_record(_make_record(
        level="ERROR",
        table="patients",
        batch=5,
        message="write failed",
        error="connection timeout",
        start_id=4001,
        end_id=5000,
    ))
    collector.close()

    failures_file = tmp_path / "deid_2026-03-09_14-30-00_failures.jsonl"
    assert failures_file.exists()
    lines = failures_file.read_text().strip().split("\n")
    assert len(lines) == 1
    failure = json.loads(lines[0])
    assert failure["table"] == "patients"
    assert failure["start_id"] == 4001
    assert failure["error"] == "connection timeout"


def test_log_collector_tracks_peak_memory(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    # Two different worker PIDs reporting at overlapping times
    collector.handle_record(_make_record(peak_memory_mb=200, worker_pid=1001))
    collector.handle_record(_make_record(peak_memory_mb=500, worker_pid=1002))
    collector.handle_record(_make_record(peak_memory_mb=300, worker_pid=1001))

    stats = collector.get_stats()
    assert stats["memory"]["peak_single_worker_mb"] == 500
    # Peak concurrent: after 3rd record, pid 1001=300 + pid 1002=500 = 800
    assert stats["memory"]["peak_concurrent_workers_mb"] == 800
    assert stats["memory"]["worker_count"] == 2
    collector.close()


def test_log_collector_warning_counts(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    collector.handle_record(_make_record(
        level="WARNING", table="patients",
        message="null mapping", row_id="123",
    ))
    collector.handle_record(_make_record(
        level="WARNING", table="patients",
        message="null mapping", row_id="456",
    ))

    stats = collector.get_stats()
    assert stats["totals"]["warnings"] == 2
    assert stats["tables"]["patients"]["warnings"] == 2
    collector.close()


def test_write_summary_json(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    collector.handle_record(_make_record(
        table="patients", batch=1,
        rows_in_batch=100, rows_succeeded=100, rows_failed=0,
    ))
    summary = collector.write_summary()
    collector.close()

    summary_file = tmp_path / "deid_2026-03-09_14-30-00_summary.json"
    assert summary_file.exists()
    data = json.loads(summary_file.read_text())
    assert data["totals"]["rows_succeeded"] == 100
    assert "memory" in data
    assert "tables" in data


def test_format_text_summary(tmp_path):
    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )
    collector.handle_record(_make_record(
        table="patients", batch=1,
        rows_in_batch=100, rows_succeeded=95, rows_failed=5,
        duration_ms=5000,
    ))
    text = collector.format_text_summary()
    assert "Run Summary" in text
    assert "patients" in text
    collector.close()


def test_log_collector_reconnects_after_connection_error(tmp_path):
    """listen() retries after a ConnectionError and delivers messages from the second connection."""
    import asyncio
    import json
    from unittest.mock import AsyncMock, MagicMock, patch

    from deid.orchestrator.log_collector import LogCollector

    collector = LogCollector(log_dir=str(tmp_path), run_timestamp="2026-01-01_00-00-00")

    record_payload = json.dumps({
        "timestamp": "2026-01-01T00:00:00Z",
        "level": "INFO",
        "table": "patients",
        "phase": "process",
        "message": "batch processed",
        "batch": 1,
        "rows_in_batch": 10,
        "rows_succeeded": 10,
        "rows_failed": 0,
        "duration_ms": 100,
    })

    subscribe_calls = [0]

    async def fake_subscribe(channel):
        subscribe_calls[0] += 1
        if subscribe_calls[0] == 1:
            raise ConnectionError("first attempt fails")

    msgs_delivered = [0]

    async def fake_get_message(**kwargs):
        if msgs_delivered[0] == 0:
            msgs_delivered[0] += 1
            return {"type": "message", "data": record_payload.encode()}
        collector.stop()
        return None

    mock_pubsub = MagicMock()
    mock_pubsub.subscribe = fake_subscribe
    mock_pubsub.get_message = fake_get_message
    mock_pubsub.unsubscribe = AsyncMock()
    mock_pubsub.aclose = AsyncMock()

    mock_r = MagicMock()
    mock_r.pubsub.return_value = mock_pubsub
    mock_r.aclose = AsyncMock()

    async def run():
        with patch("redis.asyncio.from_url", return_value=mock_r), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            await collector.listen("redis://localhost:6379/0")

    asyncio.run(run())

    # Must have tried twice: once failing, once succeeding.
    assert subscribe_calls[0] == 2
    # Message from the second connection must have been processed.
    assert "patients" in collector._table_stats
    assert collector._table_stats["patients"]["rows_succeeded"] == 10
    collector.close()
