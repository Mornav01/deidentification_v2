"""Integration test for the logging pipeline (without real Redis/Celery)."""
import json
from pathlib import Path

from deid.config.task_models import LogLevel
from deid.core.log_publisher import make_log_record
from deid.orchestrator.log_collector import LogCollector


def test_full_logging_pipeline(tmp_path):
    """Simulate a full run: multiple tables, batches, one failure, then summary."""
    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_14-30-00",
    )

    # Table 1: patients — 3 batches, 1 warning.
    collector.handle_record(make_log_record(
        LogLevel.INFO, "patients", "deidentify", "table started",
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.INFO, "patients", "deidentify",
        "batch 1: 1000/1000 rows OK in 1500ms",
        batch=1, rows_in_batch=1000, rows_succeeded=1000, rows_failed=0,
        duration_ms=1500, peak_memory_mb=200,
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.WARNING, "patients", "deidentify",
        "PATIENT_ID on column 'patient_id': 3 rows got null values",
        batch=2, column="patient_id",
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.INFO, "patients", "deidentify",
        "batch 2: 997/1000 rows OK in 1800ms",
        batch=2, rows_in_batch=1000, rows_succeeded=997, rows_failed=3,
        duration_ms=1800, peak_memory_mb=250,
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.INFO, "patients", "deidentify",
        "batch 3: 500/500 rows OK in 900ms",
        batch=3, rows_in_batch=500, rows_succeeded=500, rows_failed=0,
        duration_ms=900, peak_memory_mb=220,
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.INFO, "patients", "deidentify", "table completed — 3 batches",
    ).model_dump())

    # Table 2: encounters — fails on batch 2.
    collector.handle_record(make_log_record(
        LogLevel.INFO, "encounters", "deidentify", "table started",
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.INFO, "encounters", "deidentify",
        "batch 1: 2000/2000 rows OK in 3000ms",
        batch=1, rows_in_batch=2000, rows_succeeded=2000, rows_failed=0,
        duration_ms=3000, peak_memory_mb=400,
    ).model_dump())

    collector.handle_record(make_log_record(
        LogLevel.ERROR, "encounters", "deidentify",
        "batch 2: write failed — connection timeout",
        batch=2, error="connection timeout",
        start_id=2001, end_id=4000,
    ).model_dump())

    # Verify log file exists and has content.
    log_file = tmp_path / "deid_2026-03-09_14-30-00.log"
    assert log_file.exists()
    log_lines = log_file.read_text().strip().split("\n")
    assert len(log_lines) >= 8

    # Verify failures file.
    failures_file = tmp_path / "deid_2026-03-09_14-30-00_failures.jsonl"
    assert failures_file.exists()
    failure_lines = failures_file.read_text().strip().split("\n")
    assert len(failure_lines) == 1
    failure = json.loads(failure_lines[0])
    assert failure["table"] == "encounters"
    assert failure["task_type"] == "range"

    # Verify stats.
    stats = collector.get_stats()
    assert stats["totals"]["tables"] == 2
    assert stats["totals"]["tables_failed"] == 1
    assert stats["totals"]["tables_completed"] == 1
    assert stats["totals"]["rows_succeeded"] == 4497
    assert stats["totals"]["rows_failed"] == 3
    assert stats["totals"]["warnings"] == 1
    assert stats["memory"]["peak_worker_mb"] == 400

    # Verify summary generation.
    summary = collector.write_summary()
    summary_file = tmp_path / "deid_2026-03-09_14-30-00_summary.json"
    assert summary_file.exists()
    summary_data = json.loads(summary_file.read_text())
    assert summary_data["totals"]["rows_succeeded"] == 4497

    # Verify text summary.
    text = collector.format_text_summary()
    assert "Run Summary" in text
    assert "patients" in text
    assert "encounters" in text
    assert "FAILED" in text
    assert "OK" in text
    assert "Retry command" in text

    collector.close()


def test_empty_run_produces_summary(tmp_path):
    """A run with no log records should still produce a valid summary."""
    collector = LogCollector(
        log_dir=str(tmp_path),
        run_timestamp="2026-03-09_15-00-00",
    )
    summary = collector.write_summary()
    assert summary["totals"]["tables"] == 0
    assert summary["totals"]["rows_succeeded"] == 0

    text = collector.format_text_summary()
    assert "Run Summary" in text
    collector.close()
