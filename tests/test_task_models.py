"""Tests for Pydantic task models validation."""
import pytest
from pydantic import ValidationError

from deid.config.task_models import (
    DeidentifyTaskConfig,
    QCTaskConfig,
    ProgressEvent,
    DataCountResult,
    LogRecord,
    LogLevel,
    BatchFailure,
)


class TestDeidentifyTaskConfig:
    def test_valid_minimal(self):
        config = DeidentifyTaskConfig(
            table_name="patients",
            source_conn_str="sqlite:///src.db",
            dest_conn_str="sqlite:///dest.db",
            table_details_for_ui={"columns_details": []},
        )
        assert config.table_name == "patients"
        assert config.batch_size == 100000
        assert config.offset_days == 34
        assert config.redis_url == ""

    def test_valid_full(self):
        config = DeidentifyTaskConfig(
            table_name="encounters",
            source_conn_str="mysql+pymysql://u:p@host/db",
            dest_conn_str="postgresql+psycopg2://u:p@host/db",
            table_details_for_ui={"columns_details": [{"col": "a"}]},
            mappings_db_path="/tmp/mappings.db",
            batch_size=50000,
            offset_days=60,
            redis_url="redis://localhost:6379/0",
            pii_config={"key": "value"},
            mapping_db_config={"path": "/tmp/map.db"},
        )
        assert config.batch_size == 50000
        assert config.pii_config == {"key": "value"}

    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            DeidentifyTaskConfig(
                table_name="test",
                source_conn_str="sqlite:///src.db",
                # missing dest_conn_str and table_details_for_ui
            )

    def test_wrong_type(self):
        with pytest.raises(ValidationError):
            DeidentifyTaskConfig(
                table_name="test",
                source_conn_str="sqlite:///src.db",
                dest_conn_str="sqlite:///dest.db",
                table_details_for_ui="not_a_dict",
            )

    def test_model_dump_roundtrip(self):
        config = DeidentifyTaskConfig(
            table_name="t1",
            source_conn_str="sqlite:///s.db",
            dest_conn_str="sqlite:///d.db",
            table_details_for_ui={},
        )
        dumped = config.model_dump()
        restored = DeidentifyTaskConfig(**dumped)
        assert restored == config

    def test_cache_dir_defaults_to_none(self):
        config = DeidentifyTaskConfig(
            table_name="patients",
            source_conn_str="sqlite:///src.db",
            dest_conn_str="sqlite:///dest.db",
            table_details_for_ui={"columns_details": []},
        )
        assert config.cache_dir is None

    def test_cache_dir_set(self):
        config = DeidentifyTaskConfig(
            table_name="patients",
            source_conn_str="sqlite:///src.db",
            dest_conn_str="sqlite:///dest.db",
            table_details_for_ui={"columns_details": []},
            cache_dir="/tmp/.deid_cache/patients",
        )
        assert config.cache_dir == "/tmp/.deid_cache/patients"

    def test_cache_dir_survives_roundtrip(self):
        config = DeidentifyTaskConfig(
            table_name="t1",
            source_conn_str="sqlite:///s.db",
            dest_conn_str="sqlite:///d.db",
            table_details_for_ui={},
            cache_dir="/tmp/cache/t1",
        )
        dumped = config.model_dump()
        restored = DeidentifyTaskConfig(**dumped)
        assert restored.cache_dir == "/tmp/cache/t1"


class TestQCTaskConfig:
    def test_valid(self):
        config = QCTaskConfig(
            table_name="patients",
            source_conn_str="sqlite:///src.db",
            dest_conn_str="sqlite:///dest.db",
            table_config={"columns_details": []},
        )
        assert config.sample_size == 100
        assert config.qc_settings == {}

    def test_missing_table_config(self):
        with pytest.raises(ValidationError):
            QCTaskConfig(
                table_name="patients",
                source_conn_str="sqlite:///src.db",
                dest_conn_str="sqlite:///dest.db",
                # missing table_config
            )


class TestProgressEvent:
    def test_valid(self):
        event = ProgressEvent(table="patients", status="completed")
        assert event.detail == ""

    def test_missing_status(self):
        with pytest.raises(ValidationError):
            ProgressEvent(table="patients")


class TestDataCountResult:
    def test_valid(self):
        result = DataCountResult(source_rows_count=100, dest_rows_count=95)
        assert result.ignore_rows_count == 0

    def test_missing_field(self):
        with pytest.raises(ValidationError):
            DataCountResult(source_rows_count=100)


def test_log_record_minimal():
    record = LogRecord(
        timestamp="2026-03-09T14:30:05.123Z",
        level=LogLevel.INFO,
        table="patients",
        phase="deidentify",
        message="batch 3: 1000/1000 rows OK in 2.3s",
    )
    assert record.level == LogLevel.INFO
    assert record.batch is None
    assert record.error is None


def test_log_record_full():
    record = LogRecord(
        timestamp="2026-03-09T14:30:05.123Z",
        level=LogLevel.ERROR,
        table="patients",
        phase="deidentify",
        batch=5,
        message="write failed",
        error="connection timeout",
        rows_in_batch=1000,
        rows_succeeded=0,
        rows_failed=1000,
        duration_ms=2300,
        start_id=4001,
        end_id=5000,
        peak_memory_mb=512,
    )
    assert record.batch == 5
    assert record.error == "connection timeout"
    assert record.peak_memory_mb == 512


def test_log_record_serialization():
    record = LogRecord(
        timestamp="2026-03-09T14:30:05.123Z",
        level=LogLevel.WARNING,
        table="patients",
        phase="deidentify",
        batch=3,
        row_id="12345",
        column="patient_id",
        message="null patient mapping",
    )
    data = record.model_dump_json()
    restored = LogRecord.model_validate_json(data)
    assert restored.row_id == "12345"
    assert restored.column == "patient_id"


def test_batch_failure_model():
    failure = BatchFailure(
        table="patients",
        start_id=4001,
        end_id=5000,
        batch=5,
        error="connection timeout",
        timestamp="2026-03-09T14:30:07.890Z",
        task_type="range",
    )
    assert failure.task_type == "range"
    data = failure.model_dump_json()
    restored = BatchFailure.model_validate_json(data)
    assert restored.start_id == 4001


def test_batch_failure_full_table():
    failure = BatchFailure(
        table="encounters",
        error="NLP model OOM",
        timestamp="2026-03-09T14:31:02.100Z",
        task_type="full",
    )
    assert failure.start_id is None
    assert failure.end_id is None
    assert failure.batch is None
