"""Tests for Pydantic task models validation."""
import pytest
from pydantic import ValidationError

from deid.config.task_models import (
    FetchTaskConfig,
    ProcessTaskConfig,
    WriteTaskConfig,
    QCTaskConfig,
    ProgressEvent,
    DataCountResult,
    LogRecord,
    LogLevel,
    BatchFailure,
)


class TestFetchTaskConfig:
    def test_valid_minimal(self):
        config = FetchTaskConfig(
            table_name="patients",
            start_id=1,
            end_id=1000,
            source_conn_str="sqlite:///src.db",
            state_db_path="./state.db",
            staging_root="/tmp/.deid_staging",
        )
        assert config.table_name == "patients"
        assert config.batch_size == 1000
        assert config.id_column == "nd_auto_increment_id"

    def test_model_dump_roundtrip(self):
        config = FetchTaskConfig(
            table_name="t1",
            start_id=1,
            end_id=100,
            source_conn_str="sqlite:///s.db",
            state_db_path="./state.db",
            staging_root="/tmp/staging",
        )
        dumped = config.model_dump()
        restored = FetchTaskConfig(**dumped)
        assert restored == config


class TestProcessTaskConfig:
    def test_valid_minimal(self):
        config = ProcessTaskConfig(
            table_name="patients",
            start_id=1,
            end_id=1000,
            staging_root="/tmp/.deid_staging",
            state_db_path="./state.db",
            mapping_db_config={"connection_str": "sqlite:///mappings.db"},
            table_details={"columns_details": []},
            source_conn_str="sqlite:///src.db",
        )
        assert config.offset_days == 34
        assert config.pii_config is None

    def test_model_dump_roundtrip(self):
        config = ProcessTaskConfig(
            table_name="t1",
            start_id=1,
            end_id=100,
            staging_root="/tmp/staging",
            state_db_path="./state.db",
            mapping_db_config={"connection_str": "sqlite:///m.db"},
            table_details={"columns_details": []},
            source_conn_str="sqlite:///s.db",
        )
        dumped = config.model_dump()
        restored = ProcessTaskConfig(**dumped)
        assert restored == config


class TestWriteTaskConfig:
    def test_valid_minimal(self):
        config = WriteTaskConfig(
            table_name="patients",
            start_id=1,
            end_id=1000,
            staging_root="/tmp/.deid_staging",
            state_db_path="./state.db",
            dest_conn_str="sqlite:///dest.db",
        )
        assert config.id_column == "nd_auto_increment_id"
        assert config.redis_url == ""

    def test_model_dump_roundtrip(self):
        config = WriteTaskConfig(
            table_name="t1",
            start_id=1,
            end_id=100,
            staging_root="/tmp/staging",
            state_db_path="./state.db",
            dest_conn_str="sqlite:///d.db",
        )
        dumped = config.model_dump()
        restored = WriteTaskConfig(**dumped)
        assert restored == config


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
