"""Tests for Pydantic task models validation."""
import pytest
from pydantic import ValidationError

from deid.config.task_models import (
    DeidentifyTaskConfig,
    QCTaskConfig,
    StatsTaskConfig,
    ProgressEvent,
    DataCountResult,
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


class TestStatsTaskConfig:
    def test_valid(self):
        config = StatsTaskConfig(
            table_name="patients",
            source_conn_str="sqlite:///src.db",
        )
        assert config.table_name == "patients"

    def test_missing_source(self):
        with pytest.raises(ValidationError):
            StatsTaskConfig(table_name="patients")


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
