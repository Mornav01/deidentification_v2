"""Pydantic models for task/orchestrator/QC function boundaries."""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LogRecord(BaseModel):
    timestamp: str
    level: LogLevel
    table: str
    phase: str
    batch: int | None = None
    row_id: str | None = None
    column: str | None = None
    message: str
    error: str | None = None
    rows_in_batch: int | None = None
    rows_succeeded: int | None = None
    rows_failed: int | None = None
    duration_ms: int | None = None
    start_id: int | None = None
    end_id: int | None = None
    peak_memory_mb: int | None = None


class BatchFailure(BaseModel):
    table: str
    start_id: int | None = None
    end_id: int | None = None
    batch: int | None = None
    error: str
    timestamp: str
    task_type: str


class DeidentifyTaskConfig(BaseModel):
    table_name: str
    source_conn_str: str
    dest_conn_str: str
    table_details_for_ui: dict
    mappings_db_path: str = ""
    batch_size: int = 100000
    offset_days: int = 34
    redis_url: str = ""
    pii_config: dict | None = None
    pii_db_conn_str: str | None = None
    secondary_pii_configs: list | None = None
    mapping_db_config: dict | None = None
    universal_tables_config: list | None = None
    run_config: dict | None = None


class QCTaskConfig(BaseModel):
    table_name: str
    source_conn_str: str
    dest_conn_str: str
    table_config: dict
    mapping_db_config: dict = {}
    qc_settings: dict = {}
    offset_days: int = 34
    sample_size: int = 100


class ProgressEvent(BaseModel):
    table: str
    status: str
    detail: str = ""


class DataCountResult(BaseModel):
    source_rows_count: int
    dest_rows_count: int
    ignore_rows_count: int = 0
