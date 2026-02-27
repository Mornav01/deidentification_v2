"""Pydantic models for task/orchestrator/QC function boundaries."""
from __future__ import annotations

from pydantic import BaseModel


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


class StatsTaskConfig(BaseModel):
    table_name: str
    source_conn_str: str


class ProgressEvent(BaseModel):
    table: str
    status: str
    detail: str = ""


class DataCountResult(BaseModel):
    source_rows_count: int
    dest_rows_count: int
    ignore_rows_count: int = 0
