"""Pydantic models for config.yaml validation."""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class DbType(str, Enum):
    mysql = "mysql"
    mssql = "mssql"
    postgresql = "postgresql"
    snowflake = "snowflake"


class DbConfig(BaseModel):
    type: DbType
    host: str
    port: int
    database: str
    username: str
    password: str

    def connection_string(self) -> str:
        drivers = {
            DbType.mysql: "mysql+pymysql",
            DbType.mssql: "mssql+pymssql",
            DbType.postgresql: "postgresql+psycopg2",
            DbType.snowflake: "snowflake",
        }
        driver = drivers[self.type]
        return f"{driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class DeidentificationSettings(BaseModel):
    batch_size: int = 100000
    date_offset_days: int = 34
    patient_id_prefix: int = 10000000
    parallel_tasks_per_table: int = 4
    large_table_threshold: int = 500000


class TableConfig(BaseModel):
    name: str
    rules: dict[str, str]


class MappingTableConfig(BaseModel):
    source_column: str
    destination_column: str
    reference: Optional[str] = None


class WorkerSettings(BaseModel):
    concurrency: int = 4
    max_retries: int = 1
    task_timeout: int = 3600


class QCSettings(BaseModel):
    sample_size: int = 100
    scan_for_residual_pii: bool = True


class DeidConfig(BaseModel):
    source_db: DbConfig
    destination_db: DbConfig
    state_db_path: str = "./state.db"
    mappings_db_path: str = "./mappings.db"
    redis_url: str = "redis://localhost:6379/0"
    deidentification: DeidentificationSettings = DeidentificationSettings()
    tables: Optional[list[TableConfig]] = None
    rules_csv: Optional[str] = None
    mapping_tables: dict[str, MappingTableConfig] = {}
    phases: list[str] = Field(default=["setup", "deidentify", "qc"])
    workers: WorkerSettings = WorkerSettings()
    qc: QCSettings = QCSettings()

    @model_validator(mode="after")
    def require_tables_or_csv(self) -> "DeidConfig":
        if not self.tables and not self.rules_csv:
            raise ValueError("Either 'tables' or 'rules_csv' must be provided")
        return self
