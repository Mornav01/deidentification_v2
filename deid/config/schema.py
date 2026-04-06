"""Pydantic models for config.yaml validation."""
from __future__ import annotations

import csv
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator, validate_call


class DbType(str, Enum):
    mysql = "mysql"
    mssql = "mssql"
    postgresql = "postgresql"
    snowflake = "snowflake"


class LogVerbosity(str, Enum):
    minimal = "minimal"
    standard = "standard"
    verbose = "verbose"


class DbConfig(BaseModel):
    type: DbType
    host: str
    port: int
    database: str
    username: str
    password: str

    @validate_call(config=dict(arbitrary_types_allowed=True))
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
    model_config = ConfigDict(extra="ignore")

    batch_size: int = 10000
    date_offset_days: int = 34
    patient_id_prefix: int = 10000000
    random_seed: int = 42


class TableConfig(BaseModel):
    name: str
    rules: dict[str, str]


class MappingTableConfig(BaseModel):
    source_column: str
    destination_column: str
    reference: Optional[str] = None


class WorkerSettings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fetchers: int = 2
    processors: int = 16
    max_retries: int = 1
    task_timeout: int = 3600
    max_tasks_per_child: int = 1
    max_tasks_per_child_fetch: int | None = None
    max_tasks_per_child_process: int | None = None


class QCSettings(BaseModel):
    sample_size: int = 100
    scan_for_residual_pii: bool = True
    task_timeout: int = 7200


class ClinicalBinDocConfig(BaseModel):
    source_db: str
    dest_db: str
    source_table: str = "ClinicalBin"
    metadata_table: str = "ClinicalDocuments"
    dest_table: str = "clinicalbin_xml_decrypt"
    processed_table: str = "clinicalbin_xml_processed"


class LoggingSettings(BaseModel):
    log_dir: str = "./logs"
    log_verbosity: LogVerbosity = LogVerbosity.standard


class DeidConfig(BaseModel):
    source_db: DbConfig
    destination_db: DbConfig
    join_db: Optional[DbConfig] = None
    config_key: str = "default"
    state_db_path: str = "./state.db"
    mappings_db: Optional[DbConfig] = None
    mappings_db_path: str = ""
    failed_rows_db_path: str = "./failed_rows.db"
    qc_results_db_path: str = "./qc_results.db"
    redis_url: str = "redis://localhost:6379/0"
    deidentification: DeidentificationSettings = DeidentificationSettings()
    tables: Optional[list[TableConfig]] = None
    tables_to_run: Optional[list[str]] = None
    tables_to_run_csv: Optional[str] = None
    unmatched_tables: list[str] = Field(default_factory=list, exclude=True)
    rules_csv: Optional[str] = None
    mapping_tables: dict[str, MappingTableConfig] = {}
    phases: list[str] = Field(default=["setup", "deidentify"])
    workers: WorkerSettings = WorkerSettings()
    qc: QCSettings = QCSettings()
    logging: LoggingSettings = LoggingSettings()
    clinical_bin_doc: Optional[ClinicalBinDocConfig] = None
    pii_db: Optional[dict] = None
    pii_tables_config: Optional[dict] = None
    pii_config: Optional[dict] = None
    secondary_pii_configs: Optional[list] = None
    secondary_pii_config_path: Optional[str] = None
    pii_config_path: Optional[str] = None
    reference_mappings_path: Optional[str] = None
    reference_mappings: dict[str, str] = Field(default_factory=dict, exclude=True)

    @model_validator(mode="after")
    def load_reference_mappings(self) -> "DeidConfig":
        if self.reference_mappings_path:
            p = Path(self.reference_mappings_path)
            if not p.exists():
                raise ValueError(f"reference_mappings_path '{p}' does not exist")
            import yaml as _yaml
            with open(p) as f:
                data = _yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise ValueError(
                    f"reference_mappings_path must contain a YAML mapping, "
                    f"got {type(data).__name__}"
                )
            self.reference_mappings = data
        return self

    @field_validator("config_key")
    @classmethod
    def validate_config_key(cls, v: str) -> str:
        import re as _re
        if not _re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError(
                f"config_key '{v}' is invalid — use only letters, digits, underscores, or hyphens"
            )
        return v

    @model_validator(mode="after")
    def set_default_mappings_db_path(self) -> "DeidConfig":
        if not self.mappings_db and not self.mappings_db_path:
            self.mappings_db_path = f"./{self.source_db.database}_mappings.db"
        return self

    @property
    def mappings_connection_string(self) -> str:
        """Return the connection string for the mappings database.

        Uses ``mappings_db`` (MySQL/PostgreSQL/etc.) when configured,
        otherwise falls back to the SQLite file at ``mappings_db_path``.
        """
        if self.mappings_db:
            return self.mappings_db.connection_string()
        return f"sqlite:///{self.mappings_db_path}"

    @model_validator(mode="after")
    def require_tables_or_csv(self) -> "DeidConfig":
        if not self.tables and not self.rules_csv:
            raise ValueError("Either 'tables' or 'rules_csv' must be provided")
        if not self.tables and self.rules_csv:
            self.tables = _load_tables_from_csv(self.rules_csv, self.source_db)
        return self

    @model_validator(mode="after")
    def filter_tables_to_run(self) -> "DeidConfig":
        if not self.tables_to_run and self.tables_to_run_csv:
            csv_path = Path(self.tables_to_run_csv)
            if csv_path.exists():
                import csv
                with open(csv_path) as f:
                    reader = csv.reader(f)
                    self.tables_to_run = [
                        row[0].strip() for row in reader
                        if row and row[0].strip() and not row[0].strip().startswith("#")
                    ]
        if self.tables_to_run and self.tables:
            allowed = set(self.tables_to_run)
            configured = {t.name for t in self.tables}
            self.unmatched_tables = sorted(allowed - configured)
            if self.unmatched_tables:
                import logging
                logger = logging.getLogger("deid.config")
                for tname in self.unmatched_tables:
                    logger.error(
                        "Table '%s' is in tables_to_run but has no config rules — "
                        "it will be recorded as failed and skipped.",
                        tname,
                    )
            self.tables = [t for t in self.tables if t.name in allowed]
            if not self.tables:
                import logging as _logging
                _logging.getLogger("deid.config").warning(
                    "tables_to_run=%s matched none of the configured tables — "
                    "all will be recorded as failed in state.db.",
                    self.tables_to_run,
                )
        return self



@validate_call(config=dict(arbitrary_types_allowed=True))
def _load_tables_from_csv(csv_path: str, source_db: "DbConfig") -> list[TableConfig]:
    """Parse a rules CSV into TableConfig objects.

    If the CSV doesn't exist, auto-generates it by introspecting the source DB.

    CSV columns: table_name, column_name, data_type, rule
    Rows with an empty 'rule' are skipped (non-PHI columns).
    """
    path = Path(csv_path)
    if not path.exists():
        from deid.config.rules_generator import generate_rules_csv
        generate_rules_csv(source_db, csv_path)

    tables: dict[str, dict[str, str]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rule = (row.get("rule") or "").strip()
            if not rule:
                continue
            table_name = row["table_name"].strip()
            column_name = row["column_name"].strip()
            tables.setdefault(table_name, {})[column_name] = rule

    if not tables:
        raise ValueError(f"No rules found in {csv_path}")

    return [TableConfig(name=name, rules=rules) for name, rules in tables.items()]
