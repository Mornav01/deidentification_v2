"""Pydantic models for config.yaml validation."""
from __future__ import annotations

import csv
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator, validate_call
from sqlalchemy.engine import URL


def validate_replace_value(pii_config, *, source: str = "pii_config") -> None:
    """Validate the shape of ``pii_config['replace_value']``, raising ValueError
    on malformed input so a bad ``REPLACE_VALUE_JSON`` fails at config-load time
    rather than crashing mid-batch inside ``NotesRule._apply_replace_value``.

    Accepted shapes (mirror what the runtime tolerates):
      - a list of ``{old_value, new_value}`` dicts (canonical, from REPLACE_VALUE_JSON)
      - an ``{old_value: new_value}`` mapping

    ``source`` names the origin in error messages (e.g. ``secondary_pii_configs[0]``).
    """
    if not isinstance(pii_config, dict):
        return
    rv = pii_config.get("replace_value")
    if rv is None:
        return
    hint = (
        'Check REPLACE_VALUE_JSON — it must be a JSON list of '
        '{"old_value": ..., "new_value": ...} objects.'
    )
    if isinstance(rv, dict):
        return  # {old_value: new_value} mapping form
    if not isinstance(rv, list):
        raise ValueError(
            f"{source}.replace_value must be a list of {{old_value, new_value}} dicts "
            f"(or an {{old_value: new_value}} mapping), got {type(rv).__name__}. {hint}"
        )
    for i, entry in enumerate(rv):
        if not isinstance(entry, dict):
            raise ValueError(
                f"{source}.replace_value[{i}] must be a {{old_value, new_value}} dict, "
                f"got {type(entry).__name__}: {entry!r}. {hint}"
            )
        if "old_value" not in entry or "new_value" not in entry:
            raise ValueError(
                f"{source}.replace_value[{i}] is missing 'old_value' and/or 'new_value': "
                f"{entry!r}. {hint}"
            )


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
        return URL.create(
            drivername=drivers[self.type],
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        ).render_as_string(hide_password=False)


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
    identifier_columns: list[str] = []


class WorkerSettings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fetchers: int = 2
    processors: int = 16
    max_retries: int = 1
    max_batch_retries: int = 3
    task_timeout: int = 3600
    max_tasks_per_child: int = 1
    max_tasks_per_child_fetch: int | None = None
    max_tasks_per_child_process: int | None = None
    table_batch_size: int = 0  # process N tables per batch; 0 = all at once


class QCSettings(BaseModel):
    sample_size: int = 100
    scan_for_residual_pii: bool = True
    task_timeout: int = 7200
    # QC Framework Part 2 — blocking mapping & count checks run before the pipeline.
    # ``part2`` is the Part2Config dict (see deid/qc/mapping_count.py); empty → gate is a no-op.
    part2_blocking: bool = True
    part2: dict = {}
    # Delta-identity QC (cross-env row-level diff, run on CDC delta). DeltaIdentityConfig dict.
    delta_identity: dict = {}
    # Part 3 master-referenced unstructured audit (deid qc-audit). Holds shared keys
    # (pii_master_conn_str, pii_columns, facility_names, ...) + a ``tables`` list of per-table
    # MasterPhiConfig dicts. See deid/qc/master_phi.py.
    master_phi: dict = {}
    # Max allowed source↔dest divergence (percentage points) for identifier-column fill rates
    # (auto-qc fill-rate check). e.g. 1.0 → dest rate must be within 1pt of source rate.
    fillrate_tolerance_pct: float = 1.0


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
    state_db_name: Optional[str] = None
    mappings_db: Optional[DbConfig] = None
    mappings_db_path: str = ""
    failed_rows_db_path: str = "./failed_rows.db"
    failed_rows_db_name: Optional[str] = None
    qc_results_db_path: str = "./qc_results.db"
    qc_results_db_url: Optional[str] = None
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
    table_overrides_path: Optional[str] = None
    table_overrides: Optional[dict] = Field(default=None, exclude=True)
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

    @model_validator(mode="after")
    def validate_pii_replace_value(self) -> "DeidConfig":
        """Fail fast on a malformed inline ``replace_value``.

        Configs that instead load pii_config from ``pii_config_path`` are
        validated at their load site (async_runner / retry), since that
        assignment happens after model construction.
        """
        validate_replace_value(self.pii_config)
        for i, cfg in enumerate(self.secondary_pii_configs or []):
            validate_replace_value(cfg, source=f"secondary_pii_configs[{i}]")
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

    @property
    def resolved_state_db_url(self) -> str:
        """Full SQLAlchemy URL for the state database.

        Uses ``state_db_name`` (same server/credentials as ``destination_db``,
        different database) when set, otherwise falls back to SQLite at
        ``state_db_path``.
        """
        if self.state_db_name:
            return self.destination_db.model_copy(update={"database": self.state_db_name}).connection_string()
        return f"sqlite:///{self.state_db_path}"

    @property
    def resolved_failed_rows_db_url(self) -> str:
        """Full SQLAlchemy URL for the failed-rows audit database.

        Uses ``failed_rows_db_name`` (same server/credentials as
        ``destination_db``, different database) when set, otherwise falls
        back to SQLite at ``failed_rows_db_path``.
        """
        if self.failed_rows_db_name:
            return self.destination_db.model_copy(update={"database": self.failed_rows_db_name}).connection_string()
        return f"sqlite:///{self.failed_rows_db_path}"

    @property
    def resolved_qc_results_db_url(self) -> str:
        """Full SQLAlchemy URL for the QC results database.

        Uses ``qc_results_db_url`` when set, otherwise falls back to
        SQLite at ``qc_results_db_path``.
        """
        return self.qc_results_db_url or f"sqlite:///{self.qc_results_db_path}"

    @model_validator(mode="after")
    def build_pii_db_connection_strings(self) -> "DeidConfig":
        """Derive pii_db connection strings from ``destination_db`` credentials.

        ``pii_db`` uses the same server/user/password as ``destination_db`` and
        only differs by database name. Building the URL through
        ``DbConfig.connection_string()`` (``URL.create``) percent-encodes special
        characters in the password (e.g. ``@`` → ``%40``), avoiding the broken
        URL parsing that a raw ``mysql+pymysql://user:${DB_PASS}@host`` string
        would produce. Consumers keep reading the ``*_connection_str`` keys.
        """
        if not self.pii_db:
            return self
        # map: <db-name key in yaml> -> <conn-str key consumers read>
        _pii_map = {
            "master_db_name": "master_connection_str",
            "secondary_pii_db_name": "secondary_pii_connection_str",
            "insurance_db_name": "insurance_connection_str",
        }
        for name_key, conn_key in _pii_map.items():
            db_name = self.pii_db.get(name_key)
            if db_name:
                self.pii_db[conn_key] = self.destination_db.model_copy(
                    update={"database": db_name}
                ).connection_string()
        return self

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
    Rows with an empty 'rule' still register the table (pass-through — no PHI columns).
    """
    path = Path(csv_path)
    if not path.exists():
        from deid.config.rules_generator import generate_rules_csv
        generate_rules_csv(source_db, csv_path)

    tables: dict[str, dict[str, str]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table_name = row["table_name"].strip()
            rule = (row.get("rule") or "").strip()
            if not rule:
                # Register the table even if this column has no rule (no-PHI pass-through)
                tables.setdefault(table_name, {})
                continue
            column_name = row["column_name"].strip()
            tables.setdefault(table_name, {})[column_name] = rule

    if not tables:
        raise ValueError(f"No rules found in {csv_path}")

    return [TableConfig(name=name, rules=rules) for name, rules in tables.items()]
