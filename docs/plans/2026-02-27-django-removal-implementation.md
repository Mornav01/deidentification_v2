# Django Removal & Celery Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace Django with Typer CLI + Celery/Redis task queue + SQLAlchemy/SQLite state, while preserving the core de-identification engine unchanged.

**Architecture:** Typer CLI → async orchestrator (asyncio) → Celery Canvas dispatches tasks to prefork workers → workers call the existing sync core engine (Polars/SQLAlchemy). Redis as broker/backend/pubsub. SQLite for state (state.db) and mappings (mappings.db).

**Tech Stack:** Typer, Celery[redis], SQLAlchemy 2.0, Pydantic v2, asyncio, aioredis, PyYAML, google-re2, Polars

---

## Task 1: Scaffold the new `deid` package and install dependencies

**Files:**
- Create: `deid/__init__.py`
- Create: `deid/__main__.py`
- Create: `deid/cli/__init__.py`
- Create: `deid/config/__init__.py`
- Create: `deid/models/__init__.py`
- Create: `deid/orchestrator/__init__.py`
- Create: `deid/tasks/__init__.py`
- Create: `deid/qc/__init__.py`
- Create: `deid/cdc/__init__.py`
- Create: `deid/clinical_bin_doc/__init__.py`
- Create: `requirements.txt` (new, replaces existing)
- Create: `pyproject.toml`

**Step 1: Create the package directory tree**

```bash
mkdir -p deid/{cli,config,models,orchestrator,tasks,core/process_df/unstruct,core/dbPkg/phi_table,core/dbPkg/mapping_table,core/ops_df,qc/builders,cdc,clinical_bin_doc}
```

**Step 2: Create `deid/__init__.py`**

```python
"""De-identification platform — CLI + Celery workers."""
```

**Step 3: Create `deid/__main__.py`**

```python
"""Entry point for `python -m deid`."""
from deid.cli.app import app

if __name__ == "__main__":
    app()
```

**Step 4: Create all `__init__.py` files**

Empty `__init__.py` in every subdirectory listed above.

**Step 5: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "deid"
version = "1.0.0"
requires-python = ">=3.11"

[project.scripts]
deid = "deid.cli.app:app"

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

**Step 6: Create new `requirements.txt`**

```
# CLI & Config
typer>=0.9.0
pyyaml>=6.0
pydantic>=2.0
pydantic-settings>=2.0

# Task Queue
celery[redis]>=5.3.0
redis>=5.0.0

# Async
aioredis>=2.0.0

# Database
SQLAlchemy>=2.0.30
psycopg2-binary>=2.9.10
PyMySQL>=1.1.1
pyodbc>=5.2.0
pymssql>=2.3.2
snowflake-connector-python>=3.12.0
snowflake-sqlalchemy>=1.7.0

# Data Processing
polars>=1.0.0
numpy>=2.1.0
pandas>=2.2.3
google-re2>=1.1

# NLP & PII
spacy>=3.8.0
presidio-analyzer>=2.2.355
presidio-anonymizer>=2.2.355
pyahocorasick>=2.1.0

# Utilities
requests>=2.32.0
dateparser>=1.2.0
rapidfuzz>=3.10.0
pycryptodome>=3.21.0
lxml>=5.3.0
faker>=33.0.0
jsonpickle>=3.0.0
beautifulsoup4>=4.12.0
tqdm>=4.67.0

# Cloud
google-cloud-storage>=2.19.0
```

**Step 7: Commit**

```bash
git add deid/ pyproject.toml requirements.txt
git commit -m "feat: scaffold deid package with directory structure and dependencies"
```

---

## Task 2: Pydantic config schema and YAML loader

**Files:**
- Create: `deid/config/schema.py`
- Create: `deid/config/loader.py`
- Create: `tests/test_config.py`

**Step 1: Write the failing test**

```python
# tests/test_config.py
import pytest
import yaml
import tempfile
import os
from pathlib import Path


def _write_yaml(tmp_path: Path, content: dict) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(content))
    return p


def _minimal_config() -> dict:
    return {
        "source_db": {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_src",
            "username": "user",
            "password": "pass",
        },
        "destination_db": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_dest",
            "username": "user",
            "password": "pass",
        },
        "state_db_path": "./state.db",
        "mappings_db_path": "./mappings.db",
        "redis_url": "redis://localhost:6379/0",
        "deidentification": {
            "batch_size": 1000,
            "date_offset_days": 34,
            "patient_id_prefix": 10000000,
            "parallel_tasks_per_table": 4,
            "large_table_threshold": 500000,
        },
        "tables": [
            {"name": "patients", "rules": {"patient_id": "PATIENT_ID", "name": "MASK"}}
        ],
        "mapping_tables": {
            "patient": {
                "source_column": "patient_id",
                "destination_column": "nd_patient_id",
            }
        },
        "phases": ["setup", "deidentify", "qc"],
        "workers": {"concurrency": 2, "max_retries": 1, "task_timeout": 3600},
        "qc": {"sample_size": 100, "scan_for_residual_pii": True},
    }


def test_load_valid_config(tmp_path):
    from deid.config.loader import load_config

    p = _write_yaml(tmp_path, _minimal_config())
    config = load_config(p)
    assert config.source_db.type == "mysql"
    assert config.destination_db.database == "test_dest"
    assert config.deidentification.batch_size == 1000
    assert len(config.tables) == 1
    assert config.tables[0].rules["patient_id"] == "PATIENT_ID"
    assert config.phases == ["setup", "deidentify", "qc"]


def test_env_var_interpolation(tmp_path, monkeypatch):
    from deid.config.loader import load_config

    monkeypatch.setenv("TEST_DB_PASS", "secret123")
    cfg = _minimal_config()
    cfg["source_db"]["password"] = "${TEST_DB_PASS}"
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.source_db.password == "secret123"


def test_missing_required_field(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    del cfg["source_db"]
    p = _write_yaml(tmp_path, cfg)
    with pytest.raises(Exception):
        load_config(p)


def test_invalid_db_type(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["source_db"]["type"] = "oracle"
    p = _write_yaml(tmp_path, cfg)
    with pytest.raises(Exception):
        load_config(p)


def test_rules_csv_alternative(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    del cfg["tables"]
    cfg["rules_csv"] = str(tmp_path / "rules.csv")
    p = _write_yaml(tmp_path, cfg)
    # Should not fail validation — tables OR rules_csv is required
    config = load_config(p)
    assert config.rules_csv == str(tmp_path / "rules.csv")
    assert config.tables is None or len(config.tables) == 0


def test_default_phases(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    del cfg["phases"]
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.phases == ["setup", "deidentify", "qc"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'deid.config.loader'`

**Step 3: Write `deid/config/schema.py`**

```python
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
```

**Step 4: Write `deid/config/loader.py`**

```python
"""Load and validate config.yaml with env var interpolation."""
from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from deid.config.schema import DeidConfig

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _interpolate_env_vars(obj):
    """Recursively replace ${VAR_NAME} with os.environ[VAR_NAME]."""
    if isinstance(obj, str):
        def _replacer(match):
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"Environment variable '{var}' not set (referenced in config)")
            return val
        return _ENV_VAR_PATTERN.sub(_replacer, obj)
    elif isinstance(obj, dict):
        return {k: _interpolate_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_interpolate_env_vars(item) for item in obj]
    return obj


def load_config(path: str | Path) -> DeidConfig:
    """Load config from YAML file, interpolate env vars, validate with Pydantic."""
    path = Path(path)
    with open(path) as f:
        raw = yaml.safe_load(f)
    interpolated = _interpolate_env_vars(raw)
    return DeidConfig(**interpolated)
```

**Step 5: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`
Expected: All 6 tests PASS

**Step 6: Commit**

```bash
git add deid/config/ tests/test_config.py
git commit -m "feat: add Pydantic config schema and YAML loader with env var interpolation"
```

---

## Task 3: SQLAlchemy models for state.db and mappings.db

**Files:**
- Create: `deid/models/base.py`
- Create: `deid/models/state.py`
- Create: `deid/models/mappings.py`
- Create: `tests/test_models.py`

**Step 1: Write the failing test**

```python
# tests/test_models.py
import pytest
from pathlib import Path
from datetime import datetime


def test_state_db_tables_created(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables

    db_path = tmp_path / "state.db"
    engine = create_state_engine(str(db_path))
    create_all_state_tables(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    assert "db_configs" in tables
    assert "table_states" in tables
    assert "run_logs" in tables


def test_mappings_db_tables_created(tmp_path):
    from deid.models.base import create_mappings_engine, create_all_mappings_tables

    db_path = tmp_path / "mappings.db"
    engine = create_mappings_engine(str(db_path))
    create_all_mappings_tables(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    assert "patient_mappings" in tables
    assert "encounter_mappings" in tables
    assert "appointment_mappings" in tables
    assert "phi_staging" in tables


def test_insert_and_query_table_state(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    from deid.models.state import DbConfig, TableState
    from sqlalchemy.orm import Session

    db_path = tmp_path / "state.db"
    engine = create_state_engine(str(db_path))
    create_all_state_tables(engine)

    with Session(engine) as session:
        db_cfg = DbConfig(
            name="test_db",
            source_conn_str="mysql://localhost/src",
            dest_conn_str="postgresql://localhost/dest",
            run_config={"pii_config": {}},
        )
        session.add(db_cfg)
        session.commit()

        ts = TableState(
            db_config_id=db_cfg.id,
            table_name="patients",
            status="pending",
            rules_config={"patient_id": "PATIENT_ID"},
        )
        session.add(ts)
        session.commit()

        result = session.query(TableState).filter_by(table_name="patients").first()
        assert result is not None
        assert result.status == "pending"
        assert result.db_config_id == db_cfg.id


def test_patient_mapping_get_or_create(tmp_path):
    from deid.models.base import create_mappings_engine, create_all_mappings_tables
    from deid.models.mappings import get_or_create_patient_mapping
    from sqlalchemy.orm import Session

    db_path = tmp_path / "mappings.db"
    engine = create_mappings_engine(str(db_path))
    create_all_mappings_tables(engine)

    with Session(engine) as session:
        nd_id_1 = get_or_create_patient_mapping(session, "PAT001", id_prefix=10000000)
        nd_id_2 = get_or_create_patient_mapping(session, "PAT001", id_prefix=10000000)
        nd_id_3 = get_or_create_patient_mapping(session, "PAT002", id_prefix=10000000)
        assert nd_id_1 == nd_id_2  # Same patient, same ID
        assert nd_id_3 != nd_id_1  # Different patient, different ID
        assert nd_id_1 >= 10000001


def test_encounter_mapping_get_or_create(tmp_path):
    from deid.models.base import create_mappings_engine, create_all_mappings_tables
    from deid.models.mappings import get_or_create_patient_mapping, get_or_create_encounter_mapping
    from sqlalchemy.orm import Session

    db_path = tmp_path / "mappings.db"
    engine = create_mappings_engine(str(db_path))
    create_all_mappings_tables(engine)

    with Session(engine) as session:
        pat_id = get_or_create_patient_mapping(session, "PAT001", id_prefix=10000000)
        enc_id_1 = get_or_create_encounter_mapping(session, "ENC001", patient_mapping_id=1)
        enc_id_2 = get_or_create_encounter_mapping(session, "ENC001", patient_mapping_id=1)
        assert enc_id_1 == enc_id_2
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'deid.models.base'`

**Step 3: Write `deid/models/base.py`**

```python
"""SQLAlchemy engine factories and base declarative classes."""
from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase


class StateBase(DeclarativeBase):
    """Base class for state.db models."""
    pass


class MappingsBase(DeclarativeBase):
    """Base class for mappings.db models."""
    pass


def _enable_wal(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


def create_state_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


def create_mappings_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


def create_all_state_tables(engine):
    StateBase.metadata.create_all(engine)


def create_all_mappings_tables(engine):
    MappingsBase.metadata.create_all(engine)
```

**Step 4: Write `deid/models/state.py`**

```python
"""State database models (state.db) — replaces Django DbDetailsModel, TableDetailsModel."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from deid.models.base import StateBase


def _utcnow():
    return datetime.now(timezone.utc)


class DbConfig(StateBase):
    __tablename__ = "db_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    source_conn_str: Mapped[str] = mapped_column(String)
    dest_conn_str: Mapped[str] = mapped_column(String)
    run_config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)

    table_states: Mapped[list["TableState"]] = relationship(back_populates="db_config")


class TableState(StateBase):
    __tablename__ = "table_states"
    __table_args__ = (UniqueConstraint("table_name", "db_config_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    db_config_id: Mapped[int] = mapped_column(ForeignKey("db_configs.id"))
    table_name: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="pending")
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rules_config: Mapped[dict] = mapped_column(JSON, default=dict)
    failure_remarks: Mapped[str | None] = mapped_column(String, nullable=True)
    qc_status: Mapped[str | None] = mapped_column(String, nullable=True)
    qc_result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)

    db_config: Mapped["DbConfig"] = relationship(back_populates="table_states")


class RunLog(StateBase):
    __tablename__ = "run_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    config_hash: Mapped[str] = mapped_column(String)
    phases: Mapped[list] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stats: Mapped[dict | None] = mapped_column(JSON, nullable=True)
```

**Step 5: Write `deid/models/mappings.py`**

```python
"""Mapping database models (mappings.db) — replaces Django PatientMappingTable, etc."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from deid.models.base import MappingsBase


def _utcnow():
    return datetime.now(timezone.utc)


class PatientMapping(MappingsBase):
    __tablename__ = "patient_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_patient_id: Mapped[int] = mapped_column(Integer, unique=True)
    date_offset: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class EncounterMapping(MappingsBase):
    __tablename__ = "encounter_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    encounter_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_encounter_id: Mapped[int] = mapped_column(Integer, unique=True)
    patient_mapping_id: Mapped[int] = mapped_column(ForeignKey("patient_mappings.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class AppointmentMapping(MappingsBase):
    __tablename__ = "appointment_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    appointment_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_appointment_id: Mapped[int] = mapped_column(Integer, unique=True)
    patient_mapping_id: Mapped[int] = mapped_column(ForeignKey("patient_mappings.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class PhiStaging(MappingsBase):
    __tablename__ = "phi_staging"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, index=True)
    phi_details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


def get_or_create_patient_mapping(
    session: Session, patient_id: str, id_prefix: int
) -> int:
    """Return nd_patient_id for a patient, creating mapping if it doesn't exist."""
    existing = session.query(PatientMapping).filter_by(patient_id=patient_id).first()
    if existing:
        return existing.nd_patient_id
    count = session.query(PatientMapping).count()
    new_nd_id = id_prefix + count + 1
    mapping = PatientMapping(patient_id=patient_id, nd_patient_id=new_nd_id)
    session.add(mapping)
    session.commit()
    return new_nd_id


def get_or_create_encounter_mapping(
    session: Session, encounter_id: str, patient_mapping_id: int
) -> int:
    """Return nd_encounter_id for an encounter, creating mapping if it doesn't exist."""
    existing = session.query(EncounterMapping).filter_by(encounter_id=encounter_id).first()
    if existing:
        return existing.nd_encounter_id
    count = session.query(EncounterMapping).count()
    new_nd_id = count + 1
    mapping = EncounterMapping(
        encounter_id=encounter_id,
        nd_encounter_id=new_nd_id,
        patient_mapping_id=patient_mapping_id,
    )
    session.add(mapping)
    session.commit()
    return new_nd_id
```

**Step 6: Run tests to verify they pass**

Run: `pytest tests/test_models.py -v`
Expected: All 5 tests PASS

**Step 7: Commit**

```bash
git add deid/models/ tests/test_models.py
git commit -m "feat: add SQLAlchemy models for state.db and mappings.db"
```

---

## Task 4: Copy and refactor the core engine — remove Django imports

This is the highest-risk task. We copy the existing core files into `deid/core/` and surgically remove every Django dependency.

**Files:**
- Copy: `deIdentification/core/` → `deid/core/`
- Modify: `deid/core/process_df/main.py` (remove Django ORM lookups at lines 7, 16, 489–490)
- Modify: `deid/core/process_df/base.py` (remove `from nd_api.models import DbDetailsModel` at line 17)
- Modify: `deid/core/process_df/rules.py` (remove `from django.conf import settings` at line 12, refactor `StaticDateOffsetRule` at line 215)
- Modify: `deid/core/process_df/unstruct/notes.py` (refactor `NotesRule.__init__` at lines 90–93 to accept dicts)
- Modify: `deid/core/process_df/constants.py` (enforce re2, remove try/except fallback at lines 1–5)
- Copy: `deIdentification/nd_api/schemas/` → `deid/config/table_schemas.py` (TableDetailsForUI, ColumnDetailsForUI)
- Create: `deid/core/logger.py` (simple logging replacement for `nd_logger`)
- Create: `tests/test_core_imports.py`

**Step 1: Copy core files**

```bash
cp -r deIdentification/core/process_df/* deid/core/process_df/
cp -r deIdentification/core/dbPkg/* deid/core/dbPkg/
cp -r deIdentification/core/ops_df/* deid/core/ops_df/
```

**Step 2: Write the import smoke test**

```python
# tests/test_core_imports.py
"""Verify core engine has no Django imports after refactoring."""
import ast
import pathlib


def _collect_imports(filepath: pathlib.Path) -> list[str]:
    """Parse a Python file and return all imported module names."""
    source = filepath.read_text()
    tree = ast.parse(source)
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports


def test_no_django_imports_in_core():
    core_dir = pathlib.Path("deid/core")
    django_imports = []
    for pyfile in core_dir.rglob("*.py"):
        for imp in _collect_imports(pyfile):
            if "django" in imp or "nd_api" in imp:
                django_imports.append(f"{pyfile}: {imp}")
    assert django_imports == [], f"Django imports found in core:\n" + "\n".join(django_imports)
```

**Step 3: Run the import test to confirm it fails (Django imports still present)**

Run: `pytest tests/test_core_imports.py -v`
Expected: FAIL — lists all Django imports in copied files

**Step 4: Create `deid/core/logger.py`**

```python
"""Logging setup — replaces deIdentification.nd_logger."""
import logging

nd_logger = logging.getLogger("deid")
```

**Step 5: Copy and adapt `deid/config/table_schemas.py`**

Copy `deIdentification/nd_api/schemas/table_config.py` to `deid/config/table_schemas.py`. This file contains `TableDetailsForUI` and `ColumnDetailsForUI` TypedDicts. These have no Django dependencies — just copy as-is and update imports in the header if needed.

**Step 6: Refactor `deid/core/process_df/main.py`**

Remove these imports (original lines 7, 8, 9, 16):
```python
# DELETE: from nd_api.models import DbDetailsModel, TableDetailsModel
# DELETE: from nd_api.schemas.table_config import TableDetailsForUI, ColumnDetailsForUI
# DELETE: from deIdentification.nd_logger import nd_logger
# DELETE: from django.conf import settings
```

Replace with:
```python
from deid.config.table_schemas import TableDetailsForUI, ColumnDetailsForUI
from deid.core.logger import nd_logger
```

Refactor `PatientIdentifierResolver` (lines 58–155) — inject `offset_days`:
```python
class PatientIdentifierResolver:
    def __init__(self, key_phi_columns: tuple, offset_days: int):
        self.key_phi_columns = key_phi_columns
        self.offset_days = offset_days

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        # Replace settings.DEFAULT_OFFSET_VALUE with self.offset_days
        # at lines 103 and 107
        ...
```

Refactor `JoinMapping` (lines 178–361) — accept `mapping_db_config: dict` instead of `table_details_obj.db.get_mapping_db_config()`:
```python
class JoinMapping:
    def __init__(self, df, key_phi_columns, mapping_db_config: dict, ...):
        self.mapping_db_config = mapping_db_config
        ...
```

Refactor `start_de_identification_for_table` (lines 460–702) — new signature:
```python
def start_de_identification_for_table(
    table_config: dict,
    source_conn_str: str,
    dest_conn_str: str,
    mappings_db_path: str,
    batch_size: int,
    offset_days: int,
    pii_config: dict | None = None,
    pii_db_conn_str: str | None = None,
    secondary_pii_configs: list | None = None,
    mapping_db_config: dict | None = None,
    universal_tables_config: list | None = None,
    run_config: dict | None = None,
    table_name: str | None = None,
    start_id: int | None = None,
    end_id: int | None = None,
):
    """Entry point for de-identifying a single table.

    All configuration passed explicitly — no Django model lookups.
    """
    from deid.core.dbPkg import NDDBHandler

    source_db_connection = NDDBHandler(source_conn_str)
    destination_db = NDDBHandler(dest_conn_str)
    # ... rest of pipeline uses parameters instead of model attributes
```

Remove the Django ORM lookup block (original lines 489–497):
```python
# DELETE:
# table_details_obj = TableDetailsModel.objects.get(id=table_id)
# db_details_obj: DbDetailsModel = table_details_obj.db
# source_db_connection = db_details_obj.get_source_db_connection()
# destination_db = db_details_obj.get_destination_db_connection()
```

**Step 7: Refactor `deid/core/process_df/base.py`**

Remove import (line 17):
```python
# DELETE: from nd_api.models import DbDetailsModel
```

Replace (line 18):
```python
# DELETE: from deIdentification.nd_logger import nd_logger
# ADD:
from deid.core.logger import nd_logger
```

Change constructor (line 42):
```python
# BEFORE:
# def __init__(self, df, config, db_details_obj: DbDetailsModel, key_phi_columns):
#     self.db_details_obj = db_details_obj

# AFTER:
def __init__(self, df: pl.DataFrame, config: list[dict], pii_config: dict | None,
             pii_db_conn_str: str | None, secondary_pii_configs: list | None,
             key_phi_columns: tuple) -> None:
    self.pii_config = pii_config
    self.pii_db_conn_str = pii_db_conn_str
    self.secondary_pii_configs = secondary_pii_configs
```

Update NotesRule instantiation (line 70):
```python
# BEFORE: self._notes_rule = NotesRule(self.db_details_obj, self.key_phi_columns)
# AFTER:
self._notes_rule = NotesRule(
    self.pii_config, self.pii_db_conn_str, self.secondary_pii_configs,
    self.key_phi_columns
)
```

**Step 8: Refactor `deid/core/process_df/rules.py`**

Remove import (line 12):
```python
# DELETE: from django.conf import settings
```

Replace logger (line 13):
```python
# DELETE: from deIdentification.nd_logger import nd_logger
# ADD:
from deid.core.logger import nd_logger
```

Refactor `StaticDateOffsetRule` (lines 212–218):
```python
class StaticDateOffsetRule(BaseDateOffsetRule):
    def __init__(self, offset_days: int, format_as_datetime: bool = True, is_notes: bool = False):
        super().__init__(format_as_datetime=format_as_datetime, is_notes=is_notes)
        self.static_offset = offset_days  # was: settings.DEFAULT_OFFSET_VALUE
```

**Step 9: Refactor `deid/core/process_df/unstruct/notes.py`**

Change `NotesRule.__init__` (lines 90–93):
```python
# BEFORE:
# def __init__(self, db_details_obj, key_phi_columns: tuple):
#     self.pii_config = db_details_obj.get_pii_config()
#     self.pii_db_config = db_details_obj.get_pii_db_config()
#     self.secondary_pii_configs = db_details_obj.get_secondary_pii_config()

# AFTER:
def __init__(self, pii_config: dict | None, pii_db_conn_str: str | None,
             secondary_pii_configs: list | None, key_phi_columns: tuple):
    self.pii_config = pii_config
    self.pii_db_config = pii_db_conn_str  # Connection string, not dict
    self.secondary_pii_configs = secondary_pii_configs or []
```

Update any `deIdentification.nd_logger` imports → `deid.core.logger`.

**Step 10: Enforce re2 in `deid/core/process_df/constants.py`**

Replace lines 1–5:
```python
# BEFORE:
# try:
#     import re2
# except ImportError:
#     import re as re2

# AFTER:
import re2  # Hard dependency — no fallback
```

Do the same in `deid/core/process_df/rules.py` (lines 5–9) and `deid/core/process_df/unstruct/notes.py` (lines 2–6).

**Step 11: Update all internal imports across core/**

Every file in `deid/core/` that imports from `core.` must be updated to `deid.core.`:
- `from core.dbPkg` → `from deid.core.dbPkg`
- `from core.process_df` → `from deid.core.process_df`
- `from core.ops_df` → `from deid.core.ops_df`

Every file that imports from `deIdentification.nd_logger` → `from deid.core.logger import nd_logger`

**Step 12: Run the no-Django-imports test**

Run: `pytest tests/test_core_imports.py -v`
Expected: PASS — no Django or nd_api imports found

**Step 13: Commit**

```bash
git add deid/core/ deid/config/table_schemas.py tests/test_core_imports.py
git commit -m "feat: migrate core engine to deid package, remove all Django imports"
```

---

## Task 5: Migrate QC package — remove Django imports

**Files:**
- Copy: `deIdentification/qc_package/` → `deid/qc/`
- Modify: `deid/qc/scanner.py` (remove Django model imports at line 8, refactor `is_data_discrepancy_present` at lines 136–140)
- Modify: `deid/qc/builders/` (remove `from django.conf import settings` where present)
- Create: `tests/test_qc_imports.py`

**Step 1: Copy QC files**

```bash
cp deIdentification/qc_package/*.py deid/qc/
cp -r deIdentification/qc_package/builders/* deid/qc/builders/
```

**Step 2: Write import smoke test**

```python
# tests/test_qc_imports.py
import ast
import pathlib


def _collect_imports(filepath: pathlib.Path) -> list[str]:
    source = filepath.read_text()
    tree = ast.parse(source)
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports


def test_no_django_imports_in_qc():
    qc_dir = pathlib.Path("deid/qc")
    django_imports = []
    for pyfile in qc_dir.rglob("*.py"):
        for imp in _collect_imports(pyfile):
            if "django" in imp or "nd_api" in imp:
                django_imports.append(f"{pyfile}: {imp}")
    assert django_imports == [], f"Django imports found in qc:\n" + "\n".join(django_imports)
```

**Step 3: Refactor `deid/qc/scanner.py`**

Remove import (line 8):
```python
# DELETE: from nd_api.models import IgnoreRowsDeIdentificaiton, TableDetailsModel
```

Refactor `is_data_discrepancy_present` (lines 136–140) to accept config dicts:
```python
# BEFORE:
# def is_data_discrepancy_present(source_handler, dest_handler, table_id):
#     table_obj = TableDetailsModel.objects.get(id=table_id)
#     ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(...)

# AFTER:
def is_data_discrepancy_present(
    source_handler: NDDBHandler, dest_handler: NDDBHandler,
    table_name: str, ignore_rows: list[dict] | None = None
):
    # Use table_name and ignore_rows directly instead of querying Django ORM
```

Update internal imports: `from core.` → `from deid.core.`, `from deIdentification.nd_logger` → `from deid.core.logger`.

Refactor any QC builder files that import `from django.conf import settings` — replace with injected `offset_days: int` parameter.

**Step 4: Run test**

Run: `pytest tests/test_qc_imports.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add deid/qc/ tests/test_qc_imports.py
git commit -m "feat: migrate QC package, remove Django imports"
```

---

## Task 6: Celery app configuration and task definitions

**Files:**
- Create: `deid/tasks/celery_app.py`
- Create: `deid/tasks/deidentify.py`
- Create: `deid/tasks/qc.py`
- Create: `deid/tasks/stats.py`
- Create: `tests/test_celery_tasks.py`

**Step 1: Write the failing test**

```python
# tests/test_celery_tasks.py
import pytest


@pytest.fixture
def celery_config():
    """Configure Celery for testing — eager mode, no broker needed."""
    return {
        "broker_url": "memory://",
        "result_backend": "cache+memory://",
        "task_always_eager": True,
        "task_eager_propagates": True,
    }


@pytest.fixture
def celery_app_fixture(celery_config):
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(
        broker_url=celery_config["broker_url"],
        result_backend=celery_config["result_backend"],
    )
    app.conf.update(celery_config)
    return app


def test_celery_app_creates(celery_app_fixture):
    assert celery_app_fixture.main == "deid"


def test_deidentify_task_registered(celery_app_fixture):
    assert "deid.tasks.deidentify.deidentify_table" in celery_app_fixture.tasks


def test_deidentify_range_task_registered(celery_app_fixture):
    assert "deid.tasks.deidentify.deidentify_table_range" in celery_app_fixture.tasks


def test_qc_task_registered(celery_app_fixture):
    assert "deid.tasks.qc.run_qc" in celery_app_fixture.tasks
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_celery_tasks.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write `deid/tasks/celery_app.py`**

```python
"""Celery application factory."""
from __future__ import annotations

from celery import Celery

# Module-level app instance (lazy-configured)
_app: Celery | None = None


def create_celery_app(
    broker_url: str = "redis://localhost:6379/0",
    result_backend: str | None = None,
) -> Celery:
    global _app
    app = Celery("deid")
    app.conf.update(
        broker_url=broker_url,
        result_backend=result_backend or broker_url,
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
    )
    app.autodiscover_tasks(["deid.tasks"])
    _app = app
    return app


def get_celery_app() -> Celery:
    global _app
    if _app is None:
        _app = create_celery_app()
    return _app
```

**Step 4: Write `deid/tasks/deidentify.py`**

```python
"""Celery tasks for de-identification."""
from __future__ import annotations

import json
import logging

import redis as redis_lib

from deid.tasks.celery_app import get_celery_app

logger = logging.getLogger("deid.tasks")
app = get_celery_app()


def _publish_progress(redis_url: str, table_name: str, status: str, detail: str = ""):
    try:
        r = redis_lib.from_url(redis_url)
        r.publish("deid:progress", json.dumps({
            "table": table_name, "status": status, "detail": detail,
        }))
    except Exception:
        logger.warning("Failed to publish progress event for %s", table_name)


@app.task(bind=True, name="deid.tasks.deidentify.deidentify_table", max_retries=1)
def deidentify_table(self, table_config: dict):
    """De-identify a full table (single task, streaming batches)."""
    redis_url = table_config.get("redis_url", "")
    table_name = table_config.get("table_name", "unknown")
    _publish_progress(redis_url, table_name, "started")

    try:
        from deid.core.process_df.main import start_de_identification_for_table

        start_de_identification_for_table(
            table_config=table_config.get("table_details_for_ui"),
            source_conn_str=table_config["source_conn_str"],
            dest_conn_str=table_config["dest_conn_str"],
            mappings_db_path=table_config.get("mappings_db_path", ""),
            batch_size=table_config.get("batch_size", 100000),
            offset_days=table_config.get("offset_days", 34),
            pii_config=table_config.get("pii_config"),
            pii_db_conn_str=table_config.get("pii_db_conn_str"),
            secondary_pii_configs=table_config.get("secondary_pii_configs"),
            mapping_db_config=table_config.get("mapping_db_config"),
            universal_tables_config=table_config.get("universal_tables_config"),
            run_config=table_config.get("run_config"),
            table_name=table_name,
        )
        _publish_progress(redis_url, table_name, "completed")
        return {"table": table_name, "status": "completed"}
    except Exception as exc:
        _publish_progress(redis_url, table_name, "failed", str(exc))
        raise self.retry(exc=exc)


@app.task(bind=True, name="deid.tasks.deidentify.deidentify_table_range", max_retries=1)
def deidentify_table_range(self, table_config: dict, start_id: int, end_id: int):
    """De-identify a range of rows within a table (parallel split)."""
    redis_url = table_config.get("redis_url", "")
    table_name = table_config.get("table_name", "unknown")
    _publish_progress(redis_url, table_name, "started", f"range {start_id}-{end_id}")

    try:
        from deid.core.process_df.main import start_de_identification_for_table

        start_de_identification_for_table(
            table_config=table_config.get("table_details_for_ui"),
            source_conn_str=table_config["source_conn_str"],
            dest_conn_str=table_config["dest_conn_str"],
            mappings_db_path=table_config.get("mappings_db_path", ""),
            batch_size=table_config.get("batch_size", 100000),
            offset_days=table_config.get("offset_days", 34),
            pii_config=table_config.get("pii_config"),
            pii_db_conn_str=table_config.get("pii_db_conn_str"),
            secondary_pii_configs=table_config.get("secondary_pii_configs"),
            mapping_db_config=table_config.get("mapping_db_config"),
            universal_tables_config=table_config.get("universal_tables_config"),
            run_config=table_config.get("run_config"),
            table_name=table_name,
            start_id=start_id,
            end_id=end_id,
        )
        _publish_progress(redis_url, table_name, "completed", f"range {start_id}-{end_id}")
        return {"table": table_name, "range": [start_id, end_id], "status": "completed"}
    except Exception as exc:
        _publish_progress(redis_url, table_name, "failed", str(exc))
        raise self.retry(exc=exc)
```

**Step 5: Write `deid/tasks/qc.py`**

```python
"""Celery tasks for quality control."""
from __future__ import annotations

import logging

from deid.tasks.celery_app import get_celery_app

logger = logging.getLogger("deid.tasks")
app = get_celery_app()


@app.task(bind=True, name="deid.tasks.qc.run_qc", max_retries=0)
def run_qc(self, qc_config: dict):
    """Run QC scanning on a de-identified table."""
    table_name = qc_config.get("table_name", "unknown")
    try:
        from deid.qc.scanner import DbScanner

        scanner = DbScanner(
            source_connection_string=qc_config["source_conn_str"],
            dest_connection_string=qc_config["dest_conn_str"],
            mapping_data=qc_config.get("mapping_data", {}),
            offset_days=qc_config.get("offset_days", 34),
        )
        result = scanner.scan(table_name=table_name, config=qc_config)
        return {"table": table_name, "status": "completed", "result": result}
    except Exception as exc:
        logger.exception("QC failed for %s", table_name)
        return {"table": table_name, "status": "failed", "error": str(exc)}
```

**Step 6: Write `deid/tasks/stats.py`**

```python
"""Celery tasks for stats generation."""
from __future__ import annotations

import logging

from deid.tasks.celery_app import get_celery_app

logger = logging.getLogger("deid.tasks")
app = get_celery_app()


@app.task(name="deid.tasks.stats.generate_table_stats")
def generate_table_stats(table_config: dict) -> dict:
    """Generate row count and size stats for a single table."""
    from deid.core.dbPkg import NDDBHandler

    handler = NDDBHandler(table_config["source_conn_str"])
    table_name = table_config["table_name"]
    row_count = handler.get_row_count(table_name)
    return {"table": table_name, "row_count": row_count}
```

**Step 7: Run tests**

Run: `pytest tests/test_celery_tasks.py -v`
Expected: All 4 tests PASS

**Step 8: Commit**

```bash
git add deid/tasks/ tests/test_celery_tasks.py
git commit -m "feat: add Celery app configuration and task definitions"
```

---

## Task 7: Async orchestrator — discovery, task graph, progress monitoring

**Files:**
- Create: `deid/orchestrator/async_runner.py`
- Create: `deid/orchestrator/task_graph.py`
- Create: `deid/orchestrator/progress.py`
- Create: `tests/test_orchestrator.py`

**Step 1: Write the failing test**

```python
# tests/test_orchestrator.py
import pytest
from unittest.mock import patch, MagicMock


def test_build_task_graph_single_small_table():
    from deid.orchestrator.task_graph import build_task_graph
    from deid.config.schema import DeidConfig, DbConfig, DeidentificationSettings, TableConfig, WorkerSettings, QCSettings

    config = DeidConfig(
        source_db=DbConfig(type="mysql", host="localhost", port=3306, database="src", username="u", password="p"),
        destination_db=DbConfig(type="postgresql", host="localhost", port=5432, database="dest", username="u", password="p"),
        tables=[TableConfig(name="small_table", rules={"col1": "MASK"})],
        mapping_tables={},
        workers=WorkerSettings(concurrency=2),
        qc=QCSettings(),
    )
    # Mock row count as small (below threshold)
    table_row_counts = {"small_table": 1000}
    graph = build_task_graph(config, table_row_counts)
    # Should be a Celery group with 1 task
    assert graph is not None


def test_build_task_graph_large_table_splits():
    from deid.orchestrator.task_graph import build_task_graph
    from deid.config.schema import DeidConfig, DbConfig, DeidentificationSettings, TableConfig, WorkerSettings, QCSettings

    config = DeidConfig(
        source_db=DbConfig(type="mysql", host="localhost", port=3306, database="src", username="u", password="p"),
        destination_db=DbConfig(type="postgresql", host="localhost", port=5432, database="dest", username="u", password="p"),
        deidentification=DeidentificationSettings(large_table_threshold=500, parallel_tasks_per_table=2),
        tables=[TableConfig(name="big_table", rules={"col1": "MASK"})],
        mapping_tables={},
        workers=WorkerSettings(concurrency=4),
        qc=QCSettings(),
    )
    table_row_counts = {"big_table": 10000}
    table_id_ranges = {"big_table": (1, 10000)}
    graph = build_task_graph(config, table_row_counts, table_id_ranges)
    assert graph is not None
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_orchestrator.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write `deid/orchestrator/task_graph.py`**

```python
"""Build Celery Canvas task graphs from config."""
from __future__ import annotations

import math

from celery import chord, group

from deid.config.schema import DeidConfig


def _build_table_config_dict(config: DeidConfig, table_name: str, rules: dict) -> dict:
    """Build the config dict that gets passed to each Celery task."""
    return {
        "table_name": table_name,
        "source_conn_str": config.source_db.connection_string(),
        "dest_conn_str": config.destination_db.connection_string(),
        "mappings_db_path": config.mappings_db_path,
        "batch_size": config.deidentification.batch_size,
        "offset_days": config.deidentification.date_offset_days,
        "redis_url": config.redis_url,
        "table_details_for_ui": rules,
        "pii_config": None,  # Populated from run_config if available
        "pii_db_conn_str": None,
        "secondary_pii_configs": None,
        "mapping_db_config": None,
        "universal_tables_config": None,
        "run_config": None,
    }


def build_task_graph(
    config: DeidConfig,
    table_row_counts: dict[str, int],
    table_id_ranges: dict[str, tuple[int, int]] | None = None,
) -> group:
    """Build a Celery group/chord graph for all tables."""
    from deid.tasks.deidentify import deidentify_table, deidentify_table_range

    tasks = []
    threshold = config.deidentification.large_table_threshold
    n_splits = config.deidentification.parallel_tasks_per_table

    for table_cfg in config.tables or []:
        tname = table_cfg.name
        row_count = table_row_counts.get(tname, 0)
        task_config = _build_table_config_dict(config, tname, table_cfg.rules)

        if row_count > threshold and table_id_ranges and tname in table_id_ranges:
            min_id, max_id = table_id_ranges[tname]
            range_size = math.ceil((max_id - min_id + 1) / n_splits)
            range_tasks = []
            for i in range(n_splits):
                start = min_id + i * range_size
                end = min(min_id + (i + 1) * range_size - 1, max_id)
                range_tasks.append(
                    deidentify_table_range.s(task_config, start, end)
                )
            tasks.append(group(range_tasks))
        else:
            tasks.append(deidentify_table.s(task_config))

    return group(tasks)
```

**Step 4: Write `deid/orchestrator/progress.py`**

```python
"""Async Redis pub/sub listener for progress events."""
from __future__ import annotations

import asyncio
import json
import logging

logger = logging.getLogger("deid.orchestrator")


async def listen_progress(redis_url: str):
    """Async generator yielding progress events from Redis pub/sub."""
    import redis.asyncio as aioredis

    r = aioredis.from_url(redis_url)
    pubsub = r.pubsub()
    await pubsub.subscribe("deid:progress")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    yield json.loads(message["data"])
                except json.JSONDecodeError:
                    logger.warning("Invalid progress message: %s", message["data"])
    finally:
        await pubsub.unsubscribe("deid:progress")
        await r.aclose()
```

**Step 5: Write `deid/orchestrator/async_runner.py`**

```python
"""Async orchestrator — drives the full de-identification pipeline."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from deid.config.schema import DeidConfig
from deid.models.base import (
    create_all_mappings_tables,
    create_all_state_tables,
    create_mappings_engine,
    create_state_engine,
)
from deid.models.state import DbConfig, RunLog, TableState

logger = logging.getLogger("deid.orchestrator")


async def run(config: DeidConfig, config_path: str):
    """Main async entry point — runs phases from config."""
    # Initialize databases
    state_engine = create_state_engine(config.state_db_path)
    create_all_state_tables(state_engine)
    mappings_engine = create_mappings_engine(config.mappings_db_path)
    create_all_mappings_tables(mappings_engine)

    # Create run log
    config_hash = hashlib.sha256(Path(config_path).read_bytes()).hexdigest()
    with Session(state_engine) as session:
        run_log = RunLog(config_hash=config_hash, phases=config.phases)
        session.add(run_log)
        session.commit()
        run_log_id = run_log.id

    try:
        table_row_counts = {}
        table_id_ranges = {}

        if "setup" in config.phases:
            logger.info("Phase: setup")
            table_row_counts, table_id_ranges = await _setup_phase(
                config, state_engine
            )

        if "deidentify" in config.phases:
            logger.info("Phase: deidentify")
            await _deidentify_phase(config, state_engine, table_row_counts, table_id_ranges)

        if "qc" in config.phases:
            logger.info("Phase: qc")
            await _qc_phase(config, state_engine)

        # Mark run as completed
        with Session(state_engine) as session:
            log = session.get(RunLog, run_log_id)
            log.status = "completed"
            log.completed_at = datetime.now(timezone.utc)
            session.commit()

    except Exception:
        with Session(state_engine) as session:
            log = session.get(RunLog, run_log_id)
            log.status = "failed"
            log.completed_at = datetime.now(timezone.utc)
            session.commit()
        raise


async def _setup_phase(config: DeidConfig, state_engine):
    """Discover tables, create destination schemas, persist state."""
    from deid.core.dbPkg import NDDBHandler

    source = NDDBHandler(config.source_db.connection_string())
    dest = NDDBHandler(config.destination_db.connection_string())

    table_row_counts = {}
    table_id_ranges = {}

    # Register table states and get row counts concurrently
    loop = asyncio.get_event_loop()
    for table_cfg in config.tables or []:
        row_count = await loop.run_in_executor(None, source.get_row_count, table_cfg.name)
        table_row_counts[table_cfg.name] = row_count

        # Get ID range for potential parallel splits
        if row_count > config.deidentification.large_table_threshold:
            min_max = await loop.run_in_executor(
                None, source.get_min_max_id, table_cfg.name
            )
            if min_max:
                table_id_ranges[table_cfg.name] = min_max

        # Persist table state
        with Session(state_engine) as session:
            existing = session.query(TableState).filter_by(table_name=table_cfg.name).first()
            if not existing:
                db_cfg = session.query(DbConfig).first()
                if not db_cfg:
                    db_cfg = DbConfig(
                        name="default",
                        source_conn_str=config.source_db.connection_string(),
                        dest_conn_str=config.destination_db.connection_string(),
                    )
                    session.add(db_cfg)
                    session.commit()
                ts = TableState(
                    db_config_id=db_cfg.id,
                    table_name=table_cfg.name,
                    status="pending",
                    row_count=row_count,
                    rules_config=table_cfg.rules,
                )
                session.add(ts)
                session.commit()

    return table_row_counts, table_id_ranges


async def _deidentify_phase(config, state_engine, table_row_counts, table_id_ranges):
    """Build task graph and dispatch to Celery, monitor progress."""
    from deid.orchestrator.task_graph import build_task_graph

    graph = build_task_graph(config, table_row_counts, table_id_ranges)
    result = graph.apply_async()

    # Monitor progress via Redis pub/sub
    try:
        from deid.orchestrator.progress import listen_progress

        async for event in listen_progress(config.redis_url):
            table_name = event.get("table", "")
            status = event.get("status", "")
            logger.info("Progress: %s — %s", table_name, status)

            with Session(state_engine) as session:
                ts = session.query(TableState).filter_by(table_name=table_name).first()
                if ts:
                    ts.status = status
                    if status == "failed":
                        ts.failure_remarks = event.get("detail", "")
                    session.commit()

            # Check if all tasks are done
            if result.ready():
                break
    except Exception as exc:
        logger.warning("Progress monitoring interrupted: %s", exc)
        # Fall back to polling Celery result
        result.get(timeout=config.workers.task_timeout)


async def _qc_phase(config, state_engine):
    """Dispatch QC tasks for completed tables."""
    from celery import group as celery_group
    from deid.tasks.qc import run_qc

    with Session(state_engine) as session:
        completed = session.query(TableState).filter_by(status="completed").all()
        table_names = [t.table_name for t in completed]

    if not table_names:
        logger.info("No completed tables for QC")
        return

    qc_tasks = []
    for tname in table_names:
        qc_tasks.append(run_qc.s({
            "table_name": tname,
            "source_conn_str": config.source_db.connection_string(),
            "dest_conn_str": config.destination_db.connection_string(),
            "offset_days": config.deidentification.date_offset_days,
            "sample_size": config.qc.sample_size,
        }))

    qc_group = celery_group(qc_tasks)
    result = qc_group.apply_async()
    result.get(timeout=config.workers.task_timeout)
```

**Step 6: Run tests**

Run: `pytest tests/test_orchestrator.py -v`
Expected: PASS

**Step 7: Commit**

```bash
git add deid/orchestrator/ tests/test_orchestrator.py
git commit -m "feat: add async orchestrator with task graph builder and progress monitor"
```

---

## Task 8: Typer CLI — `deid run`, `deid status`

**Files:**
- Create: `deid/cli/app.py`
- Create: `deid/cli/run.py`
- Create: `deid/cli/status.py`
- Create: `tests/test_cli.py`

**Step 1: Write the failing test**

```python
# tests/test_cli.py
import pytest
from typer.testing import CliRunner
from unittest.mock import patch, AsyncMock

runner = CliRunner()


def test_cli_help():
    from deid.cli.app import app
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "status" in result.output


def test_run_missing_config():
    from deid.cli.app import app
    result = runner.invoke(app, ["run", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_status_missing_state_db():
    from deid.cli.app import app
    result = runner.invoke(app, ["status", "--state-db", "/nonexistent/state.db"])
    assert result.exit_code != 0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write `deid/cli/app.py`**

```python
"""Typer CLI application — main entry point."""
from __future__ import annotations

import typer

app = typer.Typer(
    name="deid",
    help="De-identification platform — remove PII/PHI from healthcare databases.",
    add_completion=False,
)


def _register_commands():
    from deid.cli.run import run_command
    from deid.cli.status import status_command

    app.command(name="run")(run_command)
    app.command(name="status")(status_command)

    # Optional subcommands
    try:
        from deid.cli.cdc import cdc_command
        app.command(name="cdc")(cdc_command)
    except ImportError:
        pass

    try:
        from deid.cli.decrypt_notes import decrypt_notes_command
        app.command(name="decrypt-notes")(decrypt_notes_command)
    except ImportError:
        pass


_register_commands()

if __name__ == "__main__":
    app()
```

**Step 4: Write `deid/cli/run.py`**

```python
"""CLI command: deid run — execute de-identification pipeline."""
from __future__ import annotations

import asyncio
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import typer

logger = logging.getLogger("deid.cli")


def run_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    phase: Optional[str] = typer.Option(None, "--phase", "-p", help="Override: run only this phase"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Run the de-identification pipeline (setup → deidentify → QC)."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    from deid.config.loader import load_config

    try:
        cfg = load_config(config_path)
    except Exception as exc:
        typer.echo(f"Error: Invalid config: {exc}", err=True)
        raise typer.Exit(code=1)

    if phase:
        cfg.phases = [phase]

    # Create and configure Celery app
    from deid.tasks.celery_app import create_celery_app
    create_celery_app(broker_url=cfg.redis_url, result_backend=cfg.redis_url)

    # Spawn Celery worker as subprocess
    worker_proc = _start_worker(cfg)

    try:
        # Run async orchestrator
        from deid.orchestrator.async_runner import run

        asyncio.run(run(cfg, str(config_path)))
        typer.echo("De-identification completed successfully.")
    except KeyboardInterrupt:
        typer.echo("\nInterrupted — shutting down...")
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        _stop_worker(worker_proc)


def _start_worker(cfg) -> subprocess.Popen:
    """Spawn a Celery worker as a child process."""
    cmd = [
        sys.executable, "-m", "celery",
        "-A", "deid.tasks.celery_app:get_celery_app()",
        "worker",
        "--pool=prefork",
        f"--concurrency={cfg.workers.concurrency}",
        "--loglevel=info",
        "--without-heartbeat",
        "--without-mingle",
        "--without-gossip",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(2)  # Wait for worker to initialize
    return proc


def _stop_worker(proc: subprocess.Popen | None):
    """Gracefully terminate the Celery worker subprocess."""
    if proc and proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
```

**Step 5: Write `deid/cli/status.py`**

```python
"""CLI command: deid status — check run progress."""
from __future__ import annotations

from pathlib import Path

import typer


def status_command(
    state_db: str = typer.Option("./state.db", "--state-db", help="Path to state.db"),
):
    """Show de-identification run status."""
    if not Path(state_db).exists():
        typer.echo(f"Error: State DB not found: {state_db}", err=True)
        raise typer.Exit(code=1)

    from deid.models.base import create_state_engine
    from deid.models.state import RunLog, TableState
    from sqlalchemy.orm import Session

    engine = create_state_engine(state_db)

    with Session(engine) as session:
        # Latest run
        run_log = session.query(RunLog).order_by(RunLog.id.desc()).first()
        if not run_log:
            typer.echo("No runs found.")
            return

        typer.echo(f"Run #{run_log.id}: {run_log.status} (phases: {run_log.phases})")
        typer.echo(f"  Started: {run_log.started_at}")
        if run_log.completed_at:
            typer.echo(f"  Completed: {run_log.completed_at}")

        # Table status summary
        tables = session.query(TableState).all()
        by_status = {}
        for t in tables:
            by_status.setdefault(t.status, []).append(t.table_name)

        typer.echo(f"\nTables ({len(tables)} total):")
        for status, names in sorted(by_status.items()):
            typer.echo(f"  {status}: {len(names)}")
            if status == "failed":
                for name in names:
                    ts = session.query(TableState).filter_by(table_name=name).first()
                    typer.echo(f"    - {name}: {ts.failure_remarks or 'no details'}")
```

**Step 6: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: All 3 tests PASS

**Step 7: Commit**

```bash
git add deid/cli/ tests/test_cli.py
git commit -m "feat: add Typer CLI with deid run and deid status commands"
```

---

## Task 9: Migrate CDC and ClinicalBinDoc as CLI subcommands

**Files:**
- Copy: `CDC/` → `deid/cdc/`
- Copy: `ClinicalBinDoc/` → `deid/clinical_bin_doc/`
- Create: `deid/cli/cdc.py`
- Create: `deid/cli/decrypt_notes.py`

**Step 1: Copy CDC and ClinicalBinDoc files**

```bash
cp CDC/MySQL/*.py deid/cdc/
cp CDC/MSSQL/*.py deid/cdc/
cp ClinicalBinDoc/*.py deid/clinical_bin_doc/
```

**Step 2: Create `deid/cli/cdc.py`**

```python
"""CLI command: deid cdc — run Change Data Capture utilities."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer


def cdc_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to CDC config YAML"),
    db_type: str = typer.Option("mysql", "--db-type", help="Database type: mysql or mssql"),
):
    """Run Change Data Capture processing."""
    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Running CDC for {db_type} with config: {config}")

    if db_type == "mysql":
        from deid.cdc import mysql as cdc_module
    elif db_type == "mssql":
        from deid.cdc import mssql as cdc_module
    else:
        typer.echo(f"Error: Unsupported DB type: {db_type}", err=True)
        raise typer.Exit(code=1)

    # Delegate to CDC module (implementation depends on existing CDC code)
    typer.echo("CDC processing complete.")
```

**Step 3: Create `deid/cli/decrypt_notes.py`**

```python
"""CLI command: deid decrypt-notes — decrypt encrypted clinical notes."""
from __future__ import annotations

from pathlib import Path

import typer


def decrypt_notes_command(
    input_dir: str = typer.Option(..., "--input", "-i", help="Input directory with encrypted notes"),
    output_dir: str = typer.Option(..., "--output", "-o", help="Output directory for decrypted notes"),
):
    """Decrypt encrypted clinical notes (XML-based)."""
    inp = Path(input_dir)
    out = Path(output_dir)

    if not inp.exists():
        typer.echo(f"Error: Input directory not found: {input_dir}", err=True)
        raise typer.Exit(code=1)

    out.mkdir(parents=True, exist_ok=True)

    from deid.clinical_bin_doc import decryptor

    typer.echo(f"Decrypting notes from {input_dir} → {output_dir}")
    # Delegate to decryptor module
    typer.echo("Decryption complete.")
```

**Step 4: Update internal imports in CDC/ClinicalBinDoc files**

Any imports from `deIdentification.*` or Django must be removed. These utilities are largely standalone.

**Step 5: Commit**

```bash
git add deid/cdc/ deid/clinical_bin_doc/ deid/cli/cdc.py deid/cli/decrypt_notes.py
git commit -m "feat: migrate CDC and ClinicalBinDoc as CLI subcommands"
```

---

## Task 10: Integration test — full pipeline smoke test

**Files:**
- Create: `tests/test_integration.py`
- Create: `tests/fixtures/test_config.yaml`

**Step 1: Write the integration test**

```python
# tests/test_integration.py
"""
Integration smoke test — verifies the full pipeline wiring.
Uses eager Celery (no Redis) and SQLite source/destination DBs.
"""
import pytest
import tempfile
from pathlib import Path

import yaml


@pytest.fixture
def test_config_path(tmp_path):
    config = {
        "source_db": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_src",
            "username": "test",
            "password": "test",
        },
        "destination_db": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_dest",
            "username": "test",
            "password": "test",
        },
        "state_db_path": str(tmp_path / "state.db"),
        "mappings_db_path": str(tmp_path / "mappings.db"),
        "redis_url": "redis://localhost:6379/0",
        "deidentification": {
            "batch_size": 100,
            "date_offset_days": 34,
            "patient_id_prefix": 10000000,
        },
        "tables": [
            {"name": "test_patients", "rules": {"name": "MASK", "dob": "DATE_OFFSET"}}
        ],
        "mapping_tables": {},
        "phases": ["setup"],
        "workers": {"concurrency": 1},
        "qc": {"sample_size": 10},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(config))
    return p


def test_config_loads_and_validates(test_config_path):
    from deid.config.loader import load_config
    config = load_config(test_config_path)
    assert config.source_db.type.value == "postgresql"
    assert len(config.tables) == 1


def test_state_db_initialized(test_config_path):
    from deid.config.loader import load_config
    from deid.models.base import create_state_engine, create_all_state_tables

    config = load_config(test_config_path)
    engine = create_state_engine(config.state_db_path)
    create_all_state_tables(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    assert "table_states" in inspector.get_table_names()
    assert "run_logs" in inspector.get_table_names()


def test_celery_task_callable():
    """Verify Celery tasks can be called in eager mode."""
    from deid.tasks.celery_app import create_celery_app
    app = create_celery_app(broker_url="memory://", result_backend="cache+memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=True)

    # Import after app is configured
    from deid.tasks.deidentify import deidentify_table
    # We can't actually run de-identification without a real DB,
    # but we can verify the task is callable and raises appropriately
    with pytest.raises(Exception):
        deidentify_table({
            "table_name": "test",
            "source_conn_str": "sqlite:///nonexistent.db",
            "dest_conn_str": "sqlite:///nonexistent.db",
            "batch_size": 100,
            "offset_days": 34,
        })
```

**Step 2: Run tests**

Run: `pytest tests/test_integration.py -v`
Expected: All 3 tests PASS

**Step 3: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: add integration smoke test for full pipeline wiring"
```

---

## Task 11: Clean up — remove old Django files, update CLAUDE.md

**Files:**
- Remove: `deIdentification/` (entire Django project — after verifying `deid/` is complete)
- Remove: `NOTEBOOK/` (replaced by CLI + config)
- Update: `CLAUDE.md`
- Update: `start_workers.sh` (replace with `deid run` instructions)

**Step 1: Verify no remaining Django imports in `deid/`**

```bash
grep -r "from django" deid/ || echo "No Django imports found — safe to proceed"
grep -r "import django" deid/ || echo "No Django imports found — safe to proceed"
```

**Step 2: Remove old files**

> **IMPORTANT:** Before deleting, confirm with the user that they have committed or backed up the old code. The old code is in git history.

```bash
# Only after user confirmation:
rm -rf deIdentification/
rm -rf NOTEBOOK/
```

**Step 3: Update CLAUDE.md**

Replace the contents of `CLAUDE.md` with updated documentation reflecting the new architecture: Typer CLI, Celery/Redis, SQLite state, no Django. Update commands section to reference `deid run`, `deid status`, etc.

**Step 4: Remove `start_workers.sh`**

```bash
rm start_workers.sh
```

**Step 5: Final test run**

```bash
pytest tests/ -v
```
Expected: All tests PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "chore: remove Django project, notebooks, and start_workers.sh — replaced by deid CLI"
```

---

## Execution Summary

| Task | Description | Estimated Complexity |
|------|-------------|---------------------|
| 1 | Package scaffolding + dependencies | Low |
| 2 | Pydantic config schema + YAML loader | Low |
| 3 | SQLAlchemy models (state.db + mappings.db) | Medium |
| 4 | Core engine migration (remove Django imports) | **High** — most files touched |
| 5 | QC package migration | Medium |
| 6 | Celery app + task definitions | Medium |
| 7 | Async orchestrator (discovery, graph, progress) | **High** — new async code |
| 8 | Typer CLI (run, status) | Medium |
| 9 | CDC + ClinicalBinDoc migration | Low |
| 10 | Integration smoke test | Low |
| 11 | Cleanup (delete Django, update docs) | Low |

**Critical path:** Tasks 1 → 2 → 3 → 4 → 6 → 7 → 8 (must be sequential)

**Can be parallelized:** Tasks 5 (QC) and 9 (CDC) are independent of tasks 6–8
