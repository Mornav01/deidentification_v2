# deid — Healthcare Database De-Identification Platform

A high-performance CLI tool for de-identifying healthcare databases at scale. It removes Protected Health Information (PHI) and Personally Identifiable Information (PII) from structured and unstructured data across MySQL, MSSQL, PostgreSQL, and Snowflake databases.

The platform reads a YAML configuration, dispatches de-identification tasks via Celery workers, tracks state in SQLite, and runs automated quality control checks after completion.

---

## Table of Contents

- [Migration from v1 (Django)](#migration-from-v1-django)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Architecture](#architecture)
- [De-Identification Rules](#de-identification-rules)
- [Quality Control](#quality-control)
- [CDC (Change Data Capture)](#cdc-change-data-capture)
- [Development](#development)

---

## Migration from v1 (Django)

This is a ground-up rewrite of the orchestration, task queue, configuration, and state management layers. The core de-identification engine (rules, NLP pipeline, database streaming) is preserved unchanged. The table below summarizes every major change and how the old functionality maps to the new implementation.

### What Changed

| Area | v1 (Django) | v2 (Current) | Why |
|------|-------------|--------------|-----|
| **Framework** | Django (full web framework) | No framework — pure Python + Typer CLI | Django was heavyweight for a batch processing tool; no web UI or REST API was needed |
| **API** | REST API via Django views (`nd_api/views/`) | No API — single CLI entry point (`deid run`) | The tool runs as a batch job, not a service; API added complexity without benefit |
| **Task Queue** | Custom PostgreSQL queue (`SELECT FOR UPDATE SKIP LOCKED` + `NOTIFY`/`LISTEN`) | Celery + Redis (prefork pool, Canvas for DAG) | Celery provides mature retry logic, monitoring, parallel workers, and task chaining out of the box |
| **Workers** | Custom `TaskWorker` class polling PostgreSQL in a `while True` loop | Celery prefork pool — spawned automatically by `deid run` | Celery's prefork pool handles process management, signal handling, and prefetch tuning |
| **State DB** | PostgreSQL (Django ORM: `DbDetailsModel`, `TableDetailsModel`, task/chain tables) | SQLite via SQLAlchemy 2.0 (`state.db` for run tracking, `mappings.db` for ID maps) | SQLite is zero-config, portable (just two files), and sufficient for state tracking; no external DB dependency |
| **ORM** | Django ORM (`models.py` in `nd_api/`) | SQLAlchemy 2.0 with `mapped_column` syntax | SQLAlchemy is framework-agnostic and supports all target databases natively |
| **Configuration** | Django `settings.py` + environment variables + database-stored config (`DbDetailsModel`, `TableDetailsModel`) | Single YAML file with Pydantic v2 validation and `${ENV_VAR}` interpolation | One file to version-control, review, and reproduce a run; no config scattered across DB rows and env vars |
| **Authentication** | Keycloak integration (`DISABLE_AUTHENTICATION` toggle) | Dropped entirely | A CLI batch tool doesn't need user authentication |
| **Operations** | Jupyter notebooks (`NOTEBOOK/DENT/setup.ipynb`, `run.ipynb`, `db-config-setup.ipynb`) | CLI commands: `deid run`, `deid status`, `deid cdc` | Notebooks were fragile for production use; CLI is scriptable, auditable, and CI/CD friendly |
| **Worker Scripts** | `start_workers.sh` (screen sessions + conda env activation) | `deid run` spawns workers automatically as a subprocess | Single command replaces manual screen management |
| **Progress** | Task status stored in PostgreSQL, polled by API | Redis pub/sub real-time events + SQLite state persistence | Sub-second progress updates; `deid status` reads state.db without connecting to Redis |
| **Task Dependencies** | Custom `Chain` model with DAG-based dependency tracking | Celery Canvas (`group`, `chord`, `chain`) | Celery Canvas is battle-tested for task DAGs and handles error propagation |
| **Logging** | `deIdentification.nd_logger` (custom Django logger) | Standard Python `logging.getLogger("deid")` | No Django dependency; same log output with standard library |

### Component-by-Component Migration Map

#### Task Queue & Workers

**Before (v1):**
```
Django management command → start_worker
  → TaskWorker.run() polls PostgreSQL every 5s
    → SELECT id FROM tasks WHERE status='pending' FOR UPDATE SKIP LOCKED LIMIT 1
    → Execute task function
    → UPDATE tasks SET status='completed'
    → Chain model triggers dependent tasks
```
- `worker/worker.py` — `TaskWorker` class with exponential backoff retry
- `worker/models/task.py` — PostgreSQL task table with status state machine and expiry
- `worker/models/chain.py` — DAG-based task dependency resolution

**After (v2):**
```
deid run → spawns `celery worker --pool=prefork`
  → Celery pulls tasks from Redis broker
  → Execute shared_task function
  → Result stored in Redis backend
  → Canvas (group/chord) handles dependencies
```
- `deid/tasks/celery_app.py` — Celery app factory with `task_acks_late`, `worker_prefetch_multiplier=1`
- `deid/tasks/deidentify.py` — `deidentify_table` and `deidentify_table_range` as `@shared_task`
- `deid/tasks/qc.py` — `run_qc` as `@shared_task`
- `deid/orchestrator/task_graph.py` — Builds `celery.group()` of tasks; large tables split into range tasks

#### Configuration & Database Models

**Before (v1):**
```python
# Django settings.py
DEFAULT_OFFSET_VALUE = int(os.environ.get("DEFAULT_OFFSET_VALUE", 34))
BATCH_SIZE_DURING_DE_IDENTIFICATION = int(os.environ.get("BATCH_SIZE_DURING_DE_IDENTIFICATION", 100000))

# Django ORM models (nd_api/models/)
class DbDetailsModel(models.Model):     # PostgreSQL
    db_name, source_conn_str, dest_conn_str, ...
class TableDetailsModel(models.Model):  # PostgreSQL
    table_name, db, columns_config (JSON), ...
class MappingTable(models.Model):       # PostgreSQL
class PHITable(models.Model):           # PostgreSQL
class IgnoreRowsDeIdentificaiton(models.Model):  # PostgreSQL
```

**After (v2):**
```python
# config.yaml (Pydantic-validated)
deidentification:
  date_offset_days: 34
  batch_size: 100000

# SQLAlchemy 2.0 models
class DbConfig(StateBase):           # state.db (SQLite)
class TableState(StateBase):         # state.db (SQLite)
class RunLog(StateBase):             # state.db (SQLite)
class PatientMapping(MappingsBase):  # mappings.db (SQLite)
class EncounterMapping(MappingsBase): # mappings.db (SQLite)
```

Key differences:
- `DbDetailsModel` → split into `DbConfig` (in state.db) and `source_db`/`destination_db` sections in YAML
- `TableDetailsModel` → `TableState` (runtime state) + `tables[].rules` (config in YAML)
- `IgnoreRowsDeIdentificaiton` → removed; invalid rows are now logged by `InvalidRowHandler` and row counts passed as parameters
- All mapping tables (`MappingTable`, `PHITable`) → `PatientMapping`, `EncounterMapping`, `AppointmentMapping`, `PhiStaging` in `mappings.db`

#### REST API → CLI

**Before (v1):** 15+ Django views handling DB registration, de-identification control, config upload/download, QC, stats, and cloud upload.

**After (v2):** Four CLI commands replace the entire API surface:

| Old API Endpoint | New CLI Equivalent |
|-----------------|-------------------|
| `POST /register-db/` | `source_db:` / `destination_db:` sections in config.yaml |
| `POST /upload-config/` | `deid generate-config` + `tables:` section in config.yaml |
| `POST /start-deidentification/` | `deid mapping` + `deid pii-table` + `deid run --config config.yaml` |
| `GET /status/` | `deid status --state-db ./state.db` |
| `POST /start-qc/` | `deid run --config config.yaml --phase qc` |
| `POST /run-cdc/` | `deid cdc --config cdc.yaml --db-type mysql` |
| `GET /download-config/` | The config is a version-controlled YAML file |
| `POST /upload-to-cloud/` | Not yet migrated (planned) |

#### Operational Notebooks → CLI

**Before (v1):**
```
NOTEBOOK/DENT/setup.ipynb       → Register DBs, create tables
NOTEBOOK/DENT/run.ipynb         → Set PARALLEL_TASKS_COUNT, trigger de-identification
NOTEBOOK/db-config-setup.ipynb  → Configure source/destination connections
NOTEBOOK/uploadconfig.ipynb     → Upload column mapping rules
```

**After (v2):**
```bash
# Setup steps (run once, before the pipeline):
deid generate-config --config config.yaml    # Introspect DB → rules CSV
deid mapping --config config.yaml            # Populate ID mapping tables
deid pii-table --config config.yaml          # Create PII lookup tables (if needed)

# Run the pipeline:
deid run --config config.yaml

# The YAML file replaces all notebook cells:
# - DB connections → source_db / destination_db
# - Parallel tasks → deidentification.parallel_tasks_per_table
# - Column rules  → tables[].rules
# - Phases        → phases: [setup, deidentify, qc]
```

#### Core Engine Changes

The core de-identification engine (`process_df/`, `dbPkg/`, `ops_df/`) is functionally unchanged. The only modifications were import path updates and removing Django model dependencies from function signatures:

| Function / Class | What Changed |
|-----------------|--------------|
| `start_de_identification_for_table()` | Signature changed from `(table_details_obj, db_details_obj)` Django models to explicit parameters: `(table_config, source_conn_str, dest_conn_str, batch_size, offset_days, ...)` |
| `DeIdentifier.__init__()` | `offset_days` passed as parameter instead of reading `django.conf.settings.DEFAULT_OFFSET_VALUE` |
| `StaticDateOffsetRule` | `offset_days` passed as constructor parameter instead of `settings.DEFAULT_OFFSET_VALUE` |
| `NotesRule.__init__()` | Changed from `(db_details_obj, key_phi_columns)` to `(pii_config, pii_db_conn_str, secondary_pii_configs, key_phi_columns)` — config dicts instead of Django model |
| `InvalidRowHandler.handle()` | No longer writes to `IgnoreRowsDeIdentificaiton` Django model; logs ignored rows instead |
| `is_data_discrepancy_present()` | Takes `table_name` and `ignore_row_count` as parameters instead of querying `TableDetailsModel.objects.get()` and `IgnoreRowsDeIdentificaiton.objects.filter()` |
| All imports | `from deIdentification.nd_logger` → `from deid.core.logger`; `from core.*` → `from deid.core.*`; `from nd_api.schemas.*` → `from deid.config.table_schemas` |

No changes were made to: rule logic, NLP pipeline, regex patterns, Polars DataFrame operations, SQLAlchemy database streaming, reference table joining, or QC detector algorithms.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Ensure Redis is running
redis-server

# 3. Create a config file (see Configuration section)
cp config.example.yaml config.yaml
# Edit config.yaml with your source/destination database details

# 4. Generate rules CSV from source DB schema (auto-assigns rules based on column names)
deid generate-config --config config.yaml
# Review and edit the generated CSV, then reference it via rules_csv in config.yaml

# 5. Populate mapping tables (patient, encounter, appointment ID mappings)
deid mapping --config config.yaml

# 6. Create PII tables and generate pii_config (only if pii_db is configured)
deid pii-table --config config.yaml

# 7. Run the pipeline (setup → deidentify → QC)
deid run --config config.yaml
```

> See [docs/quickstart.md](docs/quickstart.md) for a detailed walkthrough of each step.

---

## Installation

### Prerequisites

- Python >= 3.11
- Redis server (used as Celery broker and for progress events)
- Access to source and destination databases
- Spacy language model for unstructured text processing:
  ```bash
  python -m spacy download en_core_web_lg
  ```

### Install

```bash
# Install all dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .

# Optional (Apple Silicon only): local-LLM residual-PII backend for QC notes
pip install -e '.[mlx]'
```

This registers the `deid` CLI command via the entry point defined in `pyproject.toml`.

> The `mlx` extra installs `mlx-lm`, which has **no Linux/x86 wheels**. It is intentionally excluded
> from `requirements.txt` so the pipeline and QC install and run everywhere with the default `regex`
> residual-PII backend; enable `qc.residual_pii_backend: mlx` only where a Mac runs the QC task.

---

## Configuration

All settings are defined in a single YAML file. Environment variables can be interpolated using `${VAR_NAME}` syntax.

### Minimal Configuration

```yaml
source_db:
  type: mysql                         # mysql | mssql | postgresql | snowflake
  host: localhost
  port: 3306
  database: hospital_db
  username: ${SOURCE_DB_USER}         # Environment variable interpolation
  password: ${SOURCE_DB_PASSWORD}

destination_db:
  type: postgresql
  host: localhost
  port: 5432
  database: hospital_db_deid
  username: ${DEST_DB_USER}
  password: ${DEST_DB_PASSWORD}

tables:
  - name: patients
    rules:
      patient_id: PATIENT_ID
      first_name: MASK
      last_name: MASK
      date_of_birth: PATIENT_DOB
      ssn: MASK
      zip_code: ZIP_CODE
      admission_date: DATE_OFFSET
      discharge_date: DATE_OFFSET
      clinical_notes: NOTES
```

### Full Configuration Reference

```yaml
# ── Source & Destination Databases ──────────────────────────────────
source_db:
  type: mysql                         # mysql | mssql | postgresql | snowflake
  host: localhost
  port: 3306
  database: source_db
  username: user
  password: pass

destination_db:
  type: postgresql
  host: localhost
  port: 5432
  database: dest_db
  username: user
  password: pass

# ── State & Mappings Storage ────────────────────────────────────────
state_db_path: ./state.db             # SQLite DB for run tracking (default: ./state.db)
mappings_db_path: ./mappings.db       # SQLite DB for ID mappings (default: ./mappings.db)

# ── Redis ───────────────────────────────────────────────────────────
redis_url: redis://localhost:6379/0   # Celery broker + progress pub/sub

# ── De-Identification Settings ──────────────────────────────────────
deidentification:
  batch_size: 100000                  # Rows per batch — also the split unit for large tables (default: 100000)
  date_offset_days: 34                # Days to shift dates for STATIC_OFFSET (default: 34)
  patient_id_prefix: 10000000         # Prefix for anonymized patient IDs
  random_seed: 42                     # Seed for deterministic offset assignment

# ── Tables & Rules ──────────────────────────────────────────────────
tables:
  - name: patients
    rules:
      patient_id: PATIENT_ID          # See "De-Identification Rules" section
      first_name: MASK
      date_of_birth: PATIENT_DOB
      visit_date: DATE_OFFSET
      clinical_notes: NOTES

  - name: encounters
    rules:
      encounter_id: ENCOUNTER_ID
      patient_id: PATIENT_ID
      admit_date: DATE_OFFSET

# Alternative: load table/column rules from a CSV file (generated by deid generate-config)
# rules_csv: ./rules.csv

# ── PII Config (generated by deid pii-table) ──────────────────────
# pii_config_path: ./pii_config.yaml   # Path to pii_config YAML (auto-loaded at runtime)

# ── Mapping Tables ──────────────────────────────────────────────────
mapping_tables:
  patient:
    source_column: patient_id
    destination_column: nd_patient_id
  encounter:
    source_column: encounter_id
    destination_column: nd_encounter_id
    reference: patient                # Links encounter mapping to patient mapping

# ── Pipeline Phases ─────────────────────────────────────────────────
phases:                               # Phases to execute (default: all three)
  - setup                             # Discover tables, create destination schemas
  - deidentify                        # Run de-identification
  - qc                                # Run quality control checks

# ── Worker Settings ─────────────────────────────────────────────────
workers:
  fetchers: 2                         # Concurrency of the fetch worker pool
  processors: 16                      # Concurrency of the process (de-id) worker pool
  max_retries: 1                      # Task retry limit on failure
  max_batch_retries: 3                # Per-batch retry limit
  task_timeout: 3600                  # Max seconds per task (default: 1 hour)
  table_batch_size: 0                 # Process N tables per batch (0 = all at once)

# ── Quality Control ─────────────────────────────────────────────────
qc:
  sample_size: 100                    # Rows to sample for QC verification
  scan_for_residual_pii: true         # Run NLP-based residual PII scan on notes

  # Optional: feed the in-pipeline notes scan real PHI values from the master table
  # (without this, the notes exact-match set is empty and only the residual-PII scanner runs).
  pii_master_conn_str: mysql+pymysql://user:pass@host/master_db
  pii_columns: [users_ufname, users_ulname, users_upphone]   # default: all non-id cols

  # Residual-PII scanner backend for the notes scan (replaces Presidio):
  #   auto (default) — mlx on Apple Silicon when installed, else regex (ideal for mixed fleets)
  #   regex — portable, dependency-free | mlx — local LLM, Apple Silicon | none — disable
  residual_pii_backend: auto
  mlx_model: mlx-community/Llama-3.2-3B-Instruct-4bit   # used only when backend resolves to mlx

  # Part 2 — mapping & count checks, run as a BLOCKING gate before de-identification.
  # Empty `part2` → gate is a no-op. See "Quality Control" section for all keys.
  part2_blocking: true
  part2:
    mapping_target_pairs: [[patient_mapping_table, patients]]
    offset_min: -38
    offset_max: 38

  # Delta-identity QC (deid qc-delta) — row-level source↔dest diff on CDC delta.
  delta_identity:
    tables: [rwe_ad_mci_lab]
    delta_after: "2026-05-15"          # only check dest rows newer than this

  # Part 3 — master-referenced unstructured PHI audit (deid qc-audit).
  master_phi:
    pii_master_conn_str: mysql+pymysql://user:pass@host/master_db
    facility_names: [Northwest]
    check_address: true                # residual full street-address regex
    check_dates_in_notes: true         # flag leaked master dates + implausible dates in text
    date_columns: [users_dob]          # master date cols (default: auto by name)
    require_mask_token: false          # opt-in: assert a mask token replaced the PHI
    expected_mask_tokens: ["<<PATIENT_NAME>>"]
    sample_size: 500                   # 0 = whole table
    tables:
      - dest_table: rwe_ad_mci_lab
        content_cols: [content]
        name_columns: [users_ufname, users_ulname]
```

### Programmatic / Airflow use

Every QC task is an importable function (in [deid/qc/api.py](deid/qc/api.py)) that returns a structured
result and never exits the process — so an orchestrator can call it directly and raise/alert on the
outcome. The CLI commands are thin wrappers over these same functions.

```python
from deid.config.loader import load_config
from deid.qc.api import (
    run_part2_from_config,          # Part 2 gate — raises Part2Blocked on failure
    run_delta_identity_from_config, # Part 2 row-level diff — returns [ {is_qc_passed, ...} ]
    run_master_phi_from_config,     # Part 3 audit — returns [ {fail_count, coverage_gaps, ...} ]
    build_audit_report,             # consolidated report dict across all parts
)

cfg = load_config("config.yaml")
results = run_delta_identity_from_config(cfg)
if any(not r["is_qc_passed"] for r in results):
    raise ValueError("delta-identity QC failed")   # Airflow marks the task failed → alerts fire
```

### Environment Variable Interpolation

Any value in the YAML can reference environment variables:

```yaml
source_db:
  password: ${SOURCE_DB_PASSWORD}     # Replaced at load time with os.environ value
redis_url: ${REDIS_URL}
```

If the environment variable is not set, config loading will raise an error.

---

## Usage

### Prerequisites: Generate Config, Mappings, and PII Tables

Before running the pipeline, you must set up the mapping tables (and optionally PII tables). These are separate commands so they can be run independently, reviewed, and re-run without re-running the full pipeline.

#### Generate Rules Config

```bash
# Introspect source DB and generate a rules CSV with auto-assigned de-identification rules
deid generate-config --config config.yaml

# Filter to specific tables
deid generate-config --config config.yaml --tables patients encounters

# Override output path (default: from config.rules_csv or config_rules.csv)
deid generate-config --config config.yaml --output ./my_rules.csv

# Specify a database schema
deid generate-config --config config.yaml --schema dbo
```

Review the generated CSV — auto-assigned rules are based on column name patterns and may need manual adjustment. Reference the CSV in your config via `rules_csv: ./rules.csv`.

#### Populate Mapping Tables

```bash
# Create and populate patient/encounter/appointment ID mappings
deid mapping --config config.yaml

# Override the mappings DB path
deid mapping --config config.yaml --mappings-db ./custom_mappings.db
```

This scans source tables for columns with `PATIENT_ID`, `ENCOUNTER_ID`, and `APPOINTMENT_ID` rules, fetches distinct IDs, and creates anonymized mappings in the SQLite mappings database.

#### Create PII Tables (optional — only if `pii_db` is configured)

```bash
# Create PII lookup tables and generate pii_config YAML
deid pii-table --config config.yaml

# Override the pii_config output path
deid pii-table --config config.yaml --pii-config-output ./my_pii_config.yaml
```

This creates PII lookup tables in the destination PII database (used by `NOTES` rules for patient name masking in free text) and writes the generated `pii_config.yaml` file. Reference it in your config via `pii_config_path: ./pii_config.yaml`.

### Run the Pipeline

```bash
deid run --config config.yaml
```

`deid run` validates that mapping tables and PII tables (if configured) exist before starting. If they are missing, it will error with instructions to run the prerequisite commands.

The pipeline executes three phases in order:

1. **Setup** — Connects to the source database, discovers table schemas, counts rows, creates destination table structures, and records initial state in `state.db`.
2. **Deidentify** — Builds a Celery task graph and dispatches de-identification tasks to worker processes. Large tables (exceeding `large_table_threshold`) are automatically split into parallel ID-range tasks. Progress is streamed via Redis pub/sub.
3. **QC** — Samples rows from source and destination, runs column-level detectors to verify de-identification was applied correctly, and checks for row count discrepancies.

### Run a Single Phase

```bash
deid run --config config.yaml --phase setup
deid run --config config.yaml --phase deidentify
deid run --config config.yaml --phase qc
```

### Check Run Status

```bash
deid status
deid status --state-db /path/to/state.db
```

Displays the latest run status, phase progress, and per-table breakdown (pending, completed, failed).

### Set Log Level

```bash
deid run --config config.yaml --log-level DEBUG
```

### Quality Control Commands

```bash
deid qc --config config.yaml                    # Part 1/3 in-pipeline scan (also: run --phase qc)
deid qc-delta --config config.yaml              # Part 2 row-level source↔dest identity diff
deid qc-audit --config config.yaml --report     # Part 3 master-referenced notes audit + report
```

Part 2's mapping & count gate runs automatically inside `deid run` (before de-identification) when
`qc.part2` is configured. See the [Quality Control](#quality-control) section for the full framework.

### CDC (Change Data Capture)

```bash
deid cdc --config cdc_config.yaml --db-type mysql
deid cdc --config cdc_config.yaml --db-type mssql
```

### Decrypt Clinical Notes

```bash
deid decrypt-notes --input ./encrypted_notes --output ./decrypted_notes
```

---

## Architecture

### System Overview

```
                          ┌─────────────────┐
                          │   config.yaml   │
                          └────────┬────────┘
                                   │
                                   v
                       ┌───────────────────────┐
                       │  deid run (Typer CLI)  │
                       └───────────┬───────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │   Async Orchestrator         │
                    │   (asyncio event loop)       │
                    │                              │
                    │  ┌──────────────────────┐    │
                    │  │  Phase 1: Setup      │    │
                    │  │  - Table discovery   │    │
                    │  │  - Row counting      │    │
                    │  │  - Batch splitting   │    │
                    │  │  - State init        │    │
                    │  └──────────┬───────────┘    │
                    │             │                 │
                    │  ┌──────────v───────────┐    │
                    │  │  Phase 2: Deidentify │    │
                    │  │  - Build task graph  │────┼──────┐
                    │  │  - Monitor progress  │    │      │
                    │  └──────────┬───────────┘    │      │
                    │             │                 │      │
                    │  ┌──────────v───────────┐    │      │
                    │  │  Phase 3: QC         │    │      │
                    │  │  - Sample & verify   │    │      │
                    │  └──────────────────────┘    │      │
                    └──────────────────────────────┘      │
                                                          │
                    ┌─────────────────────────────────────┘
                    │   Celery Canvas (task graph)
                    │
          ┌─────────v──────────┐
          │   Redis Broker     │
          └─────────┬──────────┘
                    │
        ┌───────────┴───────────────────┐
        │                               │
┌───────v────────┐            ┌─────────v──────┐
│ Celery Worker  │   . . .   │ Celery Worker  │
│ (prefork pool) │            │ (prefork pool) │
└───────┬────────┘            └────────────────┘
        │
        │  Per-batch processing:
        │  ┌─────────────────────────────────────┐
        │  │ 1. Stream rows from source DB       │
        │  │ 2. Join mapping tables              │
        │  │ 3. Resolve patient identifiers      │
        │  │ 4. Apply de-identification rules    │
        │  │    - NLP notes rules (first)        │
        │  │    - Structured rules (second)      │
        │  │ 5. Filter invalid rows              │
        │  │ 6. Insert into destination DB       │
        │  │ 7. Publish progress to Redis        │
        │  └─────────────────────────────────────┘
        │
        v
┌──────────────────┐     ┌──────────────────┐
│ Source Database   │     │ Destination DB   │
│ (MySQL/MSSQL/    │     │ (de-identified)  │
│  PG/Snowflake)   │     │                  │
└──────────────────┘     └──────────────────┘
```

### Package Structure

```
deid/
├── __main__.py             # `python -m deid` entry point
├── staging.py              # Arrow IPC staging-directory helpers + crash-recovery reconciliation
│
├── cli/                    # Typer CLI commands
│   ├── app.py              #   Main entry point, command registration
│   ├── run.py              #   deid run — orchestrate full pipeline
│   ├── generate_config.py  #   deid generate-config — generate rules CSV from source DB
│   ├── mapping.py          #   deid mapping — create and populate mapping tables
│   ├── pii_table.py        #   deid pii-table — create PII tables + pii_config
│   ├── status.py           #   deid status — display run progress
│   ├── retry.py            #   deid retry — re-run failed batches
│   ├── qc.py               #   deid qc — Part 1/3 in-pipeline scan (DbScanner)
│   ├── qc_delta.py         #   deid qc-delta — Part 2 row-level source↔dest diff
│   ├── qc_audit.py         #   deid qc-audit — Part 3 master-referenced notes audit
│   ├── cdc.py              #   deid cdc — change data capture
│   └── decrypt_notes.py    #   deid decrypt-notes — decrypt ClinicalBin documents
│
├── config/                 # Configuration layer
│   ├── schema.py           #   Pydantic v2 models for config.yaml validation
│   ├── loader.py           #   YAML loading + ${ENV_VAR} interpolation
│   ├── rules_generator.py  #   Auto-assign rules from DB schema (used by generate-config)
│   ├── pii_generator.py    #   Auto-generate PII table config + column mask values
│   ├── table_schemas.py    #   TypedDicts for runtime table/column config (UI schemas)
│   └── task_models.py      #   Pydantic models for task/orchestrator/QC boundaries + LogLevel
│
├── models/                 # SQLAlchemy 2.0 models (SQLite)
│   ├── base.py             #   Engine factories with WAL + busy_timeout=5000;
│   │                       #   get_cached_state_engine(db_path) — per-process cache
│   │                       #   used by all Celery tasks for the hot batch update path
│   ├── state.py            #   DbConfig, TableState, BatchState, RunLog
│   ├── mappings.py         #   PatientMapping, EncounterMapping, AppointmentMapping, PhiStaging
│   ├── failed_rows.py      #   Per-source-schema failed_rows_{schema} tables
│   └── qc_results.py       #   QCTableResult, QCPart2Result, QCDeltaIdentityResult,
│                           #   QCUnstructuredAuditResult (qc_results.db)
│
├── orchestrator/           # Async pipeline orchestrator
│   ├── async_runner.py     #   Drives setup → deidentify phases; polls BatchState;
│   │                       #   watchdog re-dispatches stalled table chains
│   ├── task_graph.py       #   Builds Celery Canvas task graphs from config
│   ├── log_collector.py    #   Async Redis pub/sub log aggregation + summary
│   └── progress.py         #   Redis pub/sub listener for progress events
│
├── tasks/                  # Celery task definitions (3-stage pipeline)
│   ├── celery_app.py       #   App factory; preloads mapping tables on workers
│   ├── fetch.py            #   fetch_batch — keyset-paginated source → Arrow IPC;
│   │                       #   self-chains next fetch; idempotency guard recovers
│   │                       #   from acks_late re-delivery; failures reset to pending
│   ├── process.py          #   process_batch — Arrow IPC → mapping joins →
│   │                       #   de-identification → Arrow IPC; idempotent on re-delivery
│   ├── write.py            #   write_batch — Arrow IPC → idempotent dest DELETE+INSERT;
│   │                       #   per-process _created_dest_tables guard skips DDL
│   ├── batch_utils.py      #   Shared BatchState helpers used by fetch/process/write
│   ├── qc.py               #   run_qc — dispatches DbScanner; results to qc_results.db
│   └── deidentify.py       #   Legacy single-task path
│
├── core/                   # Core de-identification engine
│   ├── logger.py           #   Logging setup (nd_logger)
│   ├── log_publisher.py    #   Structured log records → Redis; peak-memory sampling
│   ├── mapping_populator.py #  Scan rule configs for ID columns; bulk-insert patient mappings
│   ├── process_df/         #   De-identification logic
│   │   ├── main.py         #     Batch orchestrator (stream → join → resolve → rules → write);
│   │   │                   #     PatientIdentifierResolver, JoinMapping, get_key_phi_column_list
│   │   ├── base.py         #     DeIdentifier class, RULE_DISPATCHER
│   │   ├── rules.py        #     Rules enum + structured rule classes (PatientID, Mask, …)
│   │   ├── columns_type_detector.py # Map rules → destination column SQL types
│   │   ├── constants.py    #     Regex patterns (dates, ZIP codes)
│   │   ├── rowhandler.py   #     InvalidRowHandler (filter/log null _resolved_nd_patient_id)
│   │   ├── exception.py    #     Custom exceptions
│   │   └── unstruct/       #     Unstructured text (clinical notes)
│   │       ├── notes.py    #       NotesRule — NLP-based PII extraction (Presidio + Spacy)
│   │       ├── genericnotes.py #   GenericNotesRule — Spacy/Presidio generic PHI detection
│   │       ├── xml.py      #       XML/SOAP note de-identification
│   │       ├── xml_utils.py #      XML parsing/serialization helpers
│   │       └── utils.py    #       Shared notes helpers
│   ├── dbPkg/              #   Database abstraction layer
│   │   ├── dbhandler.py    #     NDDBHandler (streaming reads, batch inserts, schema ops)
│   │   ├── type_mapping.py #     MSSQL → MySQL type mapping
│   │   ├── schemas.py      #     DB-layer schema helpers
│   │   ├── mapping_loader.py #   Load patient/encounter mapping tables
│   │   ├── pii_loader.py   #     Load PII staging tables
│   │   ├── mapping_table/  #     Mapping-table DDL/helpers
│   │   └── phi_table/      #     PHI/PII table creation (create_table.py)
│   └── ops_df/             #   DataFrame operations
│       ├── jointables.py   #     Multi-hop reference table joining (ReferenceMappingDataFrameJoiner)
│       └── utility.py      #     join_dataframes (Polars left-join helper), DistinctValueFetcher
│
├── clinical_bin_doc/       # ClinicalBin (MSSQL) binary document extraction
│   └── extractor.py        #   Extract + decrypt clinical binary documents
│
└── qc/                     # Quality control (3-part QC Framework)
    ├── scanner.py          #   DbScanner — Part 1/3 in-pipeline scan; get_pii_info loads master PHI
    ├── generator.py        #   DataGenerator — stratified sampling from databases
    ├── schema.py           #   Output schemas (ColumnQCResult, FinalQCResult)
    ├── mapping_count.py    #   Part 2 — mapping/count checks + blocking pre-run gate
    ├── delta_identity.py   #   Part 2 — polars row-level source↔dest diff (cdc_id_validation port)
    ├── master_phi.py       #   Part 3 — master-referenced unstructured PHI audit
    ├── coverage.py         #   Part 3 — coverage check (count parity + NULL/empty)
    ├── report.py           #   Consolidated audit report across Parts 1–3
    ├── api.py              #   Public API — *_from_config task functions (Airflow entry point)
    ├── llm_scan.py         #   Residual-PII scanner: regex (default) | mlx local-LLM | none
    └── builders/           #   Part 1 detector classes
        ├── base.py         #     Abstract Detector interface
        ├── structured.py   #     ID (incl. APPOINTMENT/CHART), mask, date, ZIP, DOB detectors
        └── unstructured.py #     Notes scan: master exact-match + pluggable residual-PII scanner
```

> Change Data Capture lives at the **repo root** (not inside the `deid/` package): `CDC/MySQL/`
> and `CDC/MSSQL/` hold the CDC SQL/parsers driven by `deid cdc` (`deid/cli/cdc.py`).
> `ClinicalBinDoc/` at the repo root accompanies `deid/clinical_bin_doc/`.

### State Management

The platform uses four SQLite databases for persistent state:

**`state.db`** — Run tracking and table progress:
| Table | Purpose |
|-------|---------|
| `db_configs` | Source/destination connection metadata snapshot (no passwords) |
| `table_states` | Per-table status (pending/started/completed/failed), row counts, rules config |
| `batch_states` | Per-batch state machine: `pending → dispatched → fetched → processed → done`. Failed tasks reset to `pending` for the watchdog to re-dispatch. |
| `run_logs` | Overall run tracking with config hash, phases, timing |

**`mappings.db`** — ID mapping tables:
| Table | Purpose |
|-------|---------|
| `patient_mappings` | Original patient ID → anonymized patient ID + date offset |
| `encounter_mappings` | Original encounter ID → anonymized encounter ID |
| `appointment_mappings` | Original appointment ID → anonymized appointment ID |
| `phi_staging` | PHI data staging per patient (JSON) |

**`failed_rows.db`** — Per-source-schema audit tables:
| Table | Purpose |
|-------|---------|
| `failed_rows_<schema>` | Rows filtered by `InvalidRowHandler` (e.g., unresolved patient IDs) — full row JSON + reason for audit |

**`qc_results.db`** — Quality control output (written by `deid qc` / `qc-delta` / `qc-audit`):
| Table | Purpose |
|-------|---------|
| `qc_table_results` | Part 1 — per-table pass/fail with per-column detector results |
| `qc_part2_results` | Part 2 — one row per mapping/count check (status, expected, actual, delta) |
| `qc_delta_identity_results` | Part 2 — per-table row-level diff (missing/extra/value-mismatch) |
| `qc_unstructured_audit_results` | Part 3 — per-table notes audit + quarantine list (`failure_detail`) |

All four databases use SQLite WAL mode and `PRAGMA busy_timeout=5000` to allow concurrent worker reads/writes without spurious `SQLITE_BUSY` errors. The state engine is created once per worker process via `deid.models.base.get_cached_state_engine(db_path)` — every Celery task on the hot batch update path reuses the same cached engine instead of creating and disposing one per call.

### 3-Stage Pipeline & Batch Processing

Every table is processed via the same 3-stage Celery pipeline (`fetch → process → write`), regardless of size. During the **setup** phase the orchestrator:

1. Calls `NDDBHandler.get_exact_row_count(table_name)` (`SELECT COUNT(*)` — exact, not catalog estimate) for each configured table.
2. Splits the row range into `deidentification.batch_size` chunks and creates a `BatchState` row per chunk in `state.db`.
3. Cleans up stale staging files.

During **deidentify**, each `fetch_batch` keyset-paginates a chunk from the source DB, writes it to an Arrow IPC file, then dispatches `process_batch`, which applies de-identification rules and dispatches `write_batch`. Each `fetch_batch` also self-chains by atomically claiming the next pending batch for the same table — guaranteeing keyset pagination order. Per-table write workers run with `concurrency=1` to avoid MySQL lock-wait timeouts from concurrent inserts on the same table.

If a worker crashes mid-task, `task_acks_late=True` causes Celery to re-deliver the message. Idempotency guards at the top of each task's inner function check `BatchState.status` and skip the work (or re-dispatch the next stage) when the batch is already past the current stage. Non-retry failures reset the batch to `pending` and the orchestrator's watchdog re-dispatches it within 2 seconds.

### Mapping Joins & Patient Identifier Resolution

The heart of `process_batch` is turning raw source IDs into de-identified ones. This happens in
`deid/core/process_df/main.py` (`JoinMapping`, `PatientIdentifierResolver`) and the equivalent
preloaded-mapping path in `deid/tasks/process.py`.

1. **Categorise columns** — `get_key_phi_column_list()` returns a 5-tuple:
   `(encounter_id_cols, patient_id_cols_by_rule, reference_pid_cols, appointment_id_cols, chart_id_cols)`.
   Patient-ID columns are grouped as a dict `{rule_name: [columns]}`, supporting **dynamic
   `PATIENT_*` rules** (e.g. `PATIENT_PATIENTID`, `PATIENT_CHARTID`) in addition to the legacy
   `PATIENT_ID`. The rule-name suffix selects which identifier column of the patient mapping
   table to join on.

2. **Join mapping tables** — each secondary mapping (encounter, appointment, chart) is enriched
   with the patient mapping via `nd_patient_id`, then left-joined into the batch. Every PATIENT_*
   **source column** is joined to the patient mapping independently.

3. **Resolve canonical columns** — `PatientIdentifierResolver.transform()` builds:
   - `_resolved_offset` — coalesced per-patient date offset (falls back to `date_offset_days`).
   - `_resolved_nd_patient_id` — the row-level de-identified patient ID (coalesce priority:
     referencepid > encounter > patient groups > appointment > chart). Used by
     `InvalidRowHandler` (null ⇒ row rejected to `failed_rows.db`), `NotesRule` (PII lookup key),
     and as the fallback replacement value.
   - `_resolved_ndpid_col_{col}` — a **per-column** de-identified value for each patient-ID
     column, taken from that column's own join.
   - `_resolved_{identifier}` — the coalesced raw identifier value per project identifier, used
     by `NotesRule` to find-and-replace identifiers embedded in free-text notes.

4. **Apply rules** — `DeIdentifier` runs `NOTES` rules first (before any column is overwritten),
   then structured rules. `PatientIDRule` writes each column's `_resolved_ndpid_col_{col}` when
   present, falling back to `_resolved_nd_patient_id`.

**Multiple distinct patients per row.** Some tables carry two patient-ID columns that reference
*different* patients in the same row — e.g. a `mergelogs` table with `FromID` and `ToID`. Because
each patient-ID column is joined and resolved independently (`_resolved_ndpid_col_{col}`), each is
de-identified to its own value rather than both collapsing to a single `_resolved_nd_patient_id`.
The first column of a rule keeps the identifier-keyed join suffix (`from_{identifier_col}_mapping`)
for backward compatibility; additional columns use a per-column suffix (`from_col_{col}_mapping`)
so their joins never collide. See `docs/mapping_join_flow.md` for the step-by-step reference.

### Progress Monitoring

Workers publish progress events to Redis channel `deid:progress`:
```json
{"table": "patients", "status": "started", "detail": ""}
{"table": "patients", "status": "completed", "detail": ""}
{"table": "patients", "status": "failed", "detail": "Connection refused"}
```

The orchestrator listens asynchronously and updates `state.db` in real time. Use `deid status` to query the current state.

---

## De-Identification Rules

Rules are assigned per-column in the `tables` section of `config.yaml`. The `DeIdentifier` applies NLP-based notes rules first (before column values are overwritten), then all structured rules.

| Rule | Effect | Example |
|------|--------|---------|
| `PATIENT_ID` | Replace with anonymized ID from patient mapping (joins on `patient_id`) | `PAT001` → `10000042` |
| `PATIENT_*` (dynamic, e.g. `PATIENT_PATIENTID`, `PATIENT_CHARTID`) | Same as `PATIENT_ID`, but the rule-name suffix selects which patient-mapping identifier column to join on | column-specific |
| `ENCOUNTER_ID` | Replace with anonymized encounter ID | `ENC789` → `20000015` |
| `REFERENCE_PID` | Replace reference patient ID via mapping (indirect / reference-mapping columns) | indirect patient ID |
| `APPOINTMENT_ID` | Replace with anonymized appointment ID | `APT456` → `30000008` |
| `CHART_ID` | Replace with anonymized chart ID via chart mapping | `CHT321` → `40000005` |
| `CHART_ID` | Replace with anonymized chart ID (joins via chart mapping) | `CHT321` → `40000012` |
| `MASK` | Replace with fixed placeholder | `John Smith` → `<<PATIENT_NAME>>` |
| `DATE_OFFSET` | Shift date by per-patient offset (days) | `2024-03-15` → `2024-04-18` |
| `STATIC_OFFSET` | Shift date by global fixed offset | `2024-03-15` → `2024-04-18` |
| `ZIP_CODE` | Truncate to 3 digits or mask | `90210` → `902` |
| `PATIENT_DOB` | Replace with birth year only (`Int64`); columns with no recognised date pattern are nulled to prevent PHI leakage | `1985-06-15` → `1985` |
| `NOTES` | PII-master lookup + regex masking | Free-text clinical notes |
| `GENERIC_NOTES` | Regex-based PII masking | Free-text with pattern-based replacement |
| `DOB` / `PATIENT_DOB` | Replace with birth year only (`Int64`); columns with no recognised date pattern are nulled to prevent PHI leakage | `1985-06-15` → `1985` |
| `NOTES` | NLP-based PII extraction and masking | Free-text clinical notes (Presidio + Spacy) |
| `GENERIC_NOTES` | Regex/Spacy/Presidio generic PHI masking | Free-text with pattern-based replacement |

> **Multiple patient-ID columns per row.** When a table has two patient-ID columns that reference
> *different* patients in the same row (e.g. `mergelogs` `FromID`/`ToID`), assign each the
> appropriate `PATIENT_*` rule. Each column is joined and resolved independently, so each is
> de-identified to its own value rather than both collapsing to one. See
> [Architecture → Mapping Joins & Patient Identifier Resolution](#mapping-joins--patient-identifier-resolution).

### Mask Values

When using the `MASK` rule, the mask value is derived from the column name. Common mappings:

| Column Pattern | Mask Value |
|---------------|------------|
| `*name*`, `*first*`, `*last*` | `<<PATIENT_NAME>>` |
| `*phone*`, `*fax*` | `<<PHONE_NUMBER>>` |
| `*email*` | `<<EMAIL_ADDRESS>>` |
| `*address*`, `*street*` | `<<ADDRESS>>` |
| `*ssn*`, `*social*` | `<<SSN>>` |

### Date Offset

The `DATE_OFFSET` rule shifts dates by a per-patient offset value. Each patient is assigned a consistent random offset (stored in `mappings.db`), so all dates for the same patient shift by the same number of days. This preserves temporal relationships within a patient's records while preventing re-identification.

The `STATIC_OFFSET` rule shifts all dates by the global `date_offset_days` value from config (default: 34 days).

### Unstructured Text (Clinical Notes)

The `NOTES` rule de-identifies free text by combining:

1. **PII master lookup** — the patient's known PHI values (names, addresses, contact info, identifiers) from the PHI master/staging table are located in the note and replaced with the de-identified surrogate.
2. **Regex patterns** — dates, phone numbers, emails, and other common PII formats, plus structured-tag/XML handling.

The `GENERIC_NOTES` rule applies regex-based masking of dates, phone numbers, emails, addresses, and other common PII formats.

> Note: the de-identification engine does **not** use Presidio (it was removed project-wide). QC's residual-PII scan uses the `regex`/`mlx` backend described under [Quality Control](#quality-control).

---

## Quality Control

QC implements a three-part framework spanning the whole de-identification lifecycle. Each part runs
at a different time, has a different scope, and a different failure action.

| Part | When | Scope | Command | Blocking? |
|------|------|-------|---------|-----------|
| **1 — Structured column checks** | after de-id (QC phase) | de-identified columns by rule category | `deid qc` (or `deid run --phase qc`) | per-column pass/fail |
| **2 — Mapping & count checks** | **before** the pipeline | mapping tables, entity counts, row-level identity | inside `deid run`; row-level via `deid qc-delta` | **yes — halts the run** |
| **3 — Unstructured data audit** | after the pipeline | de-identified notes vs PHI master | `deid qc-audit` | post-hoc quarantine |

### Part 1 — Structured column checks (`deid qc`)

Samples rows from source and destination (size scales with table size, 300–5000) and verifies each
de-identified column with a rule-specific detector ([deid/qc/builders/structured.py](deid/qc/builders/structured.py)):

- **Patient / Encounter / Reference / Appointment / Chart ID** — length + prefix of the anonymized ID (offending rows captured in remarks).
- **Mask** — value equals the expected `<<mask_value>>`.
- **Date offset** — the shift matches the patient's assigned offset, plus format (`YYYY-MM-DD`) and plausibility (`[1900, today]`) checks.
- **ZIP** — truncated to exactly 3 chars (or null).
- **DOB** — 4-digit birth year.
- **Notes / generic notes** — routed to the unstructured scan: a master exact-match (wire `qc.pii_master_conn_str` to feed real PHI values) plus a pluggable **residual-PII scanner** — `regex` by default (dependency-free, portable), or `mlx` (a local LLM via `mlx-lm`, Apple Silicon only) for stronger name/entity recall. Presidio has been removed from QC.

Plus a row-count check (source == dest + ignored). Results persist per-table to `qc_results.db`.

### Part 2 — Mapping & count checks (blocking pre-run gate)

Runs **before** any de-identification when `qc.part2` is configured; a failing blocking check halts
`deid run` (exit 1) so bad mappings never reach the pipeline ([deid/qc/mapping_count.py](deid/qc/mapping_count.py)):

- `mapping_to_table_count` — mapping table row count == target table row count.
- `patient_encounter_count` / `encounter_row_count` — per-patient / per-encounter count distributions match source↔dest.
- `mapping_uniqueness` — each `patient_id`→one `nd_patient_id`; each `encounter_id`→one `nd_encounter_id`.
- `offset_range` — patient offset within `[-38, 38]` (configurable).
- `mapping_id_format` — nd-id length/prefix on the mapping tables.

Set `qc.part2_blocking: false` to run these as warnings without halting.

**Delta-identity QC** (`deid qc-delta`, [deid/qc/delta_identity.py](deid/qc/delta_identity.py)) is the row-level
member of Part 2 — a polars source↔dest diff that classifies every key as `missing_in_dest`,
`extra_in_dest`, or `value_mismatch`, scoped to a delta window (`nd_extracted_date`). It runs on
demand and, opt-in (`DEID_CDC_DELTA_QC=1`), automatically after a CDC merge.

```bash
deid qc-delta --config config.yaml                       # uses qc.delta_identity
deid qc-delta --config config.yaml --tables t1,t2 --delta-after 2026-05-15
```

### Part 3 — Unstructured data audit (`deid qc-audit`)

Post-pipeline, **master-referenced** audit of clinical notes ([deid/qc/master_phi.py](deid/qc/master_phi.py)) —
it references the PHI master directly rather than trusting pipeline logic. For each sampled record it
scans the note text for: any raw PHI value (case-insensitive; names matched on parts), and residual
phone / 5-digit ZIP / URL / facility-name patterns; optionally asserts the surrogate id is present;
and flags NULL/empty notes as coverage gaps ([deid/qc/coverage.py](deid/qc/coverage.py)). Failed records are
recorded as a **quarantine list** for remediation before release.

```bash
deid qc-audit --config config.yaml            # audits qc.master_phi.tables
deid qc-audit --config config.yaml --report   # also print the consolidated report
```

### Consolidated report

[deid/qc/report.py](deid/qc/report.py) (`build_audit_report` / `render_markdown`) reads `qc_results.db` and
produces the framework's Audit Output across all three parts (totals, pass/fail, coverage gaps, and
per-part failure detail). `deid qc-audit --report` prints it.

### QC Output

Each table receives a pass/fail result with per-column breakdowns:

```
Table: patients
  Status: PASS
  Sample size: 1000
  Source rows: 50000 | Dest rows: 49998 | Ignored: 2
  Columns:
    patient_id:  passed=1000, failed=0
    first_name:  passed=1000, failed=0
    visit_date:  passed=998,  failed=2
```

Failed columns include detailed remarks identifying the specific rows and values that did not pass verification.

---

## CDC (Change Data Capture)

The `deid cdc` command handles incremental data feeds for databases that receive ongoing updates after initial de-identification.

```bash
# MySQL CDC
deid cdc --config cdc_config.yaml --db-type mysql

# MSSQL CDC
deid cdc --config cdc_config.yaml --db-type mssql
```

CDC utilities parse database change logs, identify new/modified rows, and apply de-identification rules to only the changed data.

---

## Development

### Standalone Scripts

Each setup command is also available as a standalone script (no `deid` CLI required):

```bash
python scripts/generate_config_csv.py --config config.yaml --output rules.csv
python scripts/populate_mappings.py --config config.yaml
python scripts/populate_pii_table.py --config config.yaml
```

These call the same core logic as their CLI counterparts.

### Running Tests

```bash
python -m pytest tests/ -v
```

### Test Structure

| Test File | What It Verifies |
|-----------|-----------------|
| `test_config.py` | Config loading, validation, env var interpolation |
| `test_models.py` | SQLAlchemy model creation, insert/query, mapping helpers |
| `test_batch_state.py` | `BatchState` CRUD, unique constraints, status transitions |
| `test_batch_utils.py` | Shared batch-state helper functions (claim/advance/reset) |
| `test_staging.py` | Arrow IPC staging directory helpers, crash-recovery reconciliation |
| `test_ipc_cache.py` | IPC cache streaming (`stream_from_ipc_cache`) |
| `test_celery_tasks.py` | Celery app creation, task registration |
| `test_task_models.py` | Pydantic task/orchestrator/QC boundary models |
| `test_fetch_task.py` | `fetch_batch` task — keyset pagination, status updates, self-chaining |
| `test_process_task.py` | `process_batch` task — Arrow read/write, mapping joins, preloaded vs SQL-fallback paths |
| `test_process_main.py` | `get_key_phi_column_list`, `PatientIdentifierResolver` (incl. per-column resolution), `PatientIDRule` |
| `test_mappings.py` | Mapping-table join helpers |
| `test_write_task.py` | `write_batch` task — idempotent DELETE+INSERT, status updates |
| `test_pipeline_integration.py` | End-to-end fetch → process → write chain with real Arrow IPC files |
| `test_orchestrator.py` | `_setup_phase` — exact row count, BatchState creation |
| `test_orchestrator_extended.py` | RunLog status, credential stripping, dispatch helpers |
| `test_run_batching.py` | Table batch splitting, worker dispatch, unmatched-table handling |
| `test_retry_logic.py` | `deid retry` — picks up pending/dispatched/failed batches, skips done |
| `test_single_identifier_bypass.py` | Single-identifier fast path (PATIENT_ID → PATIENT_PATIENTID remap) |
| `test_table_overrides.py` | Per-table config overrides |
| `test_cli.py` | CLI help output, command registration, error handling for missing files |
| `test_mapping_populator.py` | Mapping population: rule scanning, bulk inserts, idempotency |
| `test_clinical_bin_doc/` | ClinicalBin document extractor + imports |
| `test_migration.py` | Schema/state migration behavior |
| `test_core_imports.py` | Zero Django/legacy imports in core engine |
| `test_qc_imports.py` | Zero Django/legacy imports in QC package |
| `test_log_publisher.py`, `test_log_collector.py`, `test_logging_integration.py` | Redis pub/sub log aggregation and run-summary file output |
| `test_integration.py` | End-to-end wiring: config → state DB → Celery tasks |

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| CLI | Typer | Command-line interface |
| Task Queue | Celery + Redis | Distributed task execution (prefork pool) |
| State Storage | SQLAlchemy 2.0 + SQLite | Run tracking, ID mappings |
| Config | Pydantic v2 + PyYAML | Validation, env var interpolation |
| DataFrames | Polars | High-performance columnar processing |
| Regex | `regex` (PyPI), with stdlib `re` fallback | High-performance regex engine. Note: `google-re2` is deliberately avoided — its Python bindings have ~50× overhead due to string marshalling. |
| NLP (de-id) | regex + PII-master lookup | PII detection/replacement in unstructured text during de-identification (Presidio removed) |
| QC residual-PII | `auto` → regex / `mlx-lm` | Residual-PII scan in QC notes; `auto` picks `mlx` (local LLM) on Apple Silicon when installed, else `regex`. `pip install -e '.[mlx]'` on Macs. |
| Databases | SQLAlchemy | MySQL, MSSQL, PostgreSQL, Snowflake |
