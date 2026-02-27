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
| `POST /upload-config/` | `tables:` section in config.yaml |
| `POST /start-deidentification/` | `deid run --config config.yaml` |
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
# All of the above is now a single command:
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

# 4. Run the full pipeline
deid run --config config.yaml
```

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
```

This registers the `deid` CLI command via the entry point defined in `pyproject.toml`.

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
  batch_size: 100000                  # Rows processed per batch (default: 100000)
  date_offset_days: 34                # Days to shift dates (default: 34)
  patient_id_prefix: 10000000         # Prefix for anonymized patient IDs
  parallel_tasks_per_table: 4         # Number of parallel splits for large tables
  large_table_threshold: 500000       # Row count threshold for parallel splitting

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

# Alternative: load table/column rules from a CSV file
# rules_csv: ./rules.csv

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
  concurrency: 4                      # Number of Celery prefork worker processes
  max_retries: 1                      # Task retry limit on failure
  task_timeout: 3600                  # Max seconds per task (default: 1 hour)

# ── Quality Control ─────────────────────────────────────────────────
qc:
  sample_size: 100                    # Rows to sample for QC verification
  scan_for_residual_pii: true         # Run NLP-based residual PII scan on notes
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

### Run the Full Pipeline

```bash
deid run --config config.yaml
```

This executes three phases in order:

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
├── cli/                    # Typer CLI commands
│   ├── app.py              #   Main entry point, command registration
│   ├── run.py              #   deid run — orchestrate full pipeline
│   ├── status.py           #   deid status — display run progress
│   ├── cdc.py              #   deid cdc — change data capture
│   └── decrypt_notes.py    #   deid decrypt-notes
│
├── config/                 # Configuration layer
│   ├── schema.py           #   Pydantic v2 models (DeidConfig, DbConfig, etc.)
│   ├── loader.py           #   YAML loading + ${ENV_VAR} interpolation
│   └── table_schemas.py    #   TypedDicts for runtime table/column config
│
├── models/                 # SQLAlchemy 2.0 models (SQLite)
│   ├── base.py             #   Engine factories, create_all helpers
│   ├── state.py            #   DbConfig, TableState, RunLog
│   └── mappings.py         #   PatientMapping, EncounterMapping, PhiStaging
│
├── orchestrator/           # Async pipeline orchestrator
│   ├── async_runner.py     #   Drives setup → deidentify → QC phases
│   ├── task_graph.py       #   Builds Celery Canvas groups (single/parallel)
│   └── progress.py         #   Redis pub/sub listener for progress events
│
├── tasks/                  # Celery task definitions
│   ├── celery_app.py       #   App factory (broker, serialization, pooling)
│   ├── deidentify.py       #   deidentify_table, deidentify_table_range
│   ├── qc.py               #   run_qc
│   └── stats.py            #   generate_table_stats
│
├── core/                   # Core de-identification engine
│   ├── logger.py           #   Logging setup
│   ├── process_df/         #   De-identification logic
│   │   ├── main.py         #     Batch orchestrator (stream → join → rules → write)
│   │   ├── base.py         #     DeIdentifier class, rule dispatcher
│   │   ├── rules.py        #     10+ rule classes (PatientID, Mask, DateOffset, etc.)
│   │   ├── config.py       #     Column-name → mask-value mappings
│   │   ├── constants.py    #     Regex patterns (dates, ZIP codes)
│   │   ├── rowhandler.py   #     InvalidRowHandler (filter/log null patient IDs)
│   │   └── unstruct/       #     Unstructured text (clinical notes)
│   │       ├── notes.py    #       NotesRule — NLP-based PII extraction (Presidio + Spacy)
│   │       └── genericnotes.py #   GenericNotesRule — regex-based masking
│   ├── dbPkg/              #   Database abstraction layer
│   │   ├── dbhandler.py    #     NDDBHandler (streaming reads, batch inserts, schema ops)
│   │   ├── mapping_loader.py #   Load patient/encounter mapping tables
│   │   └── pii_loader.py   #    Load PII staging tables
│   └── ops_df/             #   DataFrame operations
│       └── jointables.py   #     Multi-hop reference table joining
│
├── qc/                     # Quality control scanning
│   ├── scanner.py          #   DbScanner — samples source/dest and runs detectors
│   ├── generator.py        #   DataGenerator — stratified sampling from databases
│   ├── schema.py           #   Output schemas (ColumnQCResult, FinalQCResult)
│   └── builders/           #   QC detector classes
│       ├── base.py         #     Abstract Detector interface
│       ├── structured.py   #     ID, mask, date, ZIP, DOB detectors
│       └── unstructured.py #     Residual PII scanner (Presidio)
│
└── cdc/                    # Change Data Capture utilities
    ├── mysql/              #   MySQL CDC parsers
    └── mssql/              #   MSSQL CDC parsers
```

### State Management

The platform uses two SQLite databases for persistent state:

**`state.db`** — Run tracking and table progress:
| Table | Purpose |
|-------|---------|
| `db_configs` | Source/destination connection strings |
| `table_states` | Per-table status (pending/started/completed/failed), row counts, rules config, QC results |
| `run_logs` | Overall run tracking with config hash, phases, timing |

**`mappings.db`** — ID mapping tables:
| Table | Purpose |
|-------|---------|
| `patient_mappings` | Original patient ID → anonymized patient ID + date offset |
| `encounter_mappings` | Original encounter ID → anonymized encounter ID |
| `appointment_mappings` | Original appointment ID → anonymized appointment ID |
| `phi_staging` | PHI data staging per patient (JSON) |

Both databases use SQLite WAL mode for concurrent read/write access.

### Large Table Parallelism

When a table exceeds `large_table_threshold` (default: 500,000 rows), the orchestrator:

1. Queries the min/max `nd_auto_increment_id` from the source table
2. Splits the ID range into `parallel_tasks_per_table` equal segments
3. Dispatches a `deidentify_table_range(start_id, end_id)` task for each segment
4. Celery executes these in parallel across prefork worker processes

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
| `PATIENT_ID` | Replace with anonymized ID from mapping table | `PAT001` → `10000042` |
| `ENCOUNTER_ID` | Replace with anonymized encounter ID | `ENC789` → `20000015` |
| `REFERENCE_PID` | Replace reference patient ID via mapping | Indirect patient ID columns |
| `APPOINTMENT_ID` | Replace with anonymized appointment ID | `APT456` → `30000008` |
| `MASK` | Replace with fixed placeholder | `John Smith` → `<<PATIENT_NAME>>` |
| `DATE_OFFSET` | Shift date by per-patient offset (days) | `2024-03-15` → `2024-04-18` |
| `STATIC_OFFSET` | Shift date by global fixed offset | `2024-03-15` → `2024-04-18` |
| `ZIP_CODE` | Truncate to 3 digits or mask | `90210` → `902` |
| `PATIENT_DOB` | Replace with birth year only | `1985-06-15` → `1985` |
| `NOTES` | NLP-based PII extraction and masking | Free-text clinical notes (Presidio + Spacy) |
| `GENERIC_NOTES` | Regex-based PII masking | Free-text with pattern-based replacement |

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

The `NOTES` rule uses a two-stage NLP pipeline:

1. **Presidio + Spacy** (`en_core_web_lg`) — Detects entities: person names, phone numbers, email addresses, dates, locations, medical record numbers
2. **PII table lookup** — Cross-references detected entities against the patient's known PII data (names, addresses, etc.) from the PHI staging table

Detected PII is replaced with typed placeholders (e.g., `<<PERSON>>`, `<<PHONE_NUMBER>>`).

The `GENERIC_NOTES` rule uses regex patterns for faster but less precise masking of dates, phone numbers, emails, addresses, and other common PII formats.

---

## Quality Control

After de-identification, the QC phase automatically verifies each table:

### Verification Process

1. **Sampling** — Stratified random sampling from both source and destination tables. Sample size scales with table size (300-5000 rows).

2. **Structured Column Checks** — Each de-identified column is verified by a specialized detector:
   - **Patient/Encounter ID** — Verifies correct length and prefix of anonymized IDs
   - **Mask** — Confirms values match the expected `<<mask_value>>` pattern
   - **Date Offset** — Validates the date shift matches the patient's assigned offset
   - **ZIP Code** — Confirms truncation to 3 or fewer digits
   - **DOB** — Verifies replacement with 4-digit birth year

3. **Unstructured Column Checks** — Runs Presidio NLP on de-identified text to detect any residual PII that was not masked.

4. **Row Count Verification** — Confirms source row count equals destination rows plus any intentionally ignored rows.

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

### Running Tests

```bash
python -m pytest tests/ -v
```

### Test Structure

| Test File | What It Verifies |
|-----------|-----------------|
| `test_config.py` | Config loading, validation, env var interpolation |
| `test_models.py` | SQLAlchemy model creation, insert/query, mapping helpers |
| `test_celery_tasks.py` | Celery app creation, task registration |
| `test_orchestrator.py` | Task graph building (single table, parallel splits) |
| `test_cli.py` | CLI help output, error handling for missing files |
| `test_core_imports.py` | Zero Django/legacy imports in core engine |
| `test_qc_imports.py` | Zero Django/legacy imports in QC package |
| `test_integration.py` | End-to-end wiring: config → state DB → Celery tasks |

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| CLI | Typer | Command-line interface |
| Task Queue | Celery + Redis | Distributed task execution (prefork pool) |
| State Storage | SQLAlchemy 2.0 + SQLite | Run tracking, ID mappings |
| Config | Pydantic v2 + PyYAML | Validation, env var interpolation |
| DataFrames | Polars | High-performance columnar processing |
| Regex | google-re2 | Safe regex (no catastrophic backtracking) |
| NLP | Presidio + Spacy | PII detection in unstructured text |
| Databases | SQLAlchemy | MySQL, MSSQL, PostgreSQL, Snowflake |
