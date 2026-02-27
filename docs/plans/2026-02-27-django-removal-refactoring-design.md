# Design: Remove Django & Modernize Orchestration

**Date:** 2026-02-27
**Status:** Approved

## Goals

1. Remove all Django dependencies
2. Replace PostgreSQL task queue with Celery + Redis
3. Use Celery prefork pool for worker process management
4. Improve regex performance (enforce re2 + Polars native regex)
5. Single-command execution via Typer CLI
6. Remove Jupyter notebook dependency (replace with YAML config)
7. Optimize for throughput and memory usage

## Non-Goals

- Changing core de-identification logic (rules, NLP pipeline, streaming architecture)
- Changing source/destination database support (MySQL, MSSQL, PostgreSQL, Snowflake)
- Adding new de-identification rules or features
- Distributed multi-machine deployment

---

## Architecture

```
CLI (Typer)
  deid run --config config.yaml
  |
  v
Async Orchestrator (asyncio)
  - Table discovery (concurrent per source DB)
  - Task graph construction (Celery Canvas)
  - Progress monitoring (async Redis pub/sub)
  - State persistence (SQLite)
  |
  v
Redis (broker + result backend + pub/sub)
  |
  v
Celery Workers (prefork pool, N processes)
  - Each worker calls sync core engine directly
  - Publishes progress events to Redis
  |
  v
Core Engine (unchanged, sync)
  - core/process_df/  DeIdentifier, rules, NLP
  - core/dbPkg/       NDDBHandler (SQLAlchemy streaming)
  - core/ops_df/      Reference mapping joins
  - qc/               QC scanning
  |
  v
Source DBs <-> Destination DBs
SQLite: state.db + mappings.db
```

**Key architectural decisions:**
- **Async for orchestration only.** The CLI runs an asyncio event loop for concurrent I/O: table discovery, task dispatch, progress monitoring. CPU-bound de-identification runs synchronously in Celery worker processes.
- **Celery Canvas replaces custom Chain/Task DAG.** `group()` for parallel tables, `chord()` for fan-out large-table ranges then merge, `chain()` for sequential dependencies.
- **Redis serves triple duty:** Celery broker, result backend, progress pub/sub channel.
- **SQLite for durable state.** Two files: `state.db` (run tracking, table status) and `mappings.db` (patient/encounter ID mappings, portable across runs).
- **Single-command execution.** `deid run` spawns the Celery worker pool as a child process. Redis is an external prerequisite.

---

## Package Structure

```
deid/
  __init__.py
  __main__.py                # python -m deid entry point
  cli/
    __init__.py
    app.py                   # Typer app with subcommands
    run.py                   # deid run
    status.py                # deid status
    cdc.py                   # deid cdc
    decrypt_notes.py         # deid decrypt-notes
  config/
    __init__.py
    schema.py                # Pydantic models for config.yaml
    loader.py                # YAML loading + validation + env var interpolation
  models/
    __init__.py
    base.py                  # SQLAlchemy declarative base, engine factory
    state.py                 # DbConfig, TableState, RunLog (state.db)
    mappings.py              # PatientMapping, EncounterMapping, etc. (mappings.db)
  orchestrator/
    __init__.py
    async_runner.py          # Async orchestrator: discovery, dispatch, monitor
    task_graph.py            # Builds Celery Canvas from config
    progress.py              # Redis pub/sub progress listener
  tasks/
    __init__.py
    celery_app.py            # Celery app configuration
    deidentify.py            # Celery task: deidentify_table / deidentify_table_range
    qc.py                    # Celery task: run_qc
    stats.py                 # Celery task: generate_stats
  core/                      # Migrated from deIdentification/core/ (minimal changes)
    process_df/
      main.py                # Refactored: accepts config dict, not Django model
      base.py                # DeIdentifier (unchanged logic)
      rules.py               # Rules (offset_days as param, not settings)
      config.py              # Static PII column mappings (unchanged)
      constants.py           # Regex patterns (unchanged)
      columns_type_detector.py
      rowhandler.py
      unstruct/
        notes.py             # NLP pipeline (accepts config dicts)
        genericnotes.py
        xml.py
        xml_utils.py
        utils.py
    dbPkg/
      dbhandler.py           # NDDBHandler (already Django-independent)
      mapping_loader.py
      pii_loader.py
      phi_table/
      mapping_table/
      schemas.py
    ops_df/
      jointables.py          # ReferenceMappingDataFrameJoiner (unchanged)
      utility.py
  qc/                        # Migrated from qc_package/
    __init__.py
    generator.py
    scanner.py
    builders/
  cdc/                       # Migrated from CDC/
    mysql.py
    mssql.py
  clinical_bin_doc/          # Migrated from ClinicalBinDoc/
    decryptor.py
```

---

## Configuration (config.yaml)

```yaml
# Databases
source_db:
  type: mysql                       # mysql | mssql | postgresql | snowflake
  host: localhost
  port: 3306
  database: source_ehr
  username: reader
  password: ${SOURCE_DB_PASSWORD}   # Env var interpolation

destination_db:
  type: postgresql
  host: localhost
  port: 5432
  database: deid_output
  username: writer
  password: ${DEST_DB_PASSWORD}

# State & Mappings
state_db_path: ./state.db
mappings_db_path: ./mappings.db

# Redis
redis_url: redis://localhost:6379/0

# De-identification settings
deidentification:
  batch_size: 100000
  date_offset_days: 34
  patient_id_prefix: 10000000
  parallel_tasks_per_table: 4
  large_table_threshold: 500000

# Tables & Rules (inline or CSV reference)
tables:
  - name: patients
    rules:
      patient_id: PATIENT_ID
      patient_name: MASK
      date_of_birth: PATIENT_DOB
      zip_code: ZIP_CODE
      clinical_notes: NOTES
  - name: encounters
    rules:
      encounter_id: ENCOUNTER_ID
      patient_id: PATIENT_ID
      visit_date: DATE_OFFSET

# Alternative: CSV config file (existing format)
# rules_csv: ./rules_config.csv

# Mapping tables
mapping_tables:
  patient:
    source_column: patient_id
    destination_column: nd_patient_id
  encounter:
    source_column: encounter_id
    destination_column: nd_encounter_id
    reference: patient

# Phases to execute
phases:
  - setup
  - deidentify
  - qc

# Worker settings
workers:
  concurrency: 4
  max_retries: 1
  task_timeout: 3600

# QC settings
qc:
  sample_size: 100
  scan_for_residual_pii: true
```

**Pydantic validation** ensures all required fields are present, types are correct, and env vars are interpolated before any work begins.

---

## Data Models

### state.db (SQLAlchemy + SQLite)

```
db_configs
  id              INTEGER PK
  name            TEXT UNIQUE
  source_conn_str TEXT
  dest_conn_str   TEXT
  run_config      JSON
  created_at      DATETIME
  updated_at      DATETIME

table_states
  id              INTEGER PK
  db_config_id    INTEGER FK -> db_configs
  table_name      TEXT
  status          TEXT (pending|processing|completed|failed)
  row_count       INTEGER NULL
  rules_config    JSON
  failure_remarks TEXT NULL
  qc_status       TEXT NULL
  qc_result       JSON NULL
  started_at      DATETIME NULL
  completed_at    DATETIME NULL
  created_at      DATETIME
  updated_at      DATETIME
  UNIQUE(table_name, db_config_id)

run_logs
  id              INTEGER PK
  config_hash     TEXT
  phases          JSON
  status          TEXT (running|completed|failed)
  started_at      DATETIME
  completed_at    DATETIME NULL
  stats           JSON NULL
```

### mappings.db (SQLAlchemy + SQLite, separate file)

```
patient_mappings
  id              INTEGER PK
  patient_id      TEXT UNIQUE
  nd_patient_id   INTEGER
  date_offset     INTEGER
  created_at      DATETIME
  updated_at      DATETIME

encounter_mappings
  id              INTEGER PK
  encounter_id    TEXT UNIQUE
  nd_encounter_id INTEGER
  patient_mapping_id INTEGER FK -> patient_mappings
  created_at      DATETIME
  updated_at      DATETIME

appointment_mappings
  id              INTEGER PK
  appointment_id  TEXT UNIQUE
  nd_appointment_id INTEGER
  patient_mapping_id INTEGER FK -> patient_mappings
  created_at      DATETIME
  updated_at      DATETIME

phi_staging
  id              INTEGER PK
  patient_id      TEXT
  phi_details     JSON
  created_at      DATETIME
  updated_at      DATETIME
```

**Concurrency note:** SQLite WAL mode for concurrent reads. Mappings are pre-populated in the setup phase (batch insert all patient/encounter IDs from source). Workers only read during deidentify phase — no write contention.

---

## Async Orchestrator

The orchestrator runs in the CLI process using `asyncio.run()`:

1. **Setup phase (async I/O):**
   - Concurrent table discovery across source DBs
   - Concurrent destination schema creation
   - Batch-insert mapping IDs from source into mappings.db
   - Persist table states to state.db

2. **Deidentify phase (async dispatch + monitoring):**
   - Build Celery Canvas task graph:
     - Small tables: `group(deidentify_table.s(config) for each table)`
     - Large tables: `chord(group(range_tasks), merge_results.s())`
   - Dispatch to Celery via `.apply_async()`
   - Monitor progress via async Redis pub/sub loop
   - Update state.db as events arrive

3. **QC phase (async dispatch + monitoring):**
   - Dispatch QC tasks as Celery group
   - Collect results, update state.db

---

## Celery Task Design

### Task: deidentify_table

```python
@celery_app.task(bind=True, max_retries=config.workers.max_retries, acks_late=True)
def deidentify_table(self, table_config: dict):
    publish_progress(table_config["name"], "started")
    try:
        start_de_identification_for_table(
            table_config=table_config,
            source_conn_str=table_config["source_connection_str"],
            dest_conn_str=table_config["dest_connection_str"],
            mappings_db_path=table_config["mappings_db_path"],
            batch_size=table_config["batch_size"],
            offset_days=table_config["offset_days"],
        )
        publish_progress(table_config["name"], "completed")
    except Exception as exc:
        publish_progress(table_config["name"], "failed", str(exc))
        raise self.retry(exc=exc)
```

### Task: deidentify_table_range

Same as above but accepts `start_id` and `end_id` for range-based parallel processing.

### Task: run_qc

Wraps existing QC logic. Accepts table config dict, runs scanner and generator.

### Worker startup

The CLI spawns Celery workers as a subprocess:
```python
subprocess.Popen([
    "celery", "-A", "deid.tasks.celery_app", "worker",
    "--pool=prefork", f"--concurrency={config.workers.concurrency}",
    "--loglevel=info"
])
```

---

## Core Engine Refactoring

**Scope:** Remove Django imports, accept config dicts instead of Django model objects. No logic changes.

### main.py signature change

```python
# Before: start_de_identification_for_table(table_id: int, ...)
# After:
def start_de_identification_for_table(
    table_config: dict,
    source_conn_str: str,
    dest_conn_str: str,
    mappings_db_path: str,
    batch_size: int,
    offset_days: int,
    pii_config: dict | None = None,
    ...
):
```

### base.py change

```python
# Before: DeIdentifier.__init__(self, df, config, db_details_obj: DbDetailsModel, ...)
# After:  DeIdentifier.__init__(self, df, config, pii_config: dict | None, ...)
```

### rules.py change

```python
# Before: settings.DEFAULT_OFFSET_VALUE
# After:  self.offset_days (injected via constructor)
```

### unstruct/notes.py change

```python
# Before: receives DbDetailsModel
# After:  receives pii_config: dict and pii_db_conn_str: str
```

### dbhandler.py, jointables.py — No changes

Already Django-independent (pure SQLAlchemy/Polars).

---

## Regex Optimization

| Use Case | Engine | Notes |
|---|---|---|
| Column-level masking (ZIP, phone, dates) | Polars `.str.replace()` | Vectorized Rust regex, zero Python overhead |
| Clinical notes NLP (Presidio entities) | google-re2 (hard dependency) | Linear-time guarantee, no backtracking |
| Presidio custom recognizers | google-re2 | Pattern matching in free text |
| Constants/patterns in constants.py | Compiled re2 at module load | One-time compile, reused across all calls |

- `google-re2` becomes a hard dependency (remove `try/except` fallback to `re`)
- All regex patterns compiled once at module level
- DataFrame column operations use Polars native `.str.replace()` / `.str.contains()` instead of Python-level regex loops

---

## CLI Interface

```
deid run --config config.yaml                          # Full pipeline
deid run --config config.yaml --phase deidentify       # Single phase override
deid status --state-db ./state.db                      # Check progress
deid cdc --config cdc_config.yaml                      # CDC utilities
deid decrypt-notes --input dir/ --output dir/          # ClinicalBinDoc
```

**`deid run` lifecycle:**
1. Validate config.yaml (Pydantic)
2. Initialize state.db and mappings.db
3. Spawn Celery worker subprocess
4. Run async orchestrator (phases from config)
5. Print summary on completion
6. Exit 0 (success) or 1 (failure)

---

## Error Handling

| Failure | Behavior |
|---|---|
| Config validation | Pydantic error, exit immediately |
| Source DB unreachable | Fail fast in setup phase |
| Celery task failure | Retry once (configurable), mark table failed |
| Worker crash | Celery redelivers (acks_late), retry limit |
| Redis down | CLI detects, exits with error |
| SQLite contention | WAL mode, single writer (orchestrator) |
| NLP/Presidio failure | Caught per-row, logged, row skipped |
| Ctrl+C | Revoke pending tasks, update state.db, kill workers |

---

## Dependencies

### Removed
- Django, djangorestframework, django_extensions, django-cors-headers
- apache-airflow-* (unused)
- jupyter_server

### Added
- celery[redis]
- typer
- aioredis (async Redis for progress monitoring)
- pyyaml
- pydantic-settings

### Kept
- SQLAlchemy, polars, google-re2, spacy, presidio-*, psycopg2-binary, PyMySQL, pyodbc, pymssql, snowflake-*, pycryptodome, lxml, pyahocorasick, faker, rapidfuzz, dateparser, beautifulsoup4, tqdm, jsonpickle, redis, numpy, pandas

---

## Testing Strategy

- **Core engine unit tests:** Rules, DeIdentifier, NDDBHandler. Already Django-independent.
- **Celery task tests:** `CELERY_ALWAYS_EAGER=True` for sync execution in tests.
- **Orchestrator tests:** Mock Celery dispatch, test task graph construction, mock Redis pub/sub.
- **Config tests:** Pydantic validation — valid configs, invalid configs, env var interpolation.
- **Integration tests:** Full `deid run` with small SQLite source DB, verify output.
