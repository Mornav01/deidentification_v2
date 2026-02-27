# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A CLI-based platform for de-identifying healthcare databases (removing PII/PHI). It reads a YAML config, dispatches de-identification tasks via Celery/Redis to prefork workers, tracks state in SQLite, and runs QC scanning after completion.

## Commands

```bash
# Run the full pipeline (setup → deidentify → QC)
deid run --config config.yaml

# Run a single phase
deid run --config config.yaml --phase deidentify

# Check run status
deid status --state-db ./state.db

# Run CDC processing
deid cdc --config cdc_config.yaml --db-type mysql

# Decrypt clinical notes
deid decrypt-notes --input ./encrypted --output ./decrypted

# Start Celery worker manually (normally managed by `deid run`)
celery -A deid.tasks.celery_app:get_celery_app() worker --pool=prefork --concurrency=4

# Run tests
python -m pytest tests/ -v
```

Python environment: `/Users/shubham/miniconda3/envs/deid/bin/python`
Package installer: `uv pip install --python /Users/shubham/miniconda3/envs/deid/bin/python <package>`

## Configuration

All settings are in a single YAML file (`config.yaml`). Environment variables can be interpolated with `${VAR_NAME}` syntax.

Key config sections: `source_db`, `destination_db`, `state_db_path`, `mappings_db_path`, `redis_url`, `deidentification` (batch_size, date_offset_days, parallel_tasks_per_table, large_table_threshold), `tables` (name + rules per column), `phases`, `workers`, `qc`.

See `deid/config/schema.py` for the full Pydantic schema.

## Architecture

### Data Flow
```
deid run --config config.yaml
  → Typer CLI (deid/cli/run.py)
    → create_celery_app() + spawn worker subprocess
    → asyncio orchestrator (deid/orchestrator/async_runner.py)
      → Phase: setup — discover tables, persist TableState to SQLite
      → Phase: deidentify — build Celery Canvas task graph, dispatch
        → Celery worker (prefork pool) runs deidentify_table / deidentify_table_range
          → start_de_identification_for_table() [deid/core/process_df/main.py]
            → Stream source data via SQLAlchemy (deid/core/dbPkg/dbhandler.py)
            → Join with patient/encounter mappings (deid/core/ops_df/jointables.py)
            → Apply DeIdentifier rules (deid/core/process_df/base.py + rules.py)
            → Handle clinical notes NLP (deid/core/process_df/unstruct/)
            → Write to destination DB
        → Redis pub/sub progress events → orchestrator updates TableState
      → Phase: qc — dispatch QC scanner tasks for completed tables
    → Update RunLog status in state.db
```

### Key Modules

**`deid/config/`** — Configuration
- `schema.py` — Pydantic v2 models: `DeidConfig`, `DbConfig`, `TableConfig`, `WorkerSettings`, etc.
- `loader.py` — YAML loading with `${ENV_VAR}` interpolation
- `table_schemas.py` — TypedDicts for table/column config (used by core engine)

**`deid/core/process_df/`** — De-identification engine (sync, Polars-based)
- `main.py` — Orchestrates full table de-identification: streaming batches, joining mappings, applying rules, writing to destination via background thread
- `base.py` — `DeIdentifier` class; applies rules to Polars DataFrames
- `rules.py` — Rule classes: `PatientIDRule`, `EncounterIDRule`, `MaskRule`, `DateOffsetRule`, `ZIPCodeRule`, `NotesRule`, etc.
- `unstruct/` — NLP-based PII extraction from free-text clinical notes (Presidio + Spacy)

**`deid/core/dbPkg/`** — Database layer (SQLAlchemy)
- `dbhandler.py` — `NDDBHandler`; streaming reads, batch inserts, schema mapping
- `mapping_loader.py` / `pii_loader.py` — Load mapping and PII staging tables

**`deid/models/`** — SQLAlchemy 2.0 models (SQLite)
- `base.py` — Engine factories with WAL mode, `create_all_state_tables()`, `create_all_mappings_tables()`
- `state.py` — `DbConfig`, `TableState`, `RunLog` (state.db)
- `mappings.py` — `PatientMapping`, `EncounterMapping`, `AppointmentMapping`, `PhiStaging` (mappings.db)

**`deid/tasks/`** — Celery task definitions
- `celery_app.py` — App factory with `include` for task autodiscovery
- `deidentify.py` — `deidentify_table`, `deidentify_table_range` (shared_task)
- `qc.py` — `run_qc` (shared_task)
- `stats.py` — `generate_table_stats` (shared_task)

**`deid/orchestrator/`** — Async pipeline orchestrator
- `async_runner.py` — Drives setup → deidentify → QC phases
- `task_graph.py` — Builds Celery Canvas groups; splits large tables by ID ranges
- `progress.py` — Async Redis pub/sub listener for progress events

**`deid/cli/`** — Typer CLI commands
- `app.py` — Main entry point with auto-registered commands
- `run.py` — `deid run` — loads config, spawns worker, runs orchestrator
- `status.py` — `deid status` — reads state.db, displays progress
- `cdc.py` — `deid cdc` — Change Data Capture wrapper
- `decrypt_notes.py` — `deid decrypt-notes` — Clinical notes decryption

**`deid/qc/`** — Quality control scanning
- `scanner.py` — `DbScanner`; samples source/dest data, runs detectors
- `builders/` — Detector classes: `SPatientIdDetector`, `SMaskDetector`, `SDateOffestDetector`, `UnstructuredDetector`, etc.

**`deid/cdc/`** — Change Data Capture utilities (MySQL and MSSQL)

### De-Identification Rules

Rules are configured per-column in `config.yaml` tables section. The `DeIdentifier` applies them in order:

| Rule | Effect |
|------|--------|
| `PATIENT_ID` | Replace with anonymized ID from mapping table |
| `ENCOUNTER_ID` | Replace encounter ID from mapping table |
| `REFERENCE_PID` | Replace reference patient ID column |
| `APPOINTMENT_ID` | Replace appointment ID |
| `MASK` | Replace with fixed placeholder, e.g. `<<PATIENT_NAME>>` |
| `DATE_OFFSET` | Shift dates by configured offset days |
| `STATIC_OFFSET` | Shift dates by a fixed global offset |
| `ZIP_CODE` | Replace with `<<ZIP_CODE>>` |
| `PATIENT_DOB` | Replace with `<<PATIENT_DOB>>` |
| `NOTES` / `GENERIC_NOTES` | NLP extraction + masking of free text |

### Technology Stack
- **Typer** — CLI framework
- **Celery + Redis** — Task queue (prefork pool, Canvas for DAG)
- **SQLAlchemy 2.0 + SQLite** — State tracking (state.db) and mappings (mappings.db)
- **Pydantic v2** — Config validation with env var interpolation
- **Polars** — Primary DataFrame library (not Pandas)
- **google-re2** — Regex engine (prevents catastrophic backtracking)
- **Presidio + Spacy** — NLP-based PII detection in unstructured text
- Source databases: MySQL, MSSQL, PostgreSQL, Snowflake via SQLAlchemy
