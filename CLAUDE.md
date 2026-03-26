# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A CLI-based platform for de-identifying healthcare databases (removing PII/PHI). It reads a YAML config, dispatches de-identification tasks via Celery/Redis to prefork workers in a 3-stage pipeline (fetch → process → write), tracks state in SQLite, persists failed rows per source schema, and runs QC scanning after completion.

## Commands

```bash
# Run the full pipeline (setup → deidentify)
deid run --config config.yaml

# Run a single phase
deid run --config config.yaml --phase deidentify

# Clean-slate rerun (drops dest tables, state, staging, failed rows for configured tables)
deid run --config config.yaml --rerun

# Rerun only specific tables listed in a CSV
deid run --config config.yaml --rerun --tables-csv tables_to_run.csv

# Run QC as a standalone step (after deidentification)
deid qc --config config.yaml
deid qc --config config.yaml --table specific_table_name

# Check run status
deid status --state-db ./state.db

# Run CDC processing
deid cdc --config cdc_config.yaml --db-type mysql

# Decrypt clinical notes
deid decrypt-notes --input ./encrypted --output ./decrypted

# Start Celery worker manually (normally managed by `deid run`)
celery -A deid.tasks.celery_app worker --pool=prefork --concurrency=4

# Run tests
python -m pytest tests/ -v
```

Python environment: `/Users/shubham/miniconda3/envs/deid/bin/python`
Package installer: `uv pip install --python /Users/shubham/miniconda3/envs/deid/bin/python <package>`

## Configuration

All settings are in a single YAML file (`config.yaml`). Environment variables can be interpolated with `${VAR_NAME}` syntax.

Key config sections: `source_db`, `destination_db`, `state_db_path`, `mappings_db_path` (or `mappings_db` for remote MySQL), `failed_rows_db_path`, `qc_results_db_path`, `redis_url`, `deidentification` (batch_size, date_offset_days), `tables` (name + rules per column), `tables_to_run` / `tables_to_run_csv`, `phases`, `workers`, `qc` (sample_size, task_timeout), `pii_db`, `pii_config` / `pii_config_path`, `secondary_pii_configs`.

See `deid/config/schema.py` for the full Pydantic schema.

## Architecture

### Data Flow (3-stage pipeline)
```
deid run --config config.yaml [--rerun] [--tables-csv tables.csv]
  → Typer CLI (deid/cli/run.py)
    → [--rerun] cleanup: drop dest tables, delete state.db, purge Redis queues,
      delete per-schema failed rows, remove .deid_staging/
    → create_celery_app() + spawn worker subprocesses (fetch, process, per-table write)
    → asyncio orchestrator (deid/orchestrator/async_runner.py)
      → Phase: setup — discover tables, persist TableState + BatchState to SQLite
      → Phase: deidentify — dispatch 3-stage Celery task chain per batch:
          Stage 1: fetch_batch (deid/tasks/fetch.py)
            → Keyset-paginated SELECT from source DB
            → Write Arrow IPC file with column schema metadata
            → Self-chain: dispatch next fetch for this table
            → Dispatch process_batch for this batch
          Stage 2: process_batch (deid/tasks/process.py)
            → Read Arrow IPC, join mappings (preloaded or per-batch SQL)
            → PatientIdentifierResolver coalesces mapping columns
            → InvalidRowHandler filters unresolved rows → per-schema failed_rows table
            → DeIdentifier.apply_rules() (rules.py + unstruct/)
            → Write processed Arrow IPC
            → Dispatch write_batch for this batch
          Stage 3: write_batch (deid/tasks/write.py)
            → Read processed Arrow IPC
            → Create dest table with PHI type overrides + MySQL row-limit adjustment
            → Idempotent DELETE + INSERT in single transaction
            → Mark batch done; check table completion
        → Redis pub/sub progress events → orchestrator polls BatchState
      → Phase: qc — dispatch QC scanner tasks for completed tables (config-filtered)
        → Results persisted to qc_results.db
    → Update RunLog status in state.db
```

### Key Modules

**`deid/config/`** — Configuration
- `schema.py` — Pydantic v2 models: `DeidConfig`, `DbConfig`, `TableConfig`, `WorkerSettings`, `QCSettings`, etc.
- `loader.py` — YAML loading with `${ENV_VAR}` interpolation
- `table_schemas.py` — TypedDicts for table/column config (used by core engine)
- `task_models.py` — Pydantic models for Celery task payloads: `FetchTaskConfig`, `ProcessTaskConfig`, `WriteTaskConfig`, `QCTaskConfig`

**`deid/core/process_df/`** — De-identification engine (sync, Polars-based)
- `main.py` — Orchestrates full table de-identification: streaming batches, joining mappings, applying rules, writing to destination via background thread (legacy single-task path)
- `base.py` — `DeIdentifier` class; applies rules to Polars DataFrames
- `rules.py` — Rule classes: `PatientIDRule`, `EncounterIDRule`, `MaskRule`, `DateOffsetRule`, `ZIPCodeRule`, etc. All ID rules cast to `Int64` to prevent Float64 decimals after joins.
- `columns_type_detector.py` — Maps de-identification rules to destination column types (BIGINT, DATETIME, etc.)
- `rowhandler.py` — `InvalidRowHandler`; filters rows with unresolved IDs, writes to per-schema failed_rows table
- `unstruct/` — Regex-based PII masking in free-text clinical notes:
  - `notes.py` — `NotesRule`: key-PHI replacement, PII table masking (patient names, DOB, insurance), secondary PII
  - `genericnotes.py` — `GenericNotesRule`: phone, URL, IP, date shifting, driver's license patterns
  - `xml.py` / `xml_utils.py` — XML tag-based PHI masking

**`deid/core/dbPkg/`** — Database layer (SQLAlchemy)
- `dbhandler.py` — `NDDBHandler`; streaming reads, batch inserts, schema mapping, dest table creation with MySQL strict mode disabled
- `mapping_loader.py` — Load mapping tables (supports SQLite and MySQL mappings DB)
- `pii_loader.py` — Load PII staging tables

**`deid/models/`** — SQLAlchemy 2.0 models
- `base.py` — Engine factories with WAL mode for state.db, mappings.db, failed_rows.db, qc_results.db
- `state.py` — `DbConfig`, `TableState`, `RunLog`, `BatchState` (state.db)
- `mappings.py` — `PatientMapping`, `EncounterMapping`, `AppointmentMapping`, `PhiStaging` (mappings.db)
- `failed_rows.py` — Per-schema `failed_rows_{schema_name}` tables (failed_rows.db); `ensure_schema_table()`, `get_schema_table_name()`
- `qc_results.py` — `QCTableResult` (qc_results.db); written incrementally per table

**`deid/tasks/`** — Celery task definitions (3-stage pipeline)
- `celery_app.py` — App factory; preloads mapping tables into memory on process workers
- `fetch.py` — `fetch_batch`: keyset-paginated source reads → Arrow IPC; self-chains next fetch
- `process.py` — `process_batch`: Arrow IPC → mapping joins → de-identification → Arrow IPC
- `write.py` — `write_batch`: Arrow IPC → idempotent dest INSERT with PHI type overrides; MySQL row-limit adjustment
- `qc.py` — `run_qc`: dispatches `DbScanner`, persists results to qc_results.db
- `deidentify.py` — Legacy `deidentify_table` / `deidentify_table_range` (single-task path)

**`deid/orchestrator/`** — Async pipeline orchestrator
- `async_runner.py` — Drives setup → deidentify → QC phases; builds task configs; polls BatchState
- `log_collector.py` — Async Redis pub/sub log aggregation and summary
- `task_graph.py` — Legacy Celery Canvas graph builder
- `progress.py` — Async Redis pub/sub listener for progress events

**`deid/cli/`** — Typer CLI commands
- `app.py` — Main entry point with auto-registered commands
- `run.py` — `deid run` — loads config, `--rerun` cleanup (dest tables, state, staging, failed rows, Redis queues), `--tables-csv` filter, spawns workers, runs orchestrator
- `qc.py` — `deid qc` — standalone QC scanning with config-filtered tables, dedicated timeout
- `status.py` — `deid status` — reads state.db, displays progress
- `retry.py` — `deid retry` — re-dispatch failed batches
- `cdc.py` — `deid cdc` — Change Data Capture wrapper
- `decrypt_notes.py` — `deid decrypt-notes` — Clinical notes decryption
- `generate_config.py` — `deid generate-config` — auto-generate config from source DB

**`deid/qc/`** — Quality control scanning
- `scanner.py` — `DbScanner`; samples source/dest data, runs detectors with per-step timing logs
- `generator.py` — `DataGenerator`; ID-range-based random sampling (no ORDER BY RAND)
- `builders/` — Detector classes: `SPatientIdDetector`, `SMaskDetector`, `SDateOffestDetector`, `UnstructuredDetector` (null-safe), etc.

**`deid/staging.py`** — Arrow IPC staging directory helpers, reconciliation after crashes

**`deid/cdc/`** — Change Data Capture utilities (MySQL and MSSQL)

### De-Identification Rules

Rules are configured per-column in `config.yaml` tables section. The `DeIdentifier` applies them in order:

| Rule | Effect | Dest Type |
|------|--------|-----------|
| `PATIENT_ID` | Replace with anonymized ID from mapping table | BIGINT |
| `ENCOUNTER_ID` | Replace encounter ID from mapping table | BIGINT |
| `REFERENCE_PID` | Replace reference patient ID column | BIGINT |
| `APPOINTMENT_ID` | Replace appointment ID | BIGINT |
| `MASK` | Replace with fixed placeholder, e.g. `<<PATIENT_NAME>>` | VARCHAR(len) |
| `DATE_OFFSET` | Shift dates by per-patient offset days | DATETIME |
| `STATIC_OFFSET` | Shift dates by a fixed global offset | DATETIME |
| `ZIP_CODE` | Replace with `<<ZIP_CODE>>` | VARCHAR(50) |
| `PATIENT_DOB` | Replace with year only | INTEGER |
| `NOTES` / `GENERIC_NOTES` | Regex-based masking of free text (PII, dates, phones, etc.) | LONGTEXT |

### Technology Stack
- **Typer** — CLI framework
- **Celery + Redis** — Task queue (prefork pool, 3-stage pipeline: fetch → process → write)
- **SQLAlchemy 2.0 + SQLite** — State tracking (state.db), failed rows (failed_rows.db), QC results (qc_results.db)
- **SQLAlchemy 2.0 + MySQL** — Mappings DB (mappings.db, supports both SQLite and MySQL)
- **Pydantic v2** — Config validation with env var interpolation
- **Polars** — Primary DataFrame library (not Pandas)
- **regex** — Regex engine (Python `regex` module; fallback to stdlib `re`)
- Source databases: MySQL, MSSQL, PostgreSQL, Snowflake via SQLAlchemy

### Important Implementation Notes
- **Regex imports**: All files use `import regex as re` with fallback to stdlib `re`. Do NOT use `google-re2` — its Python bindings have 50x overhead due to string marshalling.
- **ID column types**: All ID replacement rules (`PatientIDRule`, `EncounterIDRule`, etc.) cast to `Int64` before aliasing to prevent Float64 decimals from Polars left-join null promotion.
- **PII data caching**: `NotesRule` tracks `_known_patient_ids` and re-fetches PII data when new patient IDs appear in subsequent batches.
- **MySQL dest table creation**: `SET sql_mode = ''` and `SET innodb_strict_mode = 0` are issued on the same connection as CREATE TABLE. Large row sizes are handled by auto-converting VARCHAR to LONGTEXT.
- **Failed rows**: Written to per-source-schema tables (`failed_rows_{schema_name}`) in failed_rows.db. `--rerun` only deletes rows for the tables being rerun.
- **QC results**: Written incrementally to qc_results.db as each table scan completes.
