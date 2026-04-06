# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A CLI-based platform for de-identifying healthcare databases (removing PII/PHI). It reads a YAML config, dispatches de-identification tasks via Celery/Redis to prefork workers in a 3-stage pipeline (fetch → process → write), tracks state in SQLite, persists failed rows per source schema, and runs QC scanning after completion.

## Commands

```bash
# Run the full pipeline (setup → deidentify)
deid run --config config.yaml

# Base config + task-specific overlay (overlay overrides/appends)
deid run --config base.yaml --overlay task.yaml

# Run a single phase
deid run --config config.yaml --phase deidentify

# Clean-slate rerun (drops dest tables, state, staging, failed rows for configured tables only)
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

Settings are in YAML files. A base config can be overlaid with a task-specific config using `--overlay` / `-o` (deep-merged: overlay keys win, nested dicts merged recursively, lists replaced). Environment variables can be interpolated with `${VAR_NAME}` syntax.

Key config sections: `source_db`, `destination_db`, `join_db` (optional, defaults to `source_db`), `state_db_path`, `mappings_db_path` (or `mappings_db` for remote MySQL), `failed_rows_db_path`, `qc_results_db_path`, `redis_url`, `deidentification` (batch_size, date_offset_days), `tables` (name + rules per column), `tables_to_run` / `tables_to_run_csv`, `phases`, `workers`, `qc` (sample_size, task_timeout), `pii_db`, `pii_config` / `pii_config_path`, `secondary_pii_configs`.

See `deid/config/schema.py` for the full Pydantic schema.

## Architecture

### Data Flow (3-stage pipeline)
```
deid run --config base.yaml [--overlay task.yaml] [--rerun] [--tables-csv tables.csv]
  → Typer CLI (deid/cli/run.py)
    → load_config(base.yaml, overlay_path=task.yaml)  — deep-merge overlay onto base
    → [--tables-csv] filter: tables in CSV without config rules are logged as errors
      and recorded in state.db as failed (status="failed", failure_remarks set);
      remaining matched tables proceed normally
    → [--rerun] cleanup (table-scoped): drop dest tables, clear state/batch rows,
      delete per-schema failed rows, remove per-table staging, purge write queues
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
            → Read Arrow IPC
            → Reference mapping joins via join_db (or source DB if join_db not configured)
            → Join mappings (preloaded or per-batch SQL)
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

**`deid/core/ops_df/`** — DataFrame operations
- `jointables.py` — `ReferenceMappingDataFrameJoiner`: multi-hop reference table joins via Polars; uses `join_db` (separate DB for reference lookups) when configured, falls back to source DB; chunks IN clauses (1000-value limit) for SQL Server compatibility; auto-casts mismatched join-key types (Int64 → Utf8 fallback)
- `utility.py` — `join_dataframes`, `DistinctValueFetcher`

**`deid/core/dbPkg/`** — Database layer (SQLAlchemy)
- `dbhandler.py` — `NDDBHandler`; streaming reads, batch inserts, schema mapping, dest table creation with MySQL strict mode disabled
- `mapping_loader.py` — Load mapping tables (supports SQLite and MySQL mappings DB)
- `pii_loader.py` — Load PII staging tables

**`deid/models/`** — SQLAlchemy 2.0 models
- `base.py` — Engine factories with WAL mode + `busy_timeout=5000` for state.db, mappings.db, failed_rows.db, qc_results.db. Also exports `get_cached_state_engine(db_path)` — a per-process cached state engine used by all Celery tasks to avoid the overhead of create/dispose cycles on every batch status update.
- `state.py` — `DbConfig`, `TableState`, `RunLog`, `BatchState` (state.db)
- `mappings.py` — `PatientMapping`, `EncounterMapping`, `AppointmentMapping`, `PhiStaging` (mappings.db)
- `failed_rows.py` — Per-schema `failed_rows_{schema_name}` tables (failed_rows.db); `ensure_schema_table()`, `get_schema_table_name()`
- `qc_results.py` — `QCTableResult` (qc_results.db); written incrementally per table

**`deid/tasks/`** — Celery task definitions (3-stage pipeline)
- `celery_app.py` — App factory; preloads mapping tables into memory on process workers
- `fetch.py` — `fetch_batch`: keyset-paginated source reads → Arrow IPC; self-chains next fetch. Idempotency guard at the top of `_fetch_batch_inner` skips re-fetching when re-delivered past `"dispatched"`; if re-delivered with status `"fetched"`, re-dispatches `process_batch` to recover from a lost dispatch. On failure, resets the batch to `"pending"` so the watchdog can re-dispatch.
- `process.py` — `process_batch`: Arrow IPC → mapping joins → de-identification → Arrow IPC. Idempotency guard skips already-processed batches. On failure, resets batch to `"pending"`.
- `write.py` — `write_batch`: Arrow IPC → idempotent dest INSERT with PHI type overrides; MySQL row-limit adjustment. Idempotency guard skips already-done batches. `_created_dest_tables` module-level set guards against re-issuing `CREATE TABLE IF NOT EXISTS` DDL on every batch. On non-lock-wait failure, resets batch to `"pending"`.
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
- **Config overlay**: `load_config(base, overlay_path=task)` deep-merges two YAML files. Overlay keys win on conflict; nested dicts are merged recursively; lists are replaced entirely.
- **Regex imports**: All files use `import regex as re` with fallback to stdlib `re`. Do NOT use `google-re2` — its Python bindings have 50x overhead due to string marshalling.
- **ID column types**: All ID replacement rules (`PatientIDRule`, `EncounterIDRule`, etc.) cast to `Int64` before aliasing to prevent Float64 decimals from Polars left-join null promotion.
- **PII data caching**: `NotesRule` tracks `_known_patient_ids` and re-fetches PII data when new patient IDs appear in subsequent batches.
- **MySQL dest table creation**: Uses raw DDL (not `Table.create()`) with `_sqlalchemy_type_to_mysql_ddl` for type mapping + `_adjust_ddl_for_mysql_row_limit` for auto VARCHAR→LONGTEXT conversion. `SET sql_mode = ''` and `SET innodb_strict_mode = 0` are issued on the same connection as CREATE TABLE. MSSQL→MySQL type mapping is in `deid/core/dbPkg/type_mapping.py`.
- **--rerun is table-scoped**: Only affects the tables in the current config — drops their dest tables, clears their state/batch rows, deletes their failed rows, removes their staging dirs, purges their write queues. Other tables' data is untouched.
- **--tables-csv unmatched tables**: Tables listed in the CSV that have no de-identification rules in the config are logged as errors and recorded in state.db as `TableState` rows with `status="failed"` and `failure_remarks` explaining the issue. The pipeline continues with the matched tables.
- **join_db**: Optional `DbConfig` in the YAML config (`join_db` section, same shape as `source_db`). When set, `ReferenceMappingDataFrameJoiner` uses it for reference-table lookups instead of the source DB. Defaults to source DB when omitted.
- **Failed rows**: Written to per-source-schema tables (`failed_rows_{schema_name}`) in failed_rows.db. `--rerun` only deletes rows for the tables being rerun.
- **QC results**: Written incrementally to qc_results.db as each table scan completes. QC is standalone (`deid qc`), not part of `deid run`.
- **State engine caching**: All Celery tasks use `get_cached_state_engine(db_path)` from `deid/models/base.py` instead of creating and disposing an engine on every batch status update. The cache is a module-level dict keyed by `db_path`. With prefork workers, each child process gets its own empty cache at fork time, populated lazily — so there are no fork-safety concerns with stale connections. `PRAGMA busy_timeout=5000` lets concurrent workers wait up to 5 seconds for the SQLite write lock instead of raising `SQLITE_BUSY` immediately.
- **Task idempotency**: Because `task_acks_late=True`, a killed Celery worker causes the task to be re-delivered. Each task's `_*_inner()` function checks `BatchState.status` at the top and skips (or re-dispatches the next stage) if the batch is already past the current stage. This is the guard against duplicate fetches/processing/writes on re-delivery.
- **Fault tolerance**: On task failure (non-retry path), each task resets the batch to `"pending"` so `_deidentify_phase`'s watchdog (see `async_runner.py`) can re-dispatch it. Transient failures self-heal; permanent failures burn time until `stuck_timeout = workers.task_timeout * 2` kicks in.
- **Exact row counts for batch creation**: `_setup_phase` calls `NDDBHandler.get_exact_row_count(table_name)` (a `SELECT COUNT(*)`) rather than `get_rows_count()` (which uses catalog estimates that can be off by 40–50% on InnoDB). Under-counted batches would silently lose tail rows because the self-chaining fetch can only claim existing `BatchState` rows.
- **Path traversal validation**: `deid/staging.py` validates that `batch_fetched_path` and `batch_processed_path` resolve within the staging root, rejecting `../` escapes from untrusted table names or `config_key` values.
- **No PHI in logs**: Do NOT log raw values from clinical notes, patient names, DOBs, or built-from-PII regex patterns. Diagnostic logs in `notes.py` log only counts (`len(patterns)`), never the pattern contents.
- **Sanitized error output**: Celery task `except` blocks publish `f"{type(exc).__name__}: {exc}"` to Redis — never `traceback.format_exc()` — because Polars error messages can contain raw cell values from PHI columns.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **deidentification_v2** (1961 symbols, 4869 relationships, 164 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/deidentification_v2/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/deidentification_v2/context` | Codebase overview, check index freshness |
| `gitnexus://repo/deidentification_v2/clusters` | All functional areas |
| `gitnexus://repo/deidentification_v2/processes` | All execution flows |
| `gitnexus://repo/deidentification_v2/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## Keeping the Index Fresh

After committing code changes, the GitNexus index becomes stale. Re-run analyze to update it:

```bash
npx gitnexus analyze
```

If the index previously included embeddings, preserve them by adding `--embeddings`:

```bash
npx gitnexus analyze --embeddings
```

To check whether embeddings exist, inspect `.gitnexus/meta.json` — the `stats.embeddings` field shows the count (0 means no embeddings). **Running analyze without `--embeddings` will delete any previously generated embeddings.**

> Claude Code users: A PostToolUse hook handles this automatically after `git commit` and `git merge`.

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
