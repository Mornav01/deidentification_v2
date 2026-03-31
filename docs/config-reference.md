# Configuration Reference

This document describes every field in `config.yaml`. Configuration can be split into a **base config** (shared machine-level settings) and a **task-specific overlay** that overrides or extends the base.

---

## Config Overlay

Use `--overlay` / `-o` to layer a task-specific config on top of a base config:

```bash
deid run --config base.yaml --overlay task.yaml
```

The two YAML files are **deep-merged**:
- Overlay keys override base keys
- Nested dicts are merged recursively (e.g. `source_db.database` can be overridden without repeating `source_db.host`)
- Lists are replaced entirely (e.g. `tables` in the overlay replaces the base `tables`)
- New keys in the overlay are appended

**Example — base.yaml** (shared across all tasks on this machine):

```yaml
source_db:
  type: mysql
  host: db-source.internal
  port: 3306
  database: hospital_db
  username: ${SOURCE_DB_USER}
  password: ${SOURCE_DB_PASSWORD}

destination_db:
  type: mysql
  host: db-dest.internal
  port: 3306
  database: hospital_db_deid
  username: ${DEST_DB_USER}
  password: ${DEST_DB_PASSWORD}

redis_url: redis://localhost:6379/0
mappings_db_path: ./mappings.db
workers:
  fetchers: 2
  processors: 16
```

**Example — task.yaml** (specific to this run):

```yaml
source_db:
  database: clinic_db          # overrides just the database name

state_db_path: ./clinic_state.db
failed_rows_db_path: ./clinic_failed.db
tables_to_run_csv: ./clinic_tables.csv
deidentification:
  batch_size: 5000             # overrides batch_size; date_offset_days kept from base
```

The effective config is the deep merge of both: `clinic_db` as source database, `db-source.internal` as host (from base), `5000` as batch_size (from overlay), etc.

---

## Environment Variable Interpolation

Any string value can reference environment variables using `${VAR_NAME}` syntax. The loader resolves these at parse time (after merging, if an overlay is used) before validation.

```yaml
source_db:
  password: ${SOURCE_DB_PASSWORD}
```

If the referenced variable is not set, loading fails with an error naming the missing variable.

---

## `source_db` (required)

Connection details for the **source** database containing the original (identified) data. Read-only access is sufficient.

```yaml
source_db:
  type: mysql          # mysql | mssql | postgresql | snowflake
  host: localhost
  port: 3306
  database: hospital_db
  username: ${SOURCE_DB_USER}
  password: ${SOURCE_DB_PASSWORD}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | `mysql` \| `mssql` \| `postgresql` \| `snowflake` | Database engine. Determines the SQLAlchemy driver used (`mysql+pymysql`, `mssql+pymssql`, `postgresql+psycopg2`, `snowflake`). |
| `host` | string | Hostname or IP address of the database server. |
| `port` | integer | Port number. |
| `database` | string | Database/schema name to connect to. |
| `username` | string | Login username. |
| `password` | string | Login password. Use `${ENV_VAR}` to avoid storing secrets in the file. |

The platform constructs a SQLAlchemy connection string from these fields:
`driver://username:password@host:port/database`

---

## `destination_db` (required)

Connection details for the **destination** database where de-identified data is written. Requires read/write access.

Same structure as `source_db`.

```yaml
destination_db:
  type: postgresql
  host: localhost
  port: 5432
  database: hospital_db_deid
  username: ${DEST_DB_USER}
  password: ${DEST_DB_PASSWORD}
```

---

## `config_key` (optional)

A namespace identifier that isolates all pipeline state for this config — state DB rows, batch records, staging file paths, Redis write queues, and failed-row audit entries are all scoped to this key.

```yaml
config_key: historical
```

**Default:** `default`

**Allowed characters:** letters, digits, underscores, and hyphens (`[a-zA-Z0-9_-]`).

Use `config_key` when multiple configs run tables with the **same name** against the same `state.db`. Without it, two configs running a `patients` table would share state entries and collide. Common values:

| Value | Typical use |
|-------|-------------|
| `historical` | Full historical backfill |
| `incremental` | Periodic incremental loads |
| `adhoc` | One-off table reruns |

The key affects:
- **`state.db`** — `TableState` and `BatchState` rows have a `config_key` column; uniqueness constraints include it.
- **Staging paths** — Arrow IPC files are stored under `staging_root/<config_key>/<table>/…` instead of `staging_root/<table>/…`.
- **Redis write queues** — named `deid-write-<config_key>-<table>` instead of `deid-write-<table>`.
- **`failed_rows.db`** — each per-schema table has a `config_key` column; `--rerun` only deletes rows matching the current key.

Check status for a specific key:

```bash
deid status --state-db ./state.db --config-key historical
```

---

## `rules_csv` (optional)

Path to a CSV file that defines de-identification rules per table and column.

```yaml
rules_csv: ./rules.csv
```

The CSV has four columns:

```csv
table_name,column_name,data_type,rule
patients,patient_id,INTEGER,PATIENT_ID
patients,first_name,VARCHAR(100),MASK
patients,date_of_birth,DATE,PATIENT_DOB
encounters,diagnosis,VARCHAR(500),
```

- Rows with a non-empty `rule` column become active de-identification rules.
- Rows with an empty `rule` are copied as-is (no transformation).
- If the file does not exist at load time, it is auto-generated by introspecting the source database.

Generate this file with:

```bash
deid generate-config --config config.yaml
```

**Either `rules_csv` or `tables` must be provided.** If both are provided, `tables` takes precedence. If only `rules_csv` is provided, it is parsed into `tables` at load time.

---

## `tables` (optional)

Inline table/column rule definitions. Alternative to `rules_csv` for smaller configs or when you want everything in one file.

```yaml
tables:
  - name: patients
    rules:
      patient_id: PATIENT_ID
      first_name: MASK
      last_name: MASK
      date_of_birth: PATIENT_DOB
      ssn: MASK
      admission_date: DATE_OFFSET
      clinical_notes: GENERIC_NOTES

  - name: encounters
    rules:
      encounter_id: ENCOUNTER_ID
      patient_id: PATIENT_ID
      admit_date: DATE_OFFSET
```

Each entry:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Table name in the source database. |
| `rules` | dict | Map of `column_name` → `rule_name`. Only columns listed here are transformed; all other columns are copied as-is. |

### Available Rules

| Rule | Effect | Requires |
|------|--------|----------|
| `PATIENT_ID` | Replace with anonymized patient ID from mapping table. | Mapping DB populated (`deid mapping`). |
| `ENCOUNTER_ID` | Replace with anonymized encounter ID from mapping table. | Mapping DB populated. |
| `APPOINTMENT_ID` | Replace with anonymized appointment ID from mapping table. | Mapping DB populated. |
| `REFERENCE_PID` | Replace a secondary patient ID column (e.g., referring_patient_id) with the anonymized ID. Same mapping as `PATIENT_ID`. | Mapping DB populated. |
| `MASK` | Replace value with a fixed placeholder string, e.g., `<<PATIENT_NAME>>`. The placeholder is derived from the column name. | None. |
| `DATE_OFFSET` | Shift dates by a per-patient random offset (from mapping table). Handles datetime strings, date-only strings, and embedded dates in text. | Mapping DB populated (uses per-patient offset). |
| `STATIC_OFFSET` | Shift dates by a fixed global offset (`date_offset_days`). Unlike `DATE_OFFSET`, does not vary per patient. | None. |
| `PATIENT_DOB` | Extract the birth year only. The full date of birth is replaced with just the 4-digit year. | None. |
| `ZIP_CODE` | Truncate to first 3 digits (US ZIP codes). Prevents re-identification from geographic data. | None. |
| `NOTES` | Full PII masking for clinical free-text. Looks up each patient's actual PII values (names, SSN, DOB, etc.) from the PII table and replaces exact matches in their notes. Also applies generic regex patterns for phones, dates, IPs, URLs. | PII tables populated (`deid pii-table`), `pii_config_path` set. |
| `GENERIC_NOTES` | Regex-only PII masking for free-text. Applies generic patterns (phones, dates, IPs, URLs, driver's licenses) without patient-specific name/DOB matching. Does not require PII tables. | None. |

---

## `deidentification` (optional)

Global settings for the de-identification engine.

```yaml
deidentification:
  batch_size: 10000
  date_offset_days: 34
  patient_id_prefix: 10000000
  random_seed: 42
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | integer | `1000` | Number of rows fetched and processed per batch. Larger values use more memory but reduce round trips. |
| `date_offset_days` | integer | `34` | Maximum date offset (in days) used by the `STATIC_OFFSET` rule. For `DATE_OFFSET`, the per-patient offset is a random value derived during mapping population. |
| `patient_id_prefix` | integer | `10000000` | Starting prefix for anonymized patient IDs. New IDs are assigned sequentially starting from this value (e.g., `10000001`, `10000002`, ...). |
| `random_seed` | integer | `42` | Seed for the random number generator used during mapping population (for deterministic offset generation). |

---

## `state_db_path` (optional)

Path to the SQLite database used for tracking pipeline run state.

```yaml
state_db_path: ./state.db
```

**Default:** `./state.db`

The state DB stores:
- **TableState** — per-table status (pending, in_progress, completed, failed), row counts, and ID ranges.
- **BatchState** — per-batch status for range-based processing.
- **RunLog** — timestamped log of pipeline runs with config hash and phase list.
- **DbConfig** — source/destination connection metadata snapshot.

Created automatically by `deid run` during the setup phase. Used by `deid status` to display progress.

---

## `mappings_db_path` (optional)

Path to the SQLite database containing ID mapping tables.

```yaml
mappings_db_path: ./hospital_db_mappings.db
```

**Default:** `./<source_database_name>_mappings.db`

The mappings DB stores:
- **PatientMapping** — `patient_id` → `nd_patient_id` (anonymized ID) + random `offset` (days) for date shifting.
- **EncounterMapping** — `encounter_id` → `nd_encounter_id`.
- **AppointmentMapping** — `appointment_id` → `nd_appointment_id`.
- **PhiStaging** — staging table for PII details.

Populated by `deid mapping`. Must exist before running `deid run`.

---

## `mappings_db` (optional)

Remote database connection for mappings (alternative to SQLite `mappings_db_path`). When set, the pipeline uses this MySQL/PostgreSQL database instead of a local SQLite file.

```yaml
mappings_db:
  type: mysql
  host: mappings-db.internal
  port: 3306
  database: mappings
  username: ${MAPPINGS_DB_USER}
  password: ${MAPPINGS_DB_PASS}
```

Same structure as `source_db`. When both `mappings_db` and `mappings_db_path` are set, `mappings_db` takes precedence.

---

## `failed_rows_db_path` (optional)

Path to the SQLite database for audit-logging rows that could not be de-identified (e.g. missing patient ID mapping).

```yaml
failed_rows_db_path: ./failed_rows.db
```

**Default:** `./failed_rows.db`

Failed rows are stored in per-source-schema tables: `failed_rows_{schema_name}`. Each table has the same structure (source_db, table_name, config_key, reason, row_data, failed_at). When using `--rerun`, only the rows for the tables being rerun **and** the current `config_key` are deleted — other schemas' and other keys' data is preserved.

---

## `qc_results_db_path` (optional)

Path to the SQLite database for persisting QC scan results.

```yaml
qc_results_db_path: ./qc_results.db
```

**Default:** `./qc_results.db`

Results are written incrementally as each table's QC scan completes. Each row contains: table name, pass/fail status, reason, source/dest row counts, sample size, and per-column results (JSON).

---

## `tables_to_run` (optional)

Explicit list of table names to process. When set, only these tables are run (must be a subset of tables defined in `tables` or `rules_csv`).

```yaml
tables_to_run:
  - patients
  - encounters
```

---

## `tables_to_run_csv` (optional)

Path to a CSV file listing table names to process (one per line, `#` comments supported). Alternative to `tables_to_run` for longer lists. Can also be passed via CLI: `--tables-csv`.

```yaml
tables_to_run_csv: ./tables_to_run.csv
```

---

## `redis_url` (optional)

URL for the Redis server used as the Celery message broker and for pub/sub progress events.

```yaml
redis_url: redis://localhost:6379/0
```

**Default:** `redis://localhost:6379/0`

Redis must be running and accessible. It serves two purposes:
1. **Celery broker** — task dispatch and result storage for the prefork worker pool.
2. **Progress pub/sub** — the orchestrator subscribes to progress events published by workers.

---

## `phases` (optional)

Which pipeline phases to execute when running `deid run`.

```yaml
phases:
  - setup
  - deidentify
  - qc
```

**Default:** `["setup", "deidentify"]`

| Phase | Description |
|-------|-------------|
| `setup` | Connect to source DB, discover tables, count rows, create `BatchState` entries in the state DB. |
| `deidentify` | Dispatch 3-stage task chain per batch (fetch → process → write) to Celery workers. Streams progress via Redis. |
| `qc` | (Deprecated in `deid run`; use `deid qc` standalone.) Sample rows from source and destination, verify rules, report pass/fail per column. Results saved to `qc_results.db`. |

You can run phases individually:

```bash
deid run --config config.yaml --phase setup
deid run --config config.yaml --phase deidentify
deid run --config config.yaml --phase qc
```

The `--phase` CLI flag overrides this config value.

---

## `workers` (optional)

Celery worker pool configuration.

```yaml
workers:
  fetchers: 2
  processors: 4
  max_retries: 1
  task_timeout: 3600
  max_tasks_per_child: 50
  max_tasks_per_child_fetch: 100
  max_tasks_per_child_process: 20
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `fetchers` | integer | `2` | Number of concurrent fetch worker processes (reading from source DB). |
| `processors` | integer | `16` | Number of concurrent process worker processes (applying de-identification rules). |
| `max_retries` | integer | `1` | Maximum number of automatic retries for a failed task before marking it as failed. |
| `task_timeout` | integer | `3600` | Hard time limit (seconds) for a single deidentification Celery task. Tasks exceeding this are terminated. |
| `max_tasks_per_child` | integer | `50` | Number of tasks a worker child process handles before being replaced. Prevents memory leaks. |
| `max_tasks_per_child_fetch` | integer | (inherits `max_tasks_per_child`) | Override for fetch workers specifically. |
| `max_tasks_per_child_process` | integer | (inherits `max_tasks_per_child`) | Override for process workers specifically. |

Write workers are spawned automatically — one per table with concurrency=1 to prevent MySQL lock-wait timeouts from concurrent INSERTs on the same table.

---

## `qc` (optional)

Quality control settings for the post-deidentification verification phase.

```yaml
qc:
  sample_size: 100
  scan_for_residual_pii: true
  task_timeout: 7200
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sample_size` | integer | `100` | Number of rows to sample from each table for QC verification. |
| `scan_for_residual_pii` | boolean | `true` | Whether to scan de-identified data for residual PII patterns that may have been missed. |
| `task_timeout` | integer | `7200` | Hard time limit (seconds) for a single QC Celery task. Separate from the deidentification `workers.task_timeout`. |

QC only runs for tables present in the current config. Results are persisted to `qc_results.db`. Run standalone with `deid qc --config config.yaml`.

---

## `mapping_tables` (optional)

Custom mapping table definitions for non-standard ID relationships.

```yaml
mapping_tables:
  custom_id:
    source_column: custom_id
    destination_column: nd_custom_id
    reference: patient_id
```

| Field | Type | Description |
|-------|------|-------------|
| `source_column` | string | Column name in the source table containing the original ID. |
| `destination_column` | string | Column name to use for the anonymized ID. |
| `reference` | string (optional) | If set, the column in the same table to use as a foreign key back to an existing mapping (e.g., `patient_id`). |

---

## `logging` (optional)

Logging configuration.

```yaml
logging:
  log_dir: ./logs
  log_verbosity: standard
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `log_dir` | string | `./logs` | Directory where log files are written. Created automatically. |
| `log_verbosity` | `minimal` \| `standard` \| `verbose` | `standard` | Controls the detail level of log output. `minimal` logs only errors and warnings. `standard` includes info-level messages. `verbose` enables debug-level logging. |

---

## `pii_db` (optional)

Connection details for the PII lookup database, used by the `NOTES` rule for patient-specific PII masking in clinical free-text.

```yaml
pii_db:
  master_connection_str: mysql+pymysql://user:pass@host:3306/pii_db
```

| Field | Type | Description |
|-------|------|-------------|
| `master_connection_str` | string | Full SQLAlchemy connection string for the PII database. |

When `pii_db` is configured, `deid run` requires:
1. PII tables to exist in the destination (created by `deid pii-table`).
2. `pii_config_path` to point to a valid YAML file (generated by `deid pii-table`).

**Skip this section entirely** if you only use `MASK`, `DATE_OFFSET`, `GENERIC_NOTES`, and other rules that don't require patient-specific PII lookup.

---

## `pii_tables_config` (optional)

Defines the structure of the PII lookup table: which source tables and columns to extract, and how to join them. Used by `deid pii-table` to create and populate `pii_data_table` in the PII database.

If omitted, `deid pii-table` auto-detects PII source tables by:
1. Scanning configured table rules for `MASK`/`PATIENT_DOB` columns alongside a `PATIENT_ID` column.
2. Falling back to introspecting all source DB tables for the same pattern.

Required when running `deid pii-table --config-only` or when the PII table already exists (auto-detected), since source DB introspection is skipped in those cases.

```yaml
pii_tables_config:
  pii_data_table:
    primary_column_name: patient_id
    upsert_instead_of_append: true
    tables:
      patients:
        primary_col: patient_id
        other_required_columns:
          - first_name
          - last_name
          - ssn
          - date_of_birth
          - phone
```

The top-level key (`pii_data_table`) is the name of the table created in the PII database.

| Field | Type | Description |
|-------|------|-------------|
| `primary_column_name` | string | Name of the patient ID column in the created PII table. Rows are keyed by this column. Typically `patient_id`. |
| `upsert_instead_of_append` | boolean | If `true`, re-running `deid pii-table` updates existing rows rather than inserting duplicates. |
| `tables` | dict | Map of source table names to their extraction config (see below). Multiple source tables can be merged into a single PII table — columns are prefixed with the source table name (e.g. `patients_first_name`). |

Each entry under `tables`:

| Field | Type | Description |
|-------|------|-------------|
| `primary_col` | string | The patient ID column in this source table, used to build the `SELECT` query when populating the PII table. **Required only when `deid pii-table` is creating/populating the table.** Can be omitted when using `--config-only` or when the PII table already exists, since no data insertion occurs. |
| `other_required_columns` | list of strings | Columns to extract from this source table. Each becomes a `{table}_{column}` column in the PII table. Always required — used by both table creation and `pii_config` generation. |

---

## `pii_config_path` (optional)

Path to the PII config YAML file generated by `deid pii-table`. This file contains the masking rules used by the NLP pipeline during `NOTES` rule processing.

```yaml
pii_config_path: ./pii_config.yaml
```

Generated by:

```bash
deid pii-table --config config.yaml
```

At runtime, `deid run` loads this file and injects the PII config into the pipeline. This is required when `pii_db` is configured.

---

## `pii_config` (optional)

Inline PII configuration. Alternative to `pii_config_path` — embeds the masking rules directly in `config.yaml` instead of a separate file.

```yaml
pii_config:
  mask:
    patients_first_name: {masking_value: "((FIRST_NAME))", min_length: 2}
    patients_last_name:  {masking_value: "((LAST_NAME))",  min_length: 2}
  dob:
    patients_date_of_birth: {}
  combine:
    patients_full_name:
      combine: [patients_first_name, patients_last_name]
      masking_value: "((PATIENT_NAME))"
```

`pii_config_path` is preferred in practice — `deid pii-table` writes this to a separate file automatically. Embed inline only if you are managing the config manually.

See [pii-config-reference.md](./pii-config-reference.md) for the full field reference.

---

## `secondary_pii_configs` (optional)

Additional PII sources for notes masking. Used when clinical notes may contain PII from a second source system (e.g. a separate insurance or scheduling database) that is not in the primary PII table.

Each entry is processed identically to the primary PII masking step: patient records are fetched by patient ID and exact-match patterns are applied to note text.

```yaml
secondary_pii_configs:
  - table_name: secondary_pii_table
    config:
      secondary_patients_first_name:
        masking_value: "((FIRST_NAME))"
        min_length: 2
      secondary_patients_last_name:
        masking_value: "((LAST_NAME))"
        min_length: 2
```

The secondary PII table is fetched from `pii_db.secondary_pii_connection_str` (a separate connection string under `pii_db`).

| Field | Type | Description |
|-------|------|-------------|
| `table_name` | string | Name of the table in the secondary PII database to query. |
| `config` | dict | Masking config for this table — same structure as `pii_config.mask`: column key → `{masking_value, min_length}`. |

Most deployments do not need this.

---

## `clinical_bin_doc` (optional)

Configuration for decrypting and processing ClinicalBin XML documents (encrypted clinical notes stored as binary blobs).

```yaml
clinical_bin_doc:
  source_db: mysql+pymysql://user:pass@host:3306/source
  dest_db: mysql+pymysql://user:pass@host:3306/dest
  source_table: ClinicalBin
  metadata_table: ClinicalDocuments
  dest_table: clinicalbin_xml_decrypt
  processed_table: clinicalbin_xml_processed
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_db` | string | — | SQLAlchemy connection string for the source DB containing encrypted documents. |
| `dest_db` | string | — | SQLAlchemy connection string for the destination DB. |
| `source_table` | string | `ClinicalBin` | Table containing encrypted binary content. |
| `metadata_table` | string | `ClinicalDocuments` | Table with document metadata (links binary to patient/encounter). |
| `dest_table` | string | `clinicalbin_xml_decrypt` | Destination table for decrypted XML content. |
| `processed_table` | string | `clinicalbin_xml_processed` | Tracking table for documents that have been processed. |

Only needed for environments with encrypted clinical documents in ClinicalBin format.

---

## Full Example

```yaml
# ── Database connections ──────────────────────────────────────────────
source_db:
  type: mysql
  host: db-source.internal
  port: 3306
  database: hospital_db
  username: ${SOURCE_DB_USER}
  password: ${SOURCE_DB_PASSWORD}

destination_db:
  type: postgresql
  host: db-dest.internal
  port: 5432
  database: hospital_db_deid
  username: ${DEST_DB_USER}
  password: ${DEST_DB_PASSWORD}

# ── Namespace key (isolates state/batches/queues per config) ──────────
config_key: historical

# ── Rules ─────────────────────────────────────────────────────────────
rules_csv: ./rules.csv

# ── Paths ─────────────────────────────────────────────────────────────
state_db_path: ./state.db
mappings_db_path: ./hospital_db_mappings.db
failed_rows_db_path: ./failed_rows.db
qc_results_db_path: ./qc_results.db

# ── De-identification settings ────────────────────────────────────────
deidentification:
  batch_size: 10000
  date_offset_days: 34
  patient_id_prefix: 10000000
  random_seed: 42

# ── Infrastructure ────────────────────────────────────────────────────
redis_url: redis://localhost:6379/0

# ── Pipeline phases ───────────────────────────────────────────────────
phases:
  - setup
  - deidentify

# ── Worker pool ───────────────────────────────────────────────────────
workers:
  fetchers: 2
  processors: 4
  max_retries: 1
  task_timeout: 3600
  max_tasks_per_child: 50

# ── Quality control ──────────────────────────────────────────────────
qc:
  sample_size: 100
  scan_for_residual_pii: true

# ── Logging ───────────────────────────────────────────────────────────
logging:
  log_dir: ./logs
  log_verbosity: standard

# ── PII database (for NOTES rule — patient-specific masking) ──────────
pii_db:
  master_connection_str: mysql+pymysql://${PII_DB_USER}:${PII_DB_PASSWORD}@localhost:3306/pii_db

pii_config_path: ./pii_config.yaml
```

---

## Workflow

The config file (or base + overlay pair) is used across multiple commands:

1. **`deid generate-config`** — Reads `source_db` to introspect tables, writes `rules_csv`.
2. **`deid mapping`** — Reads `source_db` + `tables`/`rules_csv` to find ID columns, writes `mappings_db_path`.
3. **`deid pii-table`** — Reads `source_db` + `pii_db` + rules, creates PII tables, writes `pii_config_path`.
4. **`deid run`** — Reads everything, validates prerequisites, executes the pipeline. Supports `--overlay` for task-specific config, `--rerun` for table-scoped cleanup, `--tables-csv` for table filtering.
5. **`deid qc`** — Standalone QC scanning after deidentification completes.
6. **`deid status`** — Check progress. Use `--config-key <key>` to filter to a specific namespace; omit to see all `config_key` groups at once.

Only `deid run` writes to the destination database. All other commands (except `deid qc` which reads dest) are setup steps.
