# Quickstart Guide

This guide walks through every step to go from a fresh install to a fully de-identified database.

---

## Prerequisites

- Python >= 3.11
- Redis server running locally (or accessible via URL)
- Access to your source database (MySQL, MSSQL, PostgreSQL, or Snowflake)
- Access to your destination database

```bash
# Install dependencies
pip install -r requirements.txt
pip install -e .

# Start Redis (if not already running)
redis-server
```

---

## Step 1: Create Your Config File

Start with a minimal `config.yaml`:

```yaml
source_db:
  type: mysql
  host: localhost
  port: 3306
  database: hospital_db
  username: ${SOURCE_DB_USER}
  password: ${SOURCE_DB_PASSWORD}

destination_db:
  type: postgresql
  host: localhost
  port: 5432
  database: hospital_db_deid
  username: ${DEST_DB_USER}
  password: ${DEST_DB_PASSWORD}

# Where to store the rules CSV (used by generate-config)
rules_csv: ./rules.csv

# De-identification settings
deidentification:
  batch_size: 10000
  date_offset_days: 34
  patient_id_prefix: 10000000

# Redis broker for Celery workers
redis_url: redis://localhost:6379/0
```

Set the referenced environment variables:

```bash
export SOURCE_DB_USER=myuser
export SOURCE_DB_PASSWORD=mypassword
export DEST_DB_USER=destuser
export DEST_DB_PASSWORD=destpassword
```

---

## Step 2: Generate Rules CSV

Introspect your source database and generate a CSV with auto-assigned de-identification rules:

```bash
deid generate-config --config config.yaml
```

This connects to the source database (read-only), lists all tables and columns, and writes a CSV like:

```csv
table_name,column_name,data_type,rule
patients,patient_id,INTEGER,PATIENT_ID
patients,first_name,VARCHAR(100),MASK
patients,last_name,VARCHAR(100),MASK
patients,date_of_birth,DATE,PATIENT_DOB
patients,ssn,VARCHAR(11),MASK
patients,admission_date,DATETIME,DATE_OFFSET
patients,clinical_notes,LONGTEXT,GENERIC_NOTES
encounters,encounter_id,INTEGER,ENCOUNTER_ID
encounters,patient_id,INTEGER,PATIENT_ID
encounters,admit_date,DATETIME,DATE_OFFSET
encounters,diagnosis,VARCHAR(500),
```

**Review the CSV carefully.** The auto-assignment uses column name patterns and may need correction:
- Columns with no rule assigned (empty `rule` column) will be copied as-is
- Verify `PATIENT_ID` is correctly assigned to your patient ID columns
- Add `NOTES` rule (NLP-based) or `GENERIC_NOTES` (regex-based) for free-text columns
- Add `ENCOUNTER_ID` / `APPOINTMENT_ID` rules where appropriate

You can also filter to specific tables:

```bash
deid generate-config --config config.yaml --tables patients encounters labs
```

Or specify a database schema:

```bash
deid generate-config --config config.yaml --schema dbo
```

---

## Step 3: Populate Mapping Tables

Create the SQLite mappings database and populate patient, encounter, and appointment ID mappings:

```bash
deid mapping --config config.yaml
```

Output:

```
Source DB:    mysql 'hospital_db' at localhost:3306
Mappings DB:  ./hospital_db_mappings.db
Tables:       15

Scanning source tables for IDs...

Mapping population complete.
  Patients:     50000 found, 50000 created
  Encounters:   120000 found, 120000 created
  Appointments: 30000 found, 30000 created
```

This creates `mappings.db` (or the path specified in `mappings_db_path`) with:
- **PatientMapping** — original patient ID to anonymized ID + random date offset
- **EncounterMapping** — original encounter ID to anonymized ID
- **AppointmentMapping** — original appointment ID to anonymized ID

You can override the mappings DB path:

```bash
deid mapping --config config.yaml --mappings-db ./custom_mappings.db
```

---

## Step 4: Create PII Tables (Optional)

If your config has a `pii_db` section (needed for `NOTES` rule — patient-specific PII masking in clinical notes), create the PII lookup tables:

First, add the PII database config to your `config.yaml`:

```yaml
pii_db:
  master_connection_str: mysql+pymysql://user:pass@host:3306/pii_db
```

Then run:

```bash
deid pii-table --config config.yaml
```

This does three things:
1. **Auto-detects PII source tables** — scans your configured tables for columns with `MASK`/`PATIENT_DOB` rules paired with a `PATIENT_ID` column
2. **Creates and populates PII lookup tables** — in the PII destination database, with columns like `patients_first_name`, `patients_last_name`, etc.
3. **Generates `pii_config.yaml`** — masking rules for the NLP pipeline

Add the generated config path to your `config.yaml`:

```yaml
pii_config_path: ./pii_config.yaml
```

You can override where the pii_config file is written:

```bash
deid pii-table --config config.yaml --pii-config-output ./my_pii_config.yaml
```

**Skip this step** if you don't use the `NOTES` rule or don't have a `pii_db` configured.

---

## Step 5: Run the Pipeline

```bash
deid run --config config.yaml

# Or with a base config + task-specific overlay:
deid run --config base.yaml --overlay task.yaml
```

The pipeline validates prerequisites before starting:
- Mappings DB must exist with populated patient mappings
- If `pii_db` is configured, PII tables and `pii_config` must be available

Then it executes two phases by default:

### Phase 1: Setup
- Connects to source DB and counts rows per table
- Creates `state.db` with table states and batch ranges

### Phase 2: Deidentify (3-stage pipeline)
- Spawns Celery worker processes (fetch, process, per-table write queues)
- Dispatches 3-stage task chain per batch:
  - **Fetch**: keyset-paginated read from source → Arrow IPC file
  - **Process**: join mappings → apply de-identification rules → Arrow IPC file
  - **Write**: idempotent INSERT to destination DB
- Streams progress via Redis pub/sub

### Run Individual Phases

```bash
deid run --config config.yaml --phase setup
deid run --config config.yaml --phase deidentify
```

### Run QC (standalone)

QC is recommended as a separate step after deidentification:

```bash
deid qc --config config.yaml
deid qc --config config.yaml --table specific_table
```

QC results are persisted to `qc_results.db`.

### Clean-Slate Rerun

To start over for the configured tables (drops their destination tables, clears their state/batch rows, deletes their failed rows, removes their staging files, purges their write queues — other tables are untouched):

```bash
deid run --config config.yaml --rerun

# With overlay:
deid run --config base.yaml --overlay task.yaml --rerun
```

### Run Only Specific Tables

Filter to specific tables using a CSV file (one table name per line):

```bash
deid run --config config.yaml --tables-csv tables_to_run.csv
```

Or combine with rerun:

```bash
deid run --config config.yaml --rerun --tables-csv tables_to_run.csv
```

The CSV format is simple — one table name per line, `#` comments supported:

```csv
patients
encounters
# labs  — skip this one for now
appointments
```

You can also set `tables_to_run` or `tables_to_run_csv` in config.yaml.

---

## Step 6: Check Status

```bash
deid status --state-db ./state.db
```

Shows run progress, per-table status (pending/completed/failed), and batch counts.

---

## Standalone Scripts

All setup commands are also available as standalone Python scripts, useful for environments where the `deid` CLI is not installed:

```bash
python scripts/generate_config_csv.py --config config.yaml --output rules.csv
python scripts/populate_mappings.py --config config.yaml
python scripts/populate_pii_table.py --config config.yaml
```

These call the same core logic as their CLI counterparts.

---

## Command Reference

| Command | Purpose |
|---------|---------|
| `deid generate-config` | Generate rules CSV by introspecting source database |
| `deid mapping` | Create and populate mapping tables |
| `deid pii-table` | Create PII tables and generate pii_config YAML |
| `deid run` | Run the de-identification pipeline (`--rerun`, `--tables-csv`, `--phase`) |
| `deid qc` | Run QC scanning standalone (`--table` for single table) |
| `deid status` | Check run progress |
| `deid retry` | Re-dispatch failed batches |
| `deid cdc` | Process Change Data Capture feeds |
| `deid decrypt-notes` | Decrypt encrypted clinical notes |

All commands accept `--config` / `-c` and `--log-level` / `-l` flags.

---

## Troubleshooting

### "Mappings DB not found" / "No patient mappings found"

Run `deid mapping --config config.yaml` before `deid run`.

### "pii_db is configured but pii_config is not available"

Run `deid pii-table --config config.yaml` and add `pii_config_path` to your config.yaml.

### "PII tables missing in destination"

Run `deid pii-table --config config.yaml` to create the PII tables.

### Rules CSV has many unassigned columns

This is expected — only columns matching known PII patterns get auto-assigned rules. Review the CSV and manually assign rules for columns that need de-identification, or leave them empty to copy as-is.

### Pipeline seems stuck with no CPU usage

If this happens during QC, the sampling queries may be slow. QC uses efficient ID-range sampling instead of `ORDER BY RAND()`. Check your Redis connection and worker logs.

### Encounter/patient IDs appear as decimals (e.g. 12345.0)

This was a known issue with Float64 promotion after Polars left joins. All ID rules now cast to Int64 before writing. If you see this, ensure you're running the latest code and use `--rerun` to recreate destination tables with correct BIGINT types.

### MySQL "Row size too large" error

The pipeline automatically converts large VARCHAR columns to LONGTEXT when the row would exceed MySQL's 65535-byte limit. It also disables `sql_mode` and `innodb_strict_mode` during table creation. If you still encounter this, check that your MySQL user has permission to SET session variables.
