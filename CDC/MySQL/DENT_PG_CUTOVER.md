# DENT CDC — cutover to `mapping_pg` / `master_pg`

Runbook for moving the dent (mobiledoc, MySQL-source) CDC pipeline from the legacy
`mapping` / `master` schemas to the new `mapping_pg` / `master_pg` structure.

## What changed (code, already merged)

| File | Change |
|---|---|
| `airflow-automation/.../scripts/sql/mapping_migration_to_new_structure_dent.sql` | One-time migration: builds `mapping_pg` + `master_pg`, migrates data, adds keys. |
| `CDC/MySQL/mapping_delta.py` | Writes `patientid` + `nd_patient_id` bridge + `nd_ActiveFlag`; no audit/reference cols; `nd_patient_id` assigned as `MAX+1`. |
| `CDC/MySQL/master_pii_delta.py` | Resolves `patient_id → nd_patient_id` (`--mapping_schema`), keeps `patientid`, upserts on `uq_nd_patient_id`. |
| `CDC/MySQL/master_insurance_delta.py` | Same; dedups to one row per patient. |
| `airflow-automation/.../dags/dent/cdc_automation_dag.py` | Passes `--mapping_schema` to both master delta tasks. |

Final table shape (both keys present everywhere; `nd_patient_id` is the join key,
`patientid` is retained/denormalised):

- `mapping_pg.patient_mapping_table(nd_patient_id PK, patientid UNIQUE, offset, registration_date, created_at, updated_at)`
- `mapping_pg.encounter_mapping_table(id PK, nd_patient_id, encounter_id, nd_encounter_id UNIQUE, encounter_date, nd_ActiveFlag, created_at, updated_at, patientid)`
- `master_pg.pii_data_table(nd_patient_id UNIQUE, patientid, <PII cols…>)`
- `master_pg.master_insurance_table(nd_patient_id UNIQUE, patientid, encounter_id, <hcfa cols…>)`

## Prerequisites

1. Back up the dent MySQL `mapping` and `master` schemas.
2. Confirm the legacy `mapping` schema is current (the migration reads from it).
3. Pick a low-traffic window — the migration is a one-time bulk copy.

## Cutover steps (in order — do not run the pipeline against a half-migrated schema)

1. **Run the migration.** Execute `mapping_migration_to_new_structure_dent.sql` on the dent
   MySQL host. Review its inline verification output (Sections 6 & 10): row-count parity and
   the `unmapped nd` counts (should be ~0; any non-zero is orphan master rows with no mapped
   patient — investigate before proceeding).

2. **Activate the single-identifier bypass.** Add to dent's deid config
   (`dent_config_incremental.yaml`, and any other dent config the deid run uses) under
   `mapping_tables.patient`:
   ```yaml
   identifier_columns: [patientid]
   ```
   This makes `async_runner.py` auto-remap the legacy rules `PATIENT_ID → PATIENT_PATIENTID`
   and `PATIENT_DOB → DOB` at startup, so **the rules CSV needs no edits** and the deid reader
   resolves against the new `patientid` column. (Without this line the `PATIENT_ID` rule is
   silently skipped on the new schema — see `main.py` `apply_patient_mappings`.)

3. **Flip the schema env vars** in the Airflow `.env`:
   ```
   MAPPING_DB_NAME=mapping_pg
   MASTER_DB_NAME=master_pg
   ```

## Verification

- **Migration parity** — Sections 6 & 10 of the migration SQL (patient/encounter/pii/insurance
  row counts old vs new; unmapped-nd counts).
- **Delta smoke test** — with a staging schema that has known deltas, run manually:
  ```
  python CDC/MySQL/mapping_delta.py          --mapping_schema mapping_pg --staging_schema <staging>
  python CDC/MySQL/master_pii_delta.py       --master_schema master_pg  --staging_schema <staging> --mapping_schema mapping_pg
  python CDC/MySQL/master_insurance_delta.py --master_schema master_pg  --staging_schema <staging> --mapping_schema mapping_pg
  ```
  Expect: no column-unknown errors; new patients get `MAX+1` `nd_patient_id`; encounters carry
  the correct `nd_patient_id` bridge + `patientid`; master rows keyed on `nd_patient_id`.
- **Encounter bridge spot check** —
  ```sql
  SELECT e.encounter_id, e.nd_encounter_id, e.nd_patient_id, e.patientid, p.patientid
  FROM mapping_pg.encounter_mapping_table e
  JOIN mapping_pg.patient_mapping_table p ON e.nd_patient_id = p.nd_patient_id
  LIMIT 20;   -- e.patientid must equal p.patientid
  ```
- **Deid read smoke test** — run the deid on one small table and confirm rows are NOT rejected
  to `failed_rows` for null `_resolved_nd_patient_id`, and PII lookups resolve on `nd_patient_id`.

## Rollback

The migration only *creates* `mapping_pg` / `master_pg`; it does not alter the legacy schemas.
To roll back: set `MAPPING_DB_NAME=mapping` / `MASTER_DB_NAME=master` and remove the
`identifier_columns` line. The `*_pg` databases can be dropped and the migration re-run later.

## Known behavior (intentional, not a bug)

- **`nd_encounter_id` scheme unchanged** — still `nd_patient_id*10000+1` then increment, with a
  global-uniqueness guard. Existing encounter IDs are preserved (no re-identification).
- **`nd_ActiveFlag` on reassignment** — new encounters are written `'Y'`; existing ones are
  updated in place. The delta does **not** deactivate an old row when an encounter moves to a
  different patient (this matches the pre-migration behavior; the v4/v5-style deactivate-on-
  reassign was deliberately not added here). Revisit if dent sees real encounter reassignment.
