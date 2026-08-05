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

- `mapping_pg.patient_mapping_table(nd_patient_id PK, patientid UNIQUE, offset, registration_date, excluded, excluded_at, created_at, updated_at)`
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

## Patient exclusion (`excluded` / `excluded_at`)

Patients matching the exclusion criteria are no longer filtered out of the mapping
delta. They are mapped like everyone else and flagged `excluded = 1`, with
`excluded_at` recording when the flag was first raised.

Why: the old filter left excluded patients with no mapping row at all, so there was
no record of who was excluded or from when, and a patient who resurfaced in a later
CDC window without exclusion criteria was minted a **new** `nd_patient_id`.

- **Schema** — `airflow-automation/Airflow/scripts/sql/add_patient_exclusion_flag_dent.sql`
  (idempotent; `mapping_delta.py` also applies the same ALTERs on startup).
- **The flag is sticky.** `mapping_delta.py` only ever raises it. The criteria are
  evaluated against the staging *delta* (one CDC window), so a patient dropping out
  of today's exclusion set says nothing about whether they still qualify in the full
  source — auto-clearing would un-exclude nearly everyone the day after they were
  flagged. Clearing requires re-evaluating against the full source database.
- **Encounters of excluded patients are mapped.** Staging only holds the current
  window, so an encounter skipped here is never offered again; mapping them keeps the
  history complete if an exclusion is ever lifted. Gating is at the patient level.
- **Consumers filter `excluded = 0`** — `master_pii_delta.py`, `master_insurance_delta.py`,
  the de-id mapping preload (`deid/tasks/celery_app.py`) and the per-batch join path
  (`deid/core/process_df/main.py`). The preload and join path additionally drop
  encounter/appointment/chart rows for excluded patients, because those rows carry
  `nd_patient_id` themselves and the resolver coalesces it.

### Clients without the column

`excluded` is dent-only. `celery_app.py` and `main.py` are shared by every client, so
each filter is gated on the column actually being there, decided by **reflection** —
never by running a SELECT and catching the failure. That matters: `mappings` may be
PostgreSQL, where a failed statement aborts the surrounding transaction and would take
out every later mapping-table load on the same connection.

| Site | Column present | Column absent |
|---|---|---|
| `celery_app._preload_mappings` | one reflection at preload; patient rows filtered, enc/apt/chart anti-joined | full `SELECT *`, exactly as before; logs INFO that gating is off |
| `main.apply_patient_mappings` | `WHERE excluded = 0` added | clause omitted |
| `main._get_patient_mapping_from_nd_patient_id` | `WHERE excluded = 0` added | clause omitted |
| `main._get_excluded_nd_patient_ids` | returns the excluded subset | returns `[]`, memoised per `JoinMapping` so it reflects once |
| `master_*_delta.load_patient_map` | `WHERE excluded = 0` | loads everything and logs a WARNING — dent-only scripts, so absence means the migration has not run |

Adding the column to another client's mapping schema is all that is needed to turn the
gating on there; nothing is dent-specific beyond the exclusion criteria themselves,
which live in `mapping_delta.py`. Coverage: `tests/test_preload_exclusion.py` runs the
preload against a schema with the column and one without.

### Tables with only the `ENCOUNTER_ID` rule

These have no patient-ID column, so `encounter_mapping` is the *only* route to a
de-identified patient. Filtering `patient_mapping_table` alone does nothing for them —
the encounter row carries `nd_patient_id` and the resolver coalesces it straight into
`_resolved_nd_patient_id`. Dropping the excluded patient's **encounter rows** is what
gates these tables, in both paths:

- preload — anti-join in `celery_app._drop_excluded_patient_mappings`
- per-batch — anti-join in `main._get_mapping_with_patient_join` (reached via
  `_get_encounter_mapping`), scoped to excluded patients only so that rows for merely
  *unmapped* patients keep their existing behaviour

The source row then matches no encounter mapping, `_resolved_nd_patient_id` is null, and
`InvalidRowHandler` rejects it. Same mechanism covers `APPOINTMENT_ID`- and `CHART_ID`-only
tables. Coverage: `tests/test_encounter_only_exclusion.py`, which also pins the leak that
occurs if only `patient_mapping_table` is filtered.

**Operational consequence:** rejected rows land in `failed_rows` with
`reason = "unresolved_id:_resolved_nd_patient_id"` — the same reason as a genuinely broken
mapping. Once exclusions accumulate, policy exclusions will dominate that table and can
mask real mapping breakage. There is no failure threshold that aborts a run, so this is a
monitoring concern rather than a correctness one. If it becomes noisy, give the handler the
excluded set so it can tag those rows with a distinct reason (or skip persisting them).

Who is excluded, and since when:

```sql
SELECT nd_patient_id, patientid, excluded_at
FROM mapping_pg.patient_mapping_table
WHERE excluded = 1
ORDER BY excluded_at DESC;
```

**Backfill caveat** — patients excluded by *earlier* runs have no mapping row to flag,
so they start out invisible and get flagged as their criteria resurface in future CDC
windows. To flag the historical set in one pass, run the exclusion query against the
full source DB and `UPDATE ... SET excluded = 1` (template at the bottom of the
migration SQL). Separately: a patient already written to `master_pg` before being
flagged keeps those rows — purge them if that is required.

## Known behavior (intentional, not a bug)

- **`nd_encounter_id` scheme unchanged** — still `nd_patient_id*10000+1` then increment, with a
  global-uniqueness guard. Existing encounter IDs are preserved (no re-identification).
- **`nd_ActiveFlag` on reassignment** — new encounters are written `'Y'`; existing ones are
  updated in place. The delta does **not** deactivate an old row when an encounter moves to a
  different patient (this matches the pre-migration behavior; the v4/v5-style deactivate-on-
  reassign was deliberately not added here). Revisit if dent sees real encounter reassignment.
