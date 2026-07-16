# deidentification_v2 — Claude Code Context

Python de-identification pipeline for healthcare data. Source database tables are processed
through a Celery task queue: mapping joins resolve patient/encounter/appointment/chart IDs,
`PatientIdentifierResolver` coalesces them into canonical `_resolved_*` columns, and
`DeIdentifier` applies structured + NLP rules to remove PHI.

---

## Running tests

```bash
# Python 3.12 conda env with requirements.txt installed
/Users/karanchilwal/miniconda3/envs/new_deid/bin/python -m pytest -q
```

---

## Critical files

| File | Role |
|---|---|
| `deid/tasks/process.py` | Celery task: reads Arrow, runs mapping joins, calls resolver → DeIdentifier → writes Arrow |
| `deid/core/process_df/main.py` | `PatientIdentifierResolver`, `JoinMapping`, `get_key_phi_column_list` |
| `deid/core/process_df/base.py` | `DeIdentifier` — orchestrates all rules per batch |
| `deid/core/process_df/rules.py` | `Rules` enum + all structured rule implementations |
| `deid/core/process_df/unstruct/notes.py` | `NotesRule` — NLP notes de-id; PII lookup + text replacement |
| `deid/core/process_df/unstruct/genericnotes.py` | `GenericNotesRule` — regex-based generic PHI detection (`GENERIC_REGEX_DICT`) |
| `deid/core/process_df/rowhandler.py` | `InvalidRowHandler` — rejects rows with null `_resolved_nd_patient_id` |
| `deid/core/ops_df/utility.py` | `join_dataframes` — Polars left-join helper with right_suffix / drop support |
| `docs/mapping_join_flow.md` | Step-by-step reference for how joins enrich `df` and how `_resolved_*` are built |

---

## Key architecture concepts

### `key_phi_columns` 5-tuple

```python
(
    encounter_id_cols,      # [str] — columns with ENCOUNTER_ID rule
    patient_id_cols_dict,   # {rule_name: [col]} — PATIENT_* rules (e.g. PATIENT_PATIENTID)
    reference_pid_cols,     # [str] — columns with REFERENCE_PID rule
    appointment_id_cols,    # [str] — columns with APPOINTMENT_ID rule
    chart_id_cols,          # [str] — columns with CHART_ID rule
)
```

Built by `get_key_phi_column_list()` in `deid/core/process_df/main.py`.

### Mapping join flow (`docs/mapping_join_flow.md`)

1. All source columns are lowercased (`process.py:107`).
2. Each secondary mapping table (`enc_df`, `apt_df`, `chart_df`) is enriched with `pat_df`
   before being joined into `df`. The `nd_patient_id` column is **renamed on the LEFT side**
   before the join (e.g. `nd_patient_id` → `nd_patient_id_from_encounter_mapping`) so Polars
   does not drop it as a right join key.
3. Each PATIENT_* rule joins `pat_df` directly, with `right_suffix=f"from_{identifier}_mapping"`.
   The right join key is dropped; all other `pat_df` columns survive as cross-join columns.

### `PatientIdentifierResolver` (`main.py`)

- **Step 1** — `_resolved_offset`: coalesce of all `offset_from_*_mapping` cols, falls back to `offset_days` config.
- **Step 2** — `_resolved_nd_patient_id`: coalesce priority: referencepid > encounter > [PATIENT_* groups] > appointment > chart.
- **Step 2b** — `_resolved_ndpid_col_{col}` for **each** PATIENT_* source column: that column's OWN de-identified value (from its own mapping join), *not* coalesced. This keeps two patient-ID columns in the same row that reference different patients (e.g. `mergelogs` FromID/ToID) from collapsing to one value. Consumed by `PatientIDRule` (falls back to `_resolved_nd_patient_id` when absent).
- **Step 3** — `_resolved_{identifier}` for **every** identifier in `possible_patient_identifier_columns`, regardless of which rules the current table has. For a group's "own" identifier, uses the direct source column (the right join key was dropped).
- **Step 4** — Drops all intermediate `*_from_*_mapping` columns.

The identifier groups are **per source column**: the first column of each PATIENT_* rule keeps the identifier-keyed join suffix (`from_{identifier_col}_mapping`); additional columns use a per-column suffix (`from_col_{col}_mapping`) so their joins don't collide. Both mapping paths (`apply_patient_mappings` in `main.py` and the preloaded path in `process.py`) join **every** column of a rule, not just `columns[0]`.

### `_resolved_nd_patient_id`

The de-identified patient ID. Used by:
- `InvalidRowHandler` — null → row written to `failed_rows` SQLite table and excluded from output.
- `NotesRule` — PII table lookup key (`PIITable._get_table` queries `pii_table.c.nd_patient_id`).
- `DeIdentifier` / `PatientIDRule` — replacement value written to patient-ID columns (per-column `_resolved_ndpid_col_{col}` takes precedence; see Step 2b).

### `_resolved_{identifier}` (e.g. `_resolved_patientid`, `_resolved_pid`)

Coalesced raw identifier value, same priority order as `_resolved_nd_patient_id`.
Used by `NotesRule.de_identify_key_phi_columns` to find and replace identifier values in
note text with the de-identified patient ID.

**Important**: PII table lookups always use `_resolved_nd_patient_id`, NOT `_resolved_{identifier}`.

### `possible_patient_identifier_columns`

Project-level list from `mapping_tables.patient.identifier_columns` in the YAML config.
Passed through: `process.py` → `PatientIdentifierResolver` + `DeIdentifier` → `NotesRule`.

### Preloaded vs JoinMapping paths (`process.py`)

```python
preloaded = get_preloaded_data()
if preloaded:
    # use in-memory DataFrames (fast)
    enc_df  = preloaded.get("encounter_mapping")   # may be None
    pat_df  = preloaded.get("patient_mapping")      # may be None
    apt_df  = preloaded.get("appointment_mapping")  # may be None
    chart_df = preloaded.get("chart_mapping")        # may be None
    # each join is individually gated on `enc_df is not None and key_phi_columns[0]` etc.
else:
    # per-batch SQL joins via JoinMapping (fallback)
```

`preloaded` being truthy does NOT guarantee that all needed mapping tables are present —
a specific mapping (e.g. `encounter_mapping`) may still be `None` and its join silently skipped.

---

## Recent work (branch: `chartid_deid_reworked`)

- Added `CHART_ID` rule and `chart_mapping` join support throughout the pipeline.
- Fixed Polars right-key drop: renamed `nd_patient_id` on the LEFT side before each
  enrichment join so it survives as a non-key column.
- `PatientIdentifierResolver`: generates `_resolved_{identifier}` for every project identifier;
  removed the old `_resolved_patient_id` step; drops all cross-join `{id}_from_{path}_mapping`
  cols after coalesce.
- `NotesRule`: multi-identifier text replacement (iterates all `_resolved_{x}` cols);
  all PII lookup paths use `_resolved_nd_patient_id` as the lookup key.
- `PIITable._get_table`: fixed `AttributeError` — now queries `pii_table.c.nd_patient_id`
  (the PII table has no `patient_id` column).
