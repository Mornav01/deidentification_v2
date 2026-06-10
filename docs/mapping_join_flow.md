# Mapping Join Flow: How `_resolved_nd_patient_id` and `_resolved_{identifier}` Are Computed

This document walks through exactly how a source DataFrame is enriched with mapping
columns and how the resolved columns are computed from them.

---

## Example table configuration

The examples below assume a table with **six rules** and a project-level
`possible_patient_identifier_columns = ["patientid", "pid", "profileid"]`:

| Column (lowercased) | Rule |
|---|---|
| `patientid` | `PATIENT_PATIENTID` |
| `pid` | `PATIENT_PID` |
| `profileid` | `PATIENT_PROFILEID` |
| `clinicalencounterid` | `ENCOUNTER_ID` |
| `appointmentid` | `APPOINTMENT_ID` |
| `chartid` | `CHART_ID` |

All source DataFrame columns are lowercased before any join (handled at `process.py:107`).

---

## Step 0 — Starting DataFrame

```
df columns (source only):
  patientid, pid, profileid, clinicalencounterid, appointmentid, chartid,
  ... (all other source columns, lowercased)
```

---

## Step 1 — Encounter enrichment

`enc_df` from `encounter_mapping_table` contains:
`encounter_id`, `nd_encounter_id`, `patient_id`, `nd_patient_id`

Before joining with `pat_df`, `nd_patient_id` is **renamed**:
```python
_enc = enc_df.rename({"nd_patient_id": "nd_patient_id_from_encounter_mapping"})
```

`_enc` is then joined with `pat_df` (patient_mapping_table) on
`nd_patient_id_from_encounter_mapping = nd_patient_id`, right_suffix=`from_encounter_mapping`.

All `pat_df` columns are suffixed before the join, so `enc_enriched` adds:
- `offset_from_encounter_mapping`
- `patientid_from_encounter_mapping`
- `pid_from_encounter_mapping`
- `profileid_from_encounter_mapping`
- *(any other patient_mapping_table columns with the suffix)*

`enc_enriched` now contains:
`encounter_id`, `nd_encounter_id`, `patient_id`, `nd_patient_id_from_encounter_mapping`,
`offset_from_encounter_mapping`, `patientid_from_encounter_mapping`, ...

This is joined into the source `df` on `clinicalencounterid = encounter_id`
(right join key `encounter_id` is dropped). After this join `df` gains:
- `nd_encounter_id`
- `nd_patient_id_from_encounter_mapping`
- `offset_from_encounter_mapping`
- `patientid_from_encounter_mapping`, `pid_from_encounter_mapping`, `profileid_from_encounter_mapping`

---

## Steps 2, 3, 4 — PATIENT_* direct joins

For each `PATIENT_{IDENTIFIER}` rule, `pat_df` is joined directly into `df`.
`right_suffix = "from_{identifier}_mapping"` (identifier is the rule suffix lowercased:
`patientid`, `pid`, `profileid`).

All `pat_df` columns are renamed before the join. The join key on the right side becomes
`{identifier}_from_{identifier}_mapping` (dropped after the join). All OTHER identifier
columns from `pat_df` survive as cross-join columns.

**After Step 2 (PATIENT_PATIENTID):** `df` gains:
- `nd_patient_id_from_patientid_mapping`
- `offset_from_patientid_mapping`
- `pid_from_patientid_mapping` *(cross-join: pid from the same patient row)*
- `profileid_from_patientid_mapping` *(cross-join)*
- `patientid_from_patientid_mapping` — **dropped** (it was the right join key)

**After Step 3 (PATIENT_PID):** `df` gains:
- `nd_patient_id_from_pid_mapping`
- `offset_from_pid_mapping`
- `patientid_from_pid_mapping` *(cross-join)*
- `profileid_from_pid_mapping` *(cross-join)*
- `pid_from_pid_mapping` — **dropped** (right join key)

**After Step 4 (PATIENT_PROFILEID):** `df` gains:
- `nd_patient_id_from_profileid_mapping`
- `offset_from_profileid_mapping`
- `patientid_from_profileid_mapping` *(cross-join)*
- `pid_from_profileid_mapping` *(cross-join)*
- `profileid_from_profileid_mapping` — **dropped** (right join key)

---

## Step 5 — Appointment enrichment

Same pattern as encounter. `apt_df` (`appointment_mapping_table`) contains:
`appointment_id`, `nd_appointment_id`, `patient_id`, `nd_patient_id`

`nd_patient_id` is renamed to `nd_patient_id_from_appointment_mapping` before
joining with `pat_df`. The enriched `apt_enriched` is then joined into `df`
on `appointmentid = appointment_id`.

`df` gains:
- `nd_appointment_id`
- `nd_patient_id_from_appointment_mapping`
- `offset_from_appointment_mapping`
- `patientid_from_appointment_mapping`, `pid_from_appointment_mapping`, `profileid_from_appointment_mapping`

---

## Step 6 — Chart enrichment

Same pattern as encounter/appointment. `chart_df` (`chart_mapping_table`) contains:
`chart_id`, `nd_chart_id`, `patient_id`, `nd_patient_id`

`nd_patient_id` is renamed to `nd_patient_id_from_chart_mapping` before
joining with `pat_df`. The enriched `chart_enriched` is then joined into `df`
on `chartid = chart_id`.

`df` gains:
- `nd_chart_id`
- `nd_patient_id_from_chart_mapping`
- `offset_from_chart_mapping`
- `patientid_from_chart_mapping`, `pid_from_chart_mapping`, `profileid_from_chart_mapping`

> **Note**: `nd_chart_id` is NOT a source column — it comes entirely from this join.
> A row with a valid `chartid` but no matching row in `chart_mapping_table` will have
> `nd_chart_id = null`.

---

## Step 7 — `PatientIdentifierResolver` groups

`PatientIdentifierResolver.transform()` in `process_df/main.py` reads the intermediate
columns and builds coalesce groups. Each group stores an `identifier_col` key so it can
be reused for `_resolved_{identifier}` computation:

| Group name | `identifier_col` | `nd_patient_id` column | `offset` column |
|---|---|---|---|
| `referencepid_group` | — | `nd_patient_id_from_referencepid_mapping` | `offset_from_referencepid_mapping` |
| `encounter_group` | — | `nd_patient_id_from_encounter_mapping` | `offset_from_encounter_mapping` |
| `identifier_groups[0]` (PATIENT_PATIENTID) | `patientid` | `nd_patient_id_from_patientid_mapping` | `offset_from_patientid_mapping` |
| `identifier_groups[1]` (PATIENT_PID) | `pid` | `nd_patient_id_from_pid_mapping` | `offset_from_pid_mapping` |
| `identifier_groups[2]` (PATIENT_PROFILEID) | `profileid` | `nd_patient_id_from_profileid_mapping` | `offset_from_profileid_mapping` |
| `appointment_group` | — | `nd_patient_id_from_appointment_mapping` | `offset_from_appointment_mapping` |
| `chart_group` | — | `nd_patient_id_from_chart_mapping` | `offset_from_chart_mapping` |

Only groups whose columns are **actually present** in `df` are included in the coalesce —
`_coalesce_expr` filters to existing columns.

---

## Step 8 — Coalesce into `_resolved_*`

### `_resolved_offset` and `_resolved_nd_patient_id`

```python
_resolved_offset = coalesce(
    offset_from_referencepid_mapping,
    offset_from_encounter_mapping,
    offset_from_patientid_mapping,
    offset_from_pid_mapping,
    offset_from_profileid_mapping,
    offset_from_appointment_mapping,
    offset_from_chart_mapping,
)  # falls back to offset_days config value if all null

_resolved_nd_patient_id = coalesce(
    nd_patient_id_from_referencepid_mapping,
    nd_patient_id_from_encounter_mapping,
    nd_patient_id_from_patientid_mapping,
    nd_patient_id_from_pid_mapping,
    nd_patient_id_from_profileid_mapping,
    nd_patient_id_from_appointment_mapping,
    nd_patient_id_from_chart_mapping,
)
```

### `_resolved_{identifier}` — one per project identifier

Generated for **every identifier in `possible_patient_identifier_columns`**, regardless of
whether the current table has a `PATIENT_*` rule for that identifier. This means a table
with only `ENCOUNTER_ID` still produces `_resolved_patientid` and `_resolved_pid` from the
cross-join columns that the encounter enrichment brought in.

For each identifier `x`, the candidates follow the same priority order as
`_resolved_nd_patient_id`. The key difference: for the "own" group (where
`identifier_col == x`), the right join key `x_from_x_mapping` was dropped during the join,
so the **direct source column `x`** is used instead.

```python
_resolved_patientid = coalesce(
    patientid_from_referencepid_mapping,
    patientid_from_encounter_mapping,
    patientid,                          # own group: source column (join key was dropped)
    patientid_from_pid_mapping,         # cross-join from PATIENT_PID path
    patientid_from_profileid_mapping,   # cross-join from PATIENT_PROFILEID path
    patientid_from_appointment_mapping,
    patientid_from_chart_mapping,
)

_resolved_pid = coalesce(
    pid_from_referencepid_mapping,
    pid_from_encounter_mapping,
    pid_from_patientid_mapping,         # cross-join from PATIENT_PATIENTID path
    pid,                                # own group: source column (join key was dropped)
    pid_from_profileid_mapping,         # cross-join from PATIENT_PROFILEID path
    pid_from_appointment_mapping,
    pid_from_chart_mapping,
)

_resolved_profileid = coalesce(
    profileid_from_referencepid_mapping,
    profileid_from_encounter_mapping,
    profileid_from_patientid_mapping,
    profileid_from_pid_mapping,
    profileid,                          # own group: source column
    profileid_from_appointment_mapping,
    profileid_from_chart_mapping,
)
```

The first non-null value wins. If **all** candidates are null, the `_resolved_{identifier}`
column will be null for that row (which is fine as long as `_resolved_nd_patient_id` is
non-null).

---

## Step 9 — Drop intermediate columns

`PatientIdentifierResolver` drops all intermediate columns after the coalesce:

- `nd_patient_id_from_*` and `offset_from_*` (the nd_patient_id / offset intermediates)
- All cross-join identifier columns: `{identifier}_from_{path}_mapping` for every
  identifier in `possible_patient_identifier_columns` and every join path suffix

**Not dropped:**
- Original source PHI columns (`patientid`, `pid`, `profileid`) — kept as-is
- The `_resolved_*` output columns

**Surviving columns after the resolver step:**

| Column | Source |
|---|---|
| `nd_encounter_id` | encounter join |
| `nd_appointment_id` | appointment join |
| `nd_chart_id` | chart join |
| `patientid`, `pid`, `profileid` | original source columns (kept) |
| `_resolved_offset` | coalesced |
| `_resolved_nd_patient_id` | coalesced |
| `_resolved_patientid` | coalesced (all paths for `patientid`) |
| `_resolved_pid` | coalesced (all paths for `pid`) |
| `_resolved_profileid` | coalesced (all paths for `profileid`) |
| *(all other original source columns)* | source |

---

## Row rejection criterion

`InvalidRowHandler` inspects only `_resolved_nd_patient_id`:

- **null** → row is rejected, serialised to `failed_rows_{db_name}` in the audit SQLite DB
- **non-null** → row passes through

`nd_encounter_id`, `nd_appointment_id`, and `nd_chart_id` being null is **not** grounds
for rejection. A document-only table where `chartid` is the sole identifier is valid as
long as `nd_patient_id_from_chart_mapping` (and therefore `_resolved_nd_patient_id`) is
non-null.

---

## How `_resolved_{identifier}` is used in NotesRule

`NotesRule.de_identify_key_phi_columns` iterates over **all** `_resolved_{identifier}`
columns present in the DataFrame and replaces each value in the note text with
`_resolved_nd_patient_id`:

```python
# For each project identifier, find its original value in the note and replace it.
for rid_list in resolved_id_lists:   # one list per _resolved_{identifier} column
    rid = rid_list[i]
    if rid is not None:
        text = _compiled(rid).sub(nd_pid_repl, text)
```

`deidentify_primary_pii_values` uses the **first** available `_resolved_{identifier}`
column as the lookup key into `pii_data_table` (patient names, DOBs, etc.).

This means PII masking and identifier text replacement both work correctly even for tables
that have no `PATIENT_*` rules — as long as the encounter, appointment, or chart join
brought in the identifier value via a cross-join column.

---

## Why `nd_patient_id` must be renamed before the join

Polars excludes the **right join key** from the result when `left_on != right_on`.
The old code used `drop_left_join_column=True` (dropping `nd_patient_id` from the
mapping DF) and expected the renamed right key `nd_patient_id_from_{type}_mapping`
to survive — but Polars drops it instead. Evidence: `offset_from_chart_mapping`
(a non-key right column) appeared in rows; `nd_patient_id_from_chart_mapping` (the
right key) did not.

The fix is to rename `nd_patient_id` on the **left** DataFrame before the join so it
travels as a regular non-key column and is never excluded by Polars.

```python
# Correct pattern (preloaded path, process.py):
_chart = chart_df.rename({"nd_patient_id": "nd_patient_id_from_chart_mapping"})
chart_enriched = join_dataframes(
    _chart, pat_df,
    left_on="nd_patient_id_from_chart_mapping",
    right_on="nd_patient_id",
    how="left", right_suffix="from_chart_mapping",
    drop_right_join_column=True,
)

# Correct pattern (JoinMapping fallback path, main.py _get_mapping_with_patient_join):
nd_pid_col = f"nd_patient_id_{right_suffix}"
if "nd_patient_id" in df_mapping.columns:
    df_mapping = df_mapping.rename({"nd_patient_id": nd_pid_col})
df_joined = join_dataframes(
    df_mapping, df_patient_mapping,
    left_on=nd_pid_col, right_on="nd_patient_id",
    how="left", right_suffix=right_suffix,
    drop_right_join_column=True,
)
```
