# PR #1 Follow-Up Fixes — Changelog

**Date:** 2026-04-06
**Branch:** `shubhamk_rework`
**Context:** Post-merge fixes for PR #1 (`config_key` state isolation by nd-karan)
**Test result:** 173 passed, 0 failed

---

## Changes Summary

9 files changed: +35 insertions, -122 deletions (net -87 lines)

---

## Fix 1 (P0): SQL Injection — Parameterized Queries

**File:** `deid/cli/run.py` (lines 120-135, 149-155)

**Problem:** Three DELETE statements in `_rerun_cleanup()` used f-string interpolation to inject `table_names` and `config_key` directly into raw SQL. Table names originate from user-supplied CSV files (`--tables-csv`), making this a SQL injection vector:

```python
# BEFORE (vulnerable)
placeholders = ",".join(f"'{t}'" for t in all_names_to_clear)
ck = cfg.config_key
conn.execute(sa_text(
    f"DELETE FROM batch_states WHERE table_name IN ({placeholders}) AND config_key = '{ck}'"
))
```

**Fix:** Replaced all three statements with SQLAlchemy parameterized queries using `bindparam(expanding=True)` for safe IN-clause expansion:

```python
# AFTER (safe)
from sqlalchemy import bindparam
delete_batch = sa_text(
    "DELETE FROM batch_states WHERE table_name IN :names AND config_key = :ck"
).bindparams(bindparam("names", expanding=True))
conn.execute(delete_batch, {"names": all_names_to_clear, "ck": cfg.config_key})
```

**Statements fixed:**
1. `DELETE FROM batch_states` (state.db cleanup)
2. `DELETE FROM table_states` (state.db cleanup)
3. `DELETE FROM {fr_table}` (failed_rows.db cleanup)

**Note:** The `fr_table` name in statement 3 is still interpolated into the SQL string. This is a table name derived from the source database schema name (`failed_rows_{schema}`), not from user input — and SQLAlchemy does not support parameterized table names. This is acceptable.

---

## Fix 2 (P0): Removed Dead Code — `constants_2.py`

**File:** `deid/core/process_df/constants_2.py` (deleted, -98 lines)

**Problem:** This file was:
- Never imported anywhere in the codebase (confirmed via grep)
- Used `import re2` with fallback, violating the project convention: *"Do NOT use google-re2 — its Python bindings have 50x overhead due to string marshalling"* (CLAUDE.md)
- Contained duplicated date/ZIP regex patterns already present in `genericnotes.py`
- Had a `_2` suffix indicating experimental/scratch code

**Fix:** Deleted the file.

---

## Fix 3 (P0): Removed `.DS_Store` from Git Tracking

**File:** `.DS_Store` (untracked via `git rm --cached`)

**Problem:** macOS Finder metadata file was tracked in git despite `.DS_Store` already being listed in `.gitignore` (line 85). The PR modified this binary file.

**Fix:** `git rm --cached .DS_Store`. The existing `.gitignore` entry prevents re-addition.

---

## Fix 4 (P1): Validated `reference_mappings` Schema

**File:** `deid/config/schema.py` (lines 127-145)

**Problem:** The `load_reference_mappings` model validator opened a YAML file without checking existence and stored the result as an untyped `dict` with no structural validation. Errors would surface late (in `_get_table_details()`) with unclear messages.

**Fix:**
- Added file existence check with clear error: `reference_mappings_path '{p}' does not exist`
- Added type validation: rejects non-dict YAML content with `got {type}`
- Tightened type hint from `dict` to `dict[str, str]`

```python
# BEFORE
reference_mappings: dict = Field(default_factory=dict, exclude=True)
# open() without existence check, no type validation

# AFTER
reference_mappings: dict[str, str] = Field(default_factory=dict, exclude=True)
# Path.exists() check, isinstance(data, dict) check
```

---

## Fix 5 (P1): Secondary PII Config Loading Guard

**File:** `deid/orchestrator/async_runner.py` (line 49)

**Problem:** The condition for loading `secondary_pii_config_path` required `config.pii_db` to be set:

```python
# BEFORE
if config.pii_db and not config.secondary_pii_configs and config.secondary_pii_config_path:
```

This meant `secondary_pii_config_path` was silently ignored when `pii_db` was not configured, which is unnecessarily restrictive — secondary PII configs can be useful independently.

**Fix:** Removed the `config.pii_db` guard:

```python
# AFTER
if not config.secondary_pii_configs and config.secondary_pii_config_path:
```

---

## Fix 6 (P2): `Optional[str]` Style Inconsistency

**File:** `deid/cli/status.py` (lines 4, 14)

**Problem:** Used `Optional[str]` (requires `from typing import Optional`) while the rest of the codebase uses PEP 604 union syntax (`str | None`).

**Fix:** Changed to `str | None` and removed unused `from typing import Optional` import.

---

## Fix 7 (P2): Duplicate `pyarrow` in `requirements.txt`

**File:** `requirements.txt` (lines 29, 52)

**Problem:** Two conflicting entries:
- Line 29: `pyarrow==23.0.1` (pinned exact, pre-existing)
- Line 52: `pyarrow>=23.0.1` (added by PR, duplicate, missing trailing newline)

**Fix:** Changed line 29 to `pyarrow>=23.0.1` (consistent with the rest of the file's `>=` convention) and removed the duplicate entry at line 52. Restored trailing newline.

---

## Fix 8: Updated Tests for `config_key` Staging Paths

**Files:** `tests/test_staging.py`, `tests/test_fetch_task.py`, `tests/test_process_task.py`

**Problem:** PR #1 changed staging paths from `staging_root/{table}/` to `staging_root/{config_key}/{table}/` but did not update the corresponding test assertions. 4 tests were failing:

- `test_batch_fetched_path` — expected `staging_root/patients/...`
- `test_batch_processed_path` — expected `staging_root/patients/...`
- `test_fetch_batch_writes_arrow_and_updates_state` — expected `staging_root/patients/...`
- `test_process_batch_deidentifies_and_writes_proc_arrow` — expected `staging_root/t1/...`

**Fix:** Updated all path assertions to include the default `config_key` (`"default"`):

```python
# BEFORE
Path(staging_root) / "patients" / "batch_1_5.arrow"

# AFTER
Path(staging_root) / "default" / "patients" / "batch_1_5.arrow"
```

---

## Remaining Items (Not Addressed)

These items from the review report were not addressed in this changeset:

| Item | Reason |
|------|--------|
| State.db migration for existing deployments | Requires design decision — Alembic migration vs. documented "recreate state.db" |
| Test coverage for `config_key` isolation | Separate effort — needs integration-level tests with multiple configs |
| `PatientIdentifierResolver` encounter-only table handling | Needs investigation to confirm whether rework covers dev's `_fill_missing_patient_ids` |
| Filter value dedup in `jointables.py` | Minor optimization, not a correctness issue |
