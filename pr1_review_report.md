# PR #1 Review Report: `config_key` State Isolation

**PR:** neurodiscoveryai/deidentification_v2#1
**Author:** nd-karan
**Branch:** `karan_rework` -> `main`
**Review date:** 2026-04-06
**Reviewer:** shubhamk (via Claude Code)
**Status:** Approved for merge with follow-up fixes documented below

---

## 1. Summary

This PR adds a `config_key` namespace field that isolates all pipeline state per configuration. It enables multiple independent de-identification runs (e.g. `historical`, `incremental`, `adhoc`) against the **same state.db, failed_rows.db, and Redis instance** without cross-contamination.

**3 commits, 19 files changed, +328 / -75**

| Commit | SHA | Description |
|--------|-----|-------------|
| 1 | `0e493d41` | Core feature: `config_key` field across models, tasks, CLI, orchestrator, staging, docs |
| 2 | `1e61cd7a` | Bug fixes for missing `config_key` filters in qc, retry, run, async_runner |
| 3 | `41928ba4` | Rerun ordering fix, early exit guard, reference mappings path, secondary PII config path |

---

## 2. What Was Added

### 2.1 Config Schema (`deid/config/schema.py`)

- `config_key: str = "default"` with `@field_validator` enforcing `[a-zA-Z0-9_-]+`
- `reference_mappings_path: Optional[str]` + `reference_mappings: dict` with `@model_validator` to load YAML
- `secondary_pii_config_path: Optional[str]`
- `filter_tables_to_run` softened: logs warning instead of raising `ValueError` when no tables match

### 2.2 State Models (`deid/models/state.py`, `deid/models/failed_rows.py`)

- `TableState`: added `config_key` column, unique constraint changed to `(table_name, db_config_id, config_key)`
- `BatchState`: added `config_key` column, unique constraint changed to `(table_name, start_id, end_id, config_key)`, composite index `ix_batchstate_table_config`
- `failed_rows_*`: added `config_key` column with index

### 2.3 Task Payloads (`deid/config/task_models.py`)

- `FetchTaskConfig`, `ProcessTaskConfig`, `WriteTaskConfig`: all gained `config_key: str = "default"`

### 2.4 Staging (`deid/staging.py`)

- `batch_fetched_path()` / `batch_processed_path()`: path changed from `root/table/batch.arrow` to `root/config_key/table/batch.arrow`
- `reconcile()`: now accepts and filters by `config_key`

### 2.5 Tasks (`deid/tasks/fetch.py`, `process.py`, `write.py`)

- All batch status updates now filter by `config_key`
- All staging path calls pass `config_key`
- Write queue naming: `deid-write-{table}` -> `deid-write-{config_key}-{table}`
- `_claim_next_pending_batch()`: accepts `config_key` parameter

### 2.6 Orchestrator (`deid/orchestrator/async_runner.py`)

- `_setup_phase`: `DbConfig` lookup/creation scoped by `config_key`; `TableState`/`BatchState` creation includes `config_key`
- `_deidentify_phase`: all `BatchState` queries filtered by `config_key`; reconcile called with `config_key`
- `_qc_phase`: `TableState.filter_by(status="completed", config_key=...)`
- `_get_table_details`: injects `reference_mapping` from `config.reference_mappings`
- New: secondary PII config path loading

### 2.7 CLI (`deid/cli/run.py`, `qc.py`, `retry.py`, `status.py`)

- **run.py**: rerun cleanup scoped by `config_key` (state, failed rows, staging, queues); unmatched tables also cleared on rerun; early exit if all tables unmatched; worker queue names include `config_key`
- **qc.py**: QC query filtered by `config_key`
- **retry.py**: batch lookups include `config_key`
- **status.py**: new `--config-key` / `-k` option; groups tables by config_key when unfiltered

### 2.8 Row Handler (`deid/core/process_df/rowhandler.py`)

- `InvalidRowHandler.__init__` accepts `config_key`; writes it to failed_rows table

### 2.9 Documentation (`docs/config-reference.md`, `docs/quickstart.md`)

- Full documentation of `config_key` with examples, affected areas, and CLI usage

---

## 3. Issues to Fix Post-Merge

### 3.1 MUST FIX (High Priority)

#### 3.1.1 SQL Injection via String Interpolation

**Files:** `deid/cli/run.py` ~lines 120-122, 140-142

Three DELETE statements interpolate `config_key` and `table_names` directly into raw SQL:

```python
placeholders = ",".join(f"'{t}'" for t in all_names_to_clear)
ck = cfg.config_key
conn.execute(sa_text(
    f"DELETE FROM batch_states WHERE table_name IN ({placeholders}) AND config_key = '{ck}'"
))
```

`config_key` is validated by Pydantic (`[a-zA-Z0-9_-]+`), but `table_names` come from:
- `cfg.tables[].name` (from YAML config)
- `cfg.unmatched_tables` (from `tables_to_run` / `tables_to_run_csv` — user-controlled CSV input)

Table names from a CSV file are **not validated** against injection characters.

**Fix:** Use parameterized queries with SQLAlchemy `text()` bindings:

```python
from sqlalchemy import text as sa_text, bindparam

stmt = sa_text(
    "DELETE FROM batch_states WHERE table_name IN :names AND config_key = :ck"
).bindparams(bindparam("names", expanding=True))

conn.execute(stmt, {"names": list(all_names_to_clear), "ck": ck})
```

Apply to all three DELETE statements in `_rerun_cleanup()`.

**Affected lines:**
- `deid/cli/run.py`: batch_states DELETE (~line 121)
- `deid/cli/run.py`: table_states DELETE (~line 122)
- `deid/cli/run.py`: failed_rows DELETE (~line 142)

---

#### 3.1.2 Remove `constants_2.py` — Dead Code Violating Project Conventions

**File:** `deid/core/process_df/constants_2.py` (98 lines, new file)

This file:
- Is **never imported** anywhere (confirmed via grep)
- Uses `import re2` with fallback — **directly violates** the project convention in CLAUDE.md: _"Do NOT use google-re2 — its Python bindings have 50x overhead due to string marshalling"_
- Contains duplicated date/ZIP regex patterns that likely overlap with `genericnotes.py`
- Has a `_2` suffix indicating it's scratch/experimental

**Fix:** Delete the file entirely.

```bash
git rm deid/core/process_df/constants_2.py
```

---

#### 3.1.3 Remove `.DS_Store` from Version Control

**File:** `.DS_Store` (binary, macOS Finder metadata)

Should not be tracked. Already exists in the repo history but the PR modifies it.

**Fix:**

```bash
echo ".DS_Store" >> .gitignore   # if not already present
git rm --cached .DS_Store
```

---

### 3.2 SHOULD FIX (Medium Priority)

#### 3.2.1 No Database Migration for Existing `state.db` Files

The PR adds `config_key` columns to `TableState`, `BatchState`, and `failed_rows_*` tables, and changes unique constraints. SQLAlchemy's `create_all()` will NOT:
- Add new columns to existing tables
- Update existing unique constraints

Existing deployments with a populated `state.db` will fail with column-not-found errors.

**Fix options:**
- A) Add an Alembic migration (preferred for production)
- B) Document that users must delete and recreate state.db
- C) Add a `deid migrate-state` CLI command that runs ALTER TABLE statements

---

#### 3.2.2 `reference_mappings` Has No Schema Validation

**File:** `deid/config/schema.py`

`reference_mappings: dict` is completely untyped. The YAML is loaded and stored as-is with no structural validation. If the YAML has unexpected structure, the error surfaces much later in `_get_table_details()` when `.get(table_name, "")` is called.

**Fix:** Add a type hint and validator:

```python
reference_mappings: dict[str, str] = Field(default_factory=dict, exclude=True)

@model_validator(mode="after")
def load_reference_mappings(self) -> "DeidConfig":
    if self.reference_mappings_path:
        import yaml as _yaml
        p = Path(self.reference_mappings_path)
        if not p.exists():
            raise ValueError(f"reference_mappings_path '{p}' does not exist")
        with open(p) as f:
            data = _yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError(f"reference_mappings_path must contain a YAML mapping, got {type(data).__name__}")
        self.reference_mappings = data
    return self
```

---

#### 3.2.3 Secondary PII Config Loading Requires `pii_db`

**File:** `deid/orchestrator/async_runner.py` ~line 49

```python
if config.pii_db and not config.secondary_pii_configs and config.secondary_pii_config_path:
```

The `config.pii_db` guard means `secondary_pii_config_path` is **silently ignored** unless `pii_db` is also configured. If this is intentional (secondary PII configs only make sense when PII DB exists), add a comment. If not, remove the `config.pii_db` condition.

---

#### 3.2.4 No Test Coverage

No test files were added or modified. For a feature this foundational, the following tests are needed:

| Test | Purpose |
|------|---------|
| Two configs with different `config_key` produce isolated `TableState` rows | Core isolation correctness |
| `--rerun` with one `config_key` doesn't delete state for another | Isolation under cleanup |
| `config_key` validator rejects special characters (`'; DROP TABLE--`) | Input validation |
| `deid status` groups by config_key when no filter | CLI output correctness |
| `deid status --config-key X` shows only X's tables | CLI filtering correctness |
| `reference_mappings_path` loads and injects into `_get_table_details` | New feature correctness |

---

### 3.3 NICE TO HAVE (Low Priority)

#### 3.3.1 `Optional[str]` vs `str | None` Inconsistency

**File:** `deid/cli/status.py` line 4

```python
from typing import Optional
# ...
config_key: Optional[str] = typer.Option(...)
```

The rest of the codebase uses PEP 604 union syntax (`str | None`). Minor style inconsistency.

---

#### 3.3.2 Unrelated `requirements.txt` Change

**File:** `requirements.txt`

Adds `pyarrow>=23.0.1` and removes trailing newline. Unrelated to config_key feature. Should ideally be a separate commit.

---

#### 3.3.3 Commit Hygiene

Commit 2 exists solely to fix bugs introduced in commit 1. For a clean history, these should be squashed:

| Current | Suggested |
|---------|-----------|
| Commit 1: feature (with bugs) | Squash 1+2: `feat: add config_key state isolation` |
| Commit 2: bug fixes | |
| Commit 3: cleanup + ref mappings | Keep: `fix: rerun ordering, add reference_mappings_path` |

---

#### 3.3.4 `by_status: dict = {}` Type Annotation on Local

**File:** `deid/cli/status.py` ~line 192

Explicit type annotation on a local variable is unusual for this codebase. `by_status = {}` suffices.

---

#### 3.3.5 Filter Value Deduplication Missing in `jointables.py`

Not changed in this PR, but relevant context: `_load_reference_table()` in `jointables.py` does not deduplicate `filter_values` before constructing IN clauses. The `origin/dev` branch uses `dict.fromkeys()` for this. Minor efficiency improvement.

---

## 4. Cross-Branch Comparison Notes

The mapping logic was compared between `shubhamk_rework` and `origin/dev`. Two items from dev are **not present in rework** and may be worth porting:

### 4.1 `_fill_missing_patient_ids` (from dev's `MappingTableLoader`)

Dev's `MappingTableLoader._fill_missing_patient_ids()` automatically enriches patient IDs from encounter mappings when a table only has encounter IDs but no patient ID column. Rework relies on `PatientIdentifierResolver` coalesce logic, which may or may not cover the same case.

**Action:** Verify that `PatientIdentifierResolver` handles tables with encounter IDs but no patient IDs. If not, port the backfill logic.

### 4.2 PHI Safety Difference (rework is safer)

When a mapping column is missing after a join:
- **Rework:** Nulls the target column — prevents PHI leakage
- **Dev:** Logs a warning, leaves original data — real patient IDs can appear in destination

This is a **data safety bug in dev**, not in this PR. Rework's behavior is correct.

---

## 5. Prioritized Action Items

### Before Next Merge

| # | Priority | Issue | Effort | Section |
|---|----------|-------|--------|---------|
| 1 | **P0** | Parameterize SQL in `run.py` (3 DELETE statements) | Small | 3.1.1 |
| 2 | **P0** | Delete `constants_2.py` | Trivial | 3.1.2 |
| 3 | **P0** | Remove `.DS_Store` from tracking, add to `.gitignore` | Trivial | 3.1.3 |
| 4 | **P1** | Add state.db migration path or document "delete and recreate" | Medium | 3.2.1 |
| 5 | **P1** | Validate `reference_mappings` schema + file existence | Small | 3.2.2 |
| 6 | **P1** | Clarify/fix `pii_db` guard on secondary PII config loading | Trivial | 3.2.3 |
| 7 | **P1** | Add test coverage for config_key isolation | Medium | 3.2.4 |
| 8 | **P2** | Fix `Optional[str]` style inconsistency | Trivial | 3.3.1 |
| 9 | **P2** | Squash commits 1+2 (if history rewrite is acceptable) | Trivial | 3.3.3 |
| 10 | **P2** | Verify `PatientIdentifierResolver` handles encounter-only tables | Medium | 4.1 |

---

## 6. Verdict

**Approve for merge.** The feature design is solid, the scoping is comprehensive, and the documentation is excellent. Commit 2 fixed the critical bugs from commit 1, and commit 3 improved the rerun flow. The remaining issues (SQL injection, dead code, missing tests) are real but bounded — they don't block merge if addressed promptly in the next PR.
