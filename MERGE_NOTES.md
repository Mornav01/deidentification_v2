# Merge Notes — `mind` + `dent` → `main`

Review companion for the `mind-into-main` PR. **TL;DR:** we did *not* blindly merge either
branch. `main` stays the base because it has the newer engine generation (the `nd_patient_id`
mapping rework, MySQL-backed state DB, stateful retry). On top of that we brought **security &
config hardening from `mind`** and the **production CDC pipeline + worker robustness/perf
optimizations from `dent`** — taking the best of each while keeping `main`'s mapping intact.

---

## Why this approach

The three branches diverged into different strengths:

| Area | Best source | Reason |
|---|---|---|
| Mapping / `nd_patient_id` resolution, chartid, MySQL state DB, stateful retry | **main** | Newest generation; `dent` predates it |
| Security (no hardcoded creds), config-from-env, notebooks→scripts | **mind** | Hardening pass |
| CDC pipeline (`CDC/MySQL/`), worker/orchestrator robustness & perf | **dent** | Current production CDC + ops fixes |

A straight merge of `dent` would have **regressed** main's mapping (and its MySQL state DB,
retry tracking, `table_batch_size`, `identifier_columns`). So instead we **cherry-adopted**
only the mapping-agnostic improvements, adapting each to main's APIs.

---

## What's in this PR (commits)

1. **`688ec6f` — mind → main**
   - **Config loader env enhancement** (`deid/config/loader.py`): a whole-value `${VAR}` whose
     env value is valid JSON now expands to a list/dict (scalars stay strings — fully backward
     compatible). Enables structured config from env, e.g. pii `replace_value` lists.
   - **Credential scrub**: every hardcoded DB credential replaced with `os.environ` reads
     (CDC scripts, notebooks-turned-scripts, etc.). Repo-wide secret scan: **0**.
   - **Notebooks → scripts**: `.ipynb` converted to `.py` and removed.

2. **`96570b0` — dent CDC pipeline + engine optimizations → main**
   - **`CDC/MySQL/` production pipeline** brought from `dent` (parser/restore with DELETE +
     soft-delete, row-based event correctness, AI value via "prod max+1", deadlock fix,
     dump-metadata optimization, connection retry guardrails). Excludes backup/`copy`/`_old`
     files and notebooks. `deid/cdc/` left as-is (identical on both branches).
   - **DB connection pool tuning** (`dbPkg/dbhandler.py`): `pool_size` 1/5→6, `pool_timeout`
     30→1000 ms — fewer connection-exhaustion stalls under load.
   - **Celery broker resilience** (`tasks/celery_app.py`): `broker_connection_retry` +
     socket keepalive/timeouts — survives idle Redis socket drops on NAT/firewall setups.
   - **Load-once mapping preload** (`tasks/celery_app.py`): mapping/master/appointment tables
     are loaded **once in the parent** (`worker_init`) and shared with all workers via
     `fork()` copy-on-write — instead of each worker re-loading its own copy. Faster startup,
     much lower RAM.
   - **Log-collector resilience** (`orchestrator/log_collector.py`): Redis listener with
     keepalive + health-check + reconnect/backoff (async model preserved).

3. **Worker improvements (this PR's latest commit)**
   - **No-PHI pass-through** (`orchestrator/async_runner.py`): a configured table with **no PHI
     rules** skips the full fetch→process→write Celery pipeline. Same-server → server-side
     `CREATE TABLE … AS SELECT`; cross-server (e.g. **MSSQL source → MySQL dest**) → create the
     dest table from the source schema and **stream** rows over (server-side cursor, O(batch)
     memory). Marks the table `completed` in state. **Falls back to the normal pipeline on any
     failure.**
   - **Empty-config PHI-recovery guard** (`tasks/process.py`): if a worker receives a task with
     **missing `columns_details`** (e.g. Redis evicted the payload under memory pressure, or an
     orphaned batch), it recovers the PHI rules from the persistent **state DB**
     (`TableState.rules_config`). If still absent → **raises loudly and retries** rather than
     processing a batch rule-less (which would write PHI un-deidentified).
   - **Log-publisher resilience** (`core/log_publisher.py`): lru-cached Redis connection pool +
     keepalive/health-check; `publish_log` swallows transient blips so a Redis hiccup never
     kills a running task.

All adopted code uses **main's** `config.state_db_url` + `get_cached_state_engine(...)` (which
supports the **MySQL state DB**), not dent's SQLite-only `state_db_path`.

---

## Deliberately NOT brought from `dent` (and why)

These looked like "simplifications" but are actually `dent` being **behind** main — adopting
them would regress current behavior:

- **Mapping rework** (`process_df/main.py`, `mapping_populator.py`, `rowhandler.py`,
  `jointables.py`, parts of `notes.py`): main's `nd_patient_id` / `_resolved_*` / chartid /
  multi-identifier logic is newer. **Keep main.**
- **SQLite-only state** (`models/base.py`): main supports a **MySQL-backed state DB** via
  `state_db_url` (used by some clients). Dent dropped it. **Keep main.**
- **Removed stateful retry** (`models/state.py` `retry_count`/`last_failed_reason`) and the
  **`cli/retry.py` rewrite**: main keeps DB-backed retry + reconciliation. **Keep main.**
- **Removed `identifier_columns` / `table_batch_size` / table-overrides** (`config/schema.py`):
  these are main features. **Keep main.**
- **Date handling**: main intentionally **nulls** unparsed dates to prevent PHI leakage; dent
  preserved originals. **Keep main.**
- **Cruft**: `cdc_*  copy.py`, `decrypt_pnotes_old.py`, committed logs, `.ipynb`, and a
  scratch `id_validation.py` (which itself carried hardcoded creds — scrubbed).

---

## Review guide / what to verify

- **Mapping core is untouched** — `process_df/main.py`, `mapping_populator.py`,
  `cli/run.py`, `config/schema.py`, `config/loader.py` (except the additive JSON-env change)
  show no functional mapping changes.
- **Pass-through is safe-by-default** — on any error or unexpected topology it falls back to the
  normal pipeline; it only fires for tables with genuinely empty rules.
- **PHI-recovery fails closed** — never processes a batch without rules; it raises and retries.
- **No secrets** — repo-wide credential scan returns 0; new env vars (`SRC_DB_*`,
  `DST_DB_SCHEMA`, etc.) are documented in `.env.example`.
- **Backward-compatible APIs** — `log_publisher` / preload signatures unchanged; all importers
  satisfied.

### Files to focus on
- `deid/orchestrator/async_runner.py` — pass-through copy (both topologies)
- `deid/tasks/process.py` — empty-config PHI-recovery guard
- `deid/tasks/celery_app.py` — load-once preload + broker resilience
- `deid/core/log_publisher.py`, `deid/orchestrator/log_collector.py` — Redis resilience
- `deid/config/loader.py` — JSON-from-env interpolation
- `CDC/MySQL/*` — dent production CDC pipeline (credential-scrubbed)

---

## Companion PR

The orchestration side lives in the **`airflow-automation`** repo (its own `mind-into-main`
PR): EHR-based DAG reorg, single per-EHR pipelines, deid-CLI invoked via a single wrapper,
removal of the legacy Django/`deIdentification` code path, config env-driving, and the
`.env.example` partition (active vs legacy/manual). Review that PR alongside this one.
