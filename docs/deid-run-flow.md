# `deid run` — Detailed Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        deid run --config base.yaml                  │
│                  [--overlay task.yaml] [--rerun] [--tables-csv]     │
└───────────────────────────────────┬─────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │     CLI INITIALIZATION         │
                    │  (deid/cli/run.py:run_command) │
                    └───────────────┬───────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  1. load_config(base.yaml, overlay=task)   │
              │     - YAML load + ${ENV_VAR} interpolation │
              │     - Deep-merge overlay onto base         │
              │     - Pydantic validation (DeidConfig)     │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  2. --tables-csv filter (optional)         │
              │     - Parse CSV → table name list          │
              │     - Match against config rules           │
              │     - Unmatched → state.db as "failed"     │
              │       with failure_remarks                  │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  3. --rerun cleanup (optional, per-table)  │
              │     ┌───────────────────────────────────┐  │
              │     │ a. DROP dest tables (SQLAlchemy)   │  │
              │     │ b. DELETE state.db rows            │  │
              │     │    (batch_states + table_states)   │  │
              │     │ c. DELETE failed_rows for tables   │  │
              │     │ d. rm -rf staging/<table>/ dirs    │  │
              │     └───────────────────────────────────┘  │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  4. create_celery_app(redis_url)           │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  5. Purge Redis queues for this config_key │
              │     (unconditional — queue hygiene)        │
              │     ┌───────────────────────────────────┐  │
              │     │ deid-fetch-<config_key>            │  │
              │     │ deid-process-<config_key>          │  │
              │     │ deid-write-<config_key>-<table>    │  │
              │     └───────────────────────────────────┘  │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  6. Spawn Celery worker subprocesses       │
              │     ┌───────────────────────────────────┐  │
              │     │ deid-fetch-<config_key>   (N)     │  │
              │     │ deid-process-<config_key> (N)     │  │
              │     │ deid-write-<config_key>-<table>    │  │
              │     │     (1 per table)                  │  │
              │     └───────────────────────────────────┘  │
              │     Wait 3s, verify none exited early      │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  7. asyncio.run(orchestrator.run(cfg))     │
              │     (deid/orchestrator/async_runner.py)    │
              └─────────────────────┬─────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────┐
│                      ORCHESTRATOR INIT                               │
│  - Create state.db, mappings.db (read-only), failed_rows.db engines │
│  - Load pii_config from YAML if pii_db set                          │
│  - Validate: mappings exist, PII tables exist (if configured)       │
│  - Create RunLog in state.db                                         │
│  - Start LogCollector (async Redis pub/sub listener)                 │
└───────────────────────────────────┬─────────────────────────────────┘
                                    │
════════════════════════════════════╪══════════════════════════════════
                          PHASE: SETUP
════════════════════════════════════╪══════════════════════════════════
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  1. Gather row counts (parallel async)     │
              │     NDDBHandler.get_rows_count(table)      │
              │     for each table in config               │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  2. Persist state (sequential SQLite)      │
              │     - Create/find DbConfig row             │
              │     - Create TableState per table          │
              │       (status="pending", row_count=N)      │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  3. Pre-split into BatchState rows         │
              │     For each table:                        │
              │       offset=0 → row_count, step=batch_sz  │
              │       INSERT BatchState(start_id=offset,   │
              │         end_id=offset+batch_sz-1,          │
              │         status="pending")                   │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  4. Cleanup stale .tmp Arrow files         │
              └─────────────────────┬─────────────────────┘
                                    │
════════════════════════════════════╪══════════════════════════════════
                       PHASE: DEIDENTIFY
════════════════════════════════════╪══════════════════════════════════
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  Reconcile staging (crash recovery)        │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  Initial dispatch: claim FIRST pending     │
              │  batch per table via atomic UPDATE         │
              │    pending → dispatched                    │
              │    → fetch_batch.apply_async              │
              │       (queue=deid-fetch-<config_key>)      │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  Resume in-progress (crash recovery):      │
              │    fetched  → dispatch process_batch       │
              │    processed → dispatch write_batch        │
              └─────────────────────┬─────────────────────┘
                                    │
        ┌───────────────────────────▼───────────────────────────┐
        │              3-STAGE CELERY PIPELINE                   │
        │        (self-chaining — each batch flows through)      │
        │                                                        │
        │  ┌──────────────────────────────────────────────────┐  │
        │  │  STAGE 1: fetch_batch                             │  │
        │  │           (deid-fetch-<config_key> queue)         │  │
        │  │  ┌────────────────────────────────────────────┐  │  │
        │  │  │ 1. Keyset-paginated SELECT from source DB  │  │  │
        │  │  │    (WHERE id > last_fetched_id LIMIT N)    │  │  │
        │  │  │ 2. Embed column schema as Arrow metadata   │  │  │
        │  │  │ 3. Write Arrow IPC → staging/<table>/      │  │  │
        │  │  │    fetched_<start>_<end>.arrow              │  │  │
        │  │  │ 4. BatchState: dispatched → fetched        │  │  │
        │  │  │    (record actual_end_id)                   │  │  │
        │  │  │ 5. Self-chain: claim + dispatch NEXT       │  │  │
        │  │  │    pending batch for this table             │  │  │
        │  │  │ 6. Dispatch process_batch for THIS batch   │  │  │
        │  │  └────────────────────┬───────────────────────┘  │  │
        │  └───────────────────────┼──────────────────────────┘  │
        │                          │                              │
        │                          ▼                              │
        │  ┌──────────────────────────────────────────────────┐  │
        │  │  STAGE 2: process_batch                           │  │
        │  │           (deid-process-<config_key> queue)       │  │
        │  │  ┌────────────────────────────────────────────┐  │  │
        │  │  │ 1. Read fetched Arrow IPC                  │  │  │
        │  │  │ 2. Reference mapping joins (if configured) │  │  │
        │  │  │    via ReferenceMappingDataFrameJoiner      │  │  │
        │  │  │    (uses join_db or source_db)              │  │  │
        │  │  │ 3. Mapping joins (preloaded or per-batch)  │  │  │
        │  │  │    - encounter_mapping                     │  │  │
        │  │  │    - patient_mapping                       │  │  │
        │  │  │    - reference_pid_mapping                 │  │  │
        │  │  │    - appointment_mapping                   │  │  │
        │  │  │ 4. PatientIdentifierResolver.transform()   │  │  │
        │  │  │    (coalesce mapping columns)               │  │  │
        │  │  │ 5. InvalidRowHandler.handle()              │  │  │
        │  │  │    (filter unresolved → failed_rows.db)    │  │  │
        │  │  │ 6. DeIdentifier.apply_rules()              │  │  │
        │  │  │    ┌──────────────────────────────────┐    │  │  │
        │  │  │    │ PATIENT_ID   → nd_patient_id     │    │  │  │
        │  │  │    │ ENCOUNTER_ID → nd_encounter_id   │    │  │  │
        │  │  │    │ MASK         → <<COLUMN_NAME>>   │    │  │  │
        │  │  │    │ DATE_OFFSET  → shift by N days   │    │  │  │
        │  │  │    │ ZIP_CODE     → <<ZIP_CODE>>      │    │  │  │
        │  │  │    │ PATIENT_DOB  → year only         │    │  │  │
        │  │  │    │ NOTES        → regex PII masking │    │  │  │
        │  │  │    └──────────────────────────────────┘    │  │  │
        │  │  │ 7. Write processed Arrow IPC               │  │  │
        │  │  │ 8. Delete fetched Arrow file               │  │  │
        │  │  │ 9. BatchState: fetched → processed         │  │  │
        │  │  │10. Dispatch write_batch for THIS batch     │  │  │
        │  │  └────────────────────┬───────────────────────┘  │  │
        │  └───────────────────────┼──────────────────────────┘  │
        │                          │                              │
        │                          ▼                              │
        │  ┌──────────────────────────────────────────────────┐  │
        │  │  STAGE 3: write_batch                             │  │
        │  │           (deid-write-<config_key>-<table> queue) │  │
        │  │  ┌────────────────────────────────────────────┐  │  │
        │  │  │ 1. Read processed Arrow IPC + metadata     │  │  │
        │  │  │ 2. CREATE TABLE IF NOT EXISTS (raw DDL)    │  │  │
        │  │  │    - PHI type overrides (ID→BIGINT, etc.)  │  │  │
        │  │  │    - MySQL row-limit auto VARCHAR→LONGTEXT │  │  │
        │  │  │    - SET sql_mode='', innodb_strict=0      │  │  │
        │  │  │ 3. Strip extra join columns                │  │  │
        │  │  │ 4. Empty strings → NULL for numeric cols   │  │  │
        │  │  │ 5. Idempotent: DELETE WHERE id BETWEEN     │  │  │
        │  │  │    actual_start..actual_end + INSERT        │  │  │
        │  │  │    (single transaction)                     │  │  │
        │  │  │ 6. Delete processed Arrow file             │  │  │
        │  │  │ 7. BatchState: processed → done            │  │  │
        │  │  │ 8. If all batches done for table:          │  │  │
        │  │  │    TableState → "completed"                │  │  │
        │  │  └────────────────────────────────────────────┘  │  │
        │  │  Retries: up to 5x with exponential backoff     │  │
        │  │  (10s, 20s, 40s, 80s, 160s) on lock-wait       │  │
        │  └──────────────────────────────────────────────────┘  │
        └───────────────────────────────────────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  Orchestrator poll loop (every 2s)         │
              │  ┌─────────────────────────────────────┐   │
              │  │ Count done batches vs total          │   │
              │  │ If all done → break                  │   │
              │  │ If no progress > stuck_timeout → err │   │
              │  │ Watchdog: detect stalled table chains│   │
              │  │   (pending batches, no in-flight)    │   │
              │  │   → re-dispatch fetch for stalled    │   │
              │  └─────────────────────────────────────┘   │
              └─────────────────────┬─────────────────────┘
                                    │
════════════════════════════════════╪══════════════════════════════════
                         COMPLETION
════════════════════════════════════╪══════════════════════════════════
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  1. Stop LogCollector (drain Redis msgs)   │
              │  2. Write + print text summary             │
              │  3. Update RunLog in state.db              │
              │     status = "completed" | "failed"        │
              │     stats = summary dict                   │
              └─────────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │  4. Terminate all Celery worker processes  │
              │     (cli/run.py:_stop_workers)             │
              │     SIGTERM → wait 10s → SIGKILL fallback  │
              └─────────────────────┬─────────────────────┘
                                    │
                                    ▼
                            Done.
```

## Batch State Machine

```
pending → dispatched → fetched → processed → done
   ▲           │          │          │         ▲
   │           │          │          └─────────┘ (empty at write)
   │           │          └────────────────────┘ (empty at process)
   │           └───────────────────────────────┘ (empty at fetch)
   │
   └─── any stage on failure (reset by task's except block) ──┐
                                                              │
pending ←─────────────── any stage on re-delivery idempotency ┘
```

Each task's `except` block resets the batch to `"pending"` so the watchdog
can re-dispatch it.  Idempotency guards at the top of each `_*_inner()`
function skip the work — or re-dispatch the downstream task — when the
batch is already past the current stage (protects against
`task_acks_late=True` re-delivery after a worker is killed).

## Key Design Points

- **Self-chaining fetches**: Each `fetch_batch` claims and dispatches the *next* pending batch for the same table, forming a sequential chain per table (ensures keyset pagination order).
- **Per-table write queues**: Each table gets its own `deid-write-<config_key>-<table>` queue with concurrency=1, preventing MySQL lock-wait timeouts from concurrent inserts on the same table.
- **Atomic batch claiming**: Uses guarded `UPDATE ... WHERE status='pending'` to prevent two workers from dispatching the same batch.
- **Crash recovery**: On restart, the orchestrator resumes `fetched` batches to process and `processed` batches to write; the watchdog re-dispatches stalled table chains.
- **Mid-run re-delivery**: If a worker is killed mid-task, Celery re-delivers the message. The idempotency guards skip already-completed work; if the batch is in `"fetched"` status, `fetch_batch` re-dispatches `process_batch` to recover from a lost dispatch.
- **Failure handling**: All non-retry failures reset the batch to `"pending"`; the watchdog re-dispatches it. The `stuck_timeout = workers.task_timeout * 2` safety net terminates the run if nothing progresses.
- **Arrow IPC staging**: Data flows through disk files (not Redis) between stages, with column schema metadata embedded in the Arrow file for dest table creation.
- **State engine caching**: All tasks call `get_cached_state_engine(db_path)` from `deid/models/base.py`. The engine is created and state tables are initialised on first use per worker process, then reused for every subsequent batch update — eliminating the overhead of create/dispose cycles in the hot path.
- **Exact row counts**: Setup uses `get_exact_row_count()` (`SELECT COUNT(*)`) rather than catalog estimates, so `BatchState` rows cover every row in the source table.
