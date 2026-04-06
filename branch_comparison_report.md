# Branch Comparison Report: `dev` vs `dev_opt` vs `shubhamk_rework`

## Executive Summary

| Dimension | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Architecture** | Django REST API + custom PostgreSQL task queue | Standalone CLI + Celery (no Django in pipeline) | Pure Python CLI (Typer) + Celery + SQLite |
| **Security** | Critical vulnerabilities | Critical vulnerabilities (same as dev) | Hardened (3-layer read-only, parameterized SQL, Pydantic validation) |
| **Performance** | Baseline (per-batch SQL joins) | Optimized mapping joins (in-memory hash) | Multi-modal streaming (IPC cache, MSSQL pagination, keyset) |
| **Test Coverage** | 0 tests | 0 tests | 174 tests across 25 files |
| **Documentation** | None | 140-line README | 1,500+ line README + CLAUDE.md + config references |
| **Production Readiness** | Low | Medium | High |

---

## 1. Performance — Throughput & Efficiency

### Mapping Join Strategy (Biggest Differentiator)

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Strategy** | Per-batch SQL `WHERE id IN (...)` | In-memory Polars hash join | Per-batch SQL `WHERE id IN (...)` |
| **Complexity** | O(mapping_rows) per batch (DB index scan) | O(mapping_rows + batch_IDs) once, then O(batch_IDs) per batch | O(mapping_rows) per batch (DB index scan) |
| **Memory** | O(batch_size) | O(all_mappings + batch_size) — can be GB+ | O(batch_size) |
| **Best for** | Small mapping tables | Large mapping tables that fit in RAM | Distributed workers with many parallel processes |

**`dev_opt` key optimization** (commit `9a876b4`): Loads all mapping tables (patient, encounter, appointment) upfront into Polars DataFrames, then uses inner hash join per batch instead of SQL round-trips. For a 10M-row mapping table with 1K IDs per batch, this eliminates thousands of DB queries.

**`shubhamk_rework` alternative approach**: Keeps per-batch SQL but compensates with smaller connection pools (`pool_size=1` for reads) to protect the source DB when many workers run in parallel. Adds IPC cache mode where source data is fetched once to Arrow files, then multiple workers read from those files.

### Connection Pooling

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Source pool** | 100 connections | 100 connections | **1 connection** (protects source DB) |
| **Dest pool** | 100 connections | 100 connections | 5 connections |
| **Rationale** | One-size-fits-all | Same | Right-sized for read-only vs write workloads |

### Concurrency & Parallelism

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Task queue** | Custom PostgreSQL `SELECT FOR UPDATE SKIP LOCKED` | ProcessPoolExecutor (fork-based) | Celery prefork pool + Redis broker |
| **Large table splitting** | Optional keyset ranges (1M rows/task) | Optional keyset ranges | Automatic with configurable threshold + parallel_tasks_per_table |
| **Background writer** | Thread + Queue(maxsize=1) | Thread + Queue(maxsize=1) | Thread + Queue(maxsize=1) |
| **MSSQL streaming** | Basic server-side cursor | Basic server-side cursor | **Paginated mode** (workaround for FreeTDS cursor timeout) |
| **IPC caching** | No | No | **Yes** — fetch once to Arrow files, process many |
| **Per-table write workers** | No | No | **Yes** — 1 write worker per table prevents lock contention |

### Batch Sizes

All three branches default to **100K rows for reading** and **10K rows for writing**. `shubhamk_rework` makes batch_size configurable via YAML config; the others use environment variables.

### NLP Model Loading

All three branches lazy-load the spaCy model once per table and reuse it across batches — no difference here.

**Performance Verdict**: `dev_opt` wins for single-machine throughput (in-memory mapping joins eliminate DB round-trips). `shubhamk_rework` wins for distributed/multi-worker scenarios (smaller pools, IPC caching, per-table write serialization, MSSQL-specific optimizations).

---

## 2. Safety & Security

### SQL Injection

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Status** | **CRITICAL** — 42 f-string SQL instances | **CRITICAL** — 40+ f-string SQL instances | **Safe** — parameterized `text()` + `_qi()` identifier quoting |
| **Example** | `f"SELECT ... WHERE table_name = '{table_name}'"` | Same | `text("... WHERE :col BETWEEN :start AND :end")` with param dict |

### Credential Handling

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Hardcoded passwords** | Yes (`ndADMIN%402025` in CDC) | Yes (same) | **No** — requires env var, raises `ValueError` if missing |
| **Django SECRET_KEY** | Hardcoded insecure value | N/A (no Django) | N/A (no Django) |
| **Auth default** | `DISABLE_AUTHENTICATION = True` | N/A | N/A (CLI — OS-level auth) |
| **CORS** | `ALLOWED_HOSTS = ["*"]`, `CORS_ORIGIN_ALLOW_ALL = True` | N/A | N/A (no HTTP) |

### Source Database Read-Only Enforcement

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **DB-level** | **None** | **None** | `SET SESSION TRANSACTION READ ONLY` (MySQL/PG/SQLite/Snowflake) |
| **App-level guard** | **None** | **None** | `before_cursor_execute` event blocks INSERT/UPDATE/DELETE/DROP |
| **Handler assertion** | **None** | **None** | `_assert_writable()` raises `RuntimeError` on write attempt |
| **MSSQL NOLOCK** | No | No | **Yes** — `WITH (NOLOCK)` hints on all SELECT queries |

This is the single most important safety difference. `dev` and `dev_opt` have zero protection against accidentally writing to the source production database. `shubhamk_rework` has three independent layers.

### Input Validation

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Framework** | None (raw dict access) | TypedDict (compile-time only) | **Pydantic v2** (runtime validation + error messages) |
| **Config validation** | None | None | Strict at load time — fails fast on invalid config |
| **Env var validation** | Silently uses defaults | Basic regex substitution | Raises `ValueError` if referenced env var is not set |

### ReDoS (Regex Denial of Service) Prevention

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Strategy** | re2 → stdlib re (2-tier) | re2 → stdlib re (2-tier) | **re2 → regex → stdlib re (3-tier)** |

**Security Verdict**: `shubhamk_rework` is dramatically safer. `dev` and `dev_opt` share critical vulnerabilities: unparameterized SQL, hardcoded credentials, no source DB protection, no input validation, and disabled authentication.

---

## 3. Completeness & Operational Readiness

### Testing

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Test files** | 0 | 0 | **25** |
| **Test functions** | 0 | 0 | **174** |
| **Coverage areas** | — | — | Config, models, tasks, orchestration, QC, CLI, logging, pipeline |

### Error Handling & Recovery

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Task retry** | Custom exponential backoff (4^n, cap 300s) | None visible | Celery retry with exponential backoff (10x2^n, max 5) |
| **Deadlock detection** | Generic `OperationalError` catch | Minimal | **MySQL-specific** (codes 1205, 1213, "deadlock found", "lock wait timeout") |
| **Failed row tracking** | Django ORM → PostgreSQL | Dict in memory (lost on crash) | **SQLite table** with source_db, table, reason, row_data JSON, timestamp |
| **CLI retry command** | No | No | **`deid retry`** — replays failed batches from JSONL |
| **Graceful shutdown** | Django implicit | None | **SIGTERM → 10s wait → SIGKILL** per worker subprocess |

### Logging & Observability

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Output** | Console only | Console only | **File + Redis pub/sub + JSON summary** |
| **Structure** | Unstructured text | Unstructured text | **Structured** (LogRecord with table, batch, timing, memory, error) |
| **Real-time progress** | PostgreSQL polling | None | **Redis pub/sub → LogCollector** |
| **Peak memory tracking** | No | No | **Yes** (per-task) |

### Configuration

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Format** | Django settings + PostgreSQL JSON fields | JSON + CSV + env substitution | **YAML + Pydantic v2** |
| **Validation** | None | None (TypedDict = compile-time only) | **Runtime** (Pydantic BaseModel, model_validators) |
| **Config examples** | None | config.example.json | **Full YAML examples + config-reference.md** |
| **`$ref` / includes** | No | Yes (JSON $ref) | Yes (YAML anchors + env interpolation) |

### CLI / API Surface

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Interface** | REST API (9+ endpoints, requires Django server) | CLI (5 basic commands) | **CLI (8+ commands via Typer)** |
| **Commands** | — | run, run-async, run-full, status, status-fast | **run, status, retry, cdc, decrypt-notes, generate-config, mapping, pii-table** |
| **Worker management** | Manual (screen sessions via start_workers.sh) | Manual | **Automatic** — `deid run` spawns and manages worker subprocesses |

### QC (Quality Control)

All three branches have QC scanners with structured/unstructured detectors. `shubhamk_rework` enhances this with Celery task integration, Pydantic validation, and automatic QC as a pipeline phase.

### Documentation

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **README** | None | 140 lines | **1,500+ lines** |
| **CLAUDE.md** | No | No | **Yes** (140 lines — architecture guide for AI) |
| **Config reference** | No | No | **Yes** (config-reference.md, pii-config-reference.md) |
| **Migration guide** | No | No | **Yes** (Django → pure Python mapping table) |

---

## 4. Architecture Comparison

```
dev:              User → Django REST API → PostgreSQL Task Queue → Custom Workers → Source DB → Rules → Dest DB
dev_opt:          User → CLI/Celery      → Redis/ProcessPool    → Workers        → Source DB → Rules → Dest DB
shubhamk_rework:  User → Typer CLI       → Celery + Redis       → Prefork Pool   → Source DB → Rules → Dest DB
                         ↓                                                            ↑
                    Pydantic validation                                          Read-only enforced
                    YAML config                                                  3 streaming modes
                    Auto worker mgmt                                             Per-table write workers
                    Graceful shutdown                                             Failed rows → SQLite audit
```

**`dev`** is a traditional Django monolith — good for web UI integration but carries Django overhead and web-security burden (CORS, CSRF, auth).

**`dev_opt`** removes Django from the processing path and adds in-memory mapping cache — a pure performance optimization on top of `dev`.

**`shubhamk_rework`** is a ground-up rearchitecture: no Django, strict validation, comprehensive error handling, structured logging, and operational tooling (retry, status, generate-config).

---

## 5. De-Identification Rules

All three branches implement the same 11 rule types:

| Rule | All Branches |
|---|---|
| PATIENT_ID | Replace with anonymized nd_patient_id |
| ENCOUNTER_ID | Replace with nd_encounter_id |
| REFERENCE_PID | Resolve indirect → nd_patient_id |
| APPOINTMENT_ID | Replace with nd_appointment_id |
| MASK | Replace with `<<placeholder>>` |
| DATE_OFFSET | Per-patient random date shift |
| STATIC_OFFSET | Global fixed date shift |
| ZIP_CODE | Truncate to 3-digit prefix |
| PATIENT_DOB | Extract year only |
| NOTES | NLP + PII table + patient-specific matching |
| GENERIC_NOTES | Regex-only pattern matching |

The core de-identification logic is functionally equivalent across branches. The differences are in the surrounding infrastructure.

---

## 6. Summary: Feature Completeness Matrix

| Feature | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **SQL Injection Protection** | No (42 f-string instances) | No (40+ f-string instances) | Yes (parameterized queries) |
| **Hardcoded Credentials** | Yes | Yes | No (env-required, fail-fast) |
| **Source DB Read-Only** | No | No | Yes (3-layer enforcement) |
| **Input Validation** | None | None (TypedDict) | Pydantic v2 (runtime) |
| **ReDoS Prevention** | 2-tier fallback | 2-tier fallback | 3-tier fallback |
| **Authentication** | Disabled by default | N/A | N/A (CLI-based) |
| **Test Files / Functions** | 0 / 0 | 0 / 0 | 25 / 174 |
| **Retry Strategy** | Custom exponential | None | Celery exponential + deadlock-aware |
| **Failed Row Tracking** | PostgreSQL ORM | Dict (in-memory, lost on crash) | SQLite (persistent, indexed, auditable) |
| **Logging** | Console, unstructured | Console, unstructured | File + Redis pub/sub, structured |
| **Config Validation** | None | None | Pydantic v2 (strict, fail-fast) |
| **Config Format** | Django settings | JSON + CSV | YAML + Pydantic |
| **CLI Commands** | N/A (REST only) | 5 basic | 8+ comprehensive |
| **Worker Management** | Manual (screen) | Manual | Automatic (subprocess) |
| **Graceful Shutdown** | Django implicit | None | SIGTERM → SIGKILL (10s+5s) |
| **Deadlock Handling** | Generic OperationalError | Minimal | MySQL-specific (codes 1205, 1213) |
| **Per-Table Write Workers** | No | No | Yes (prevents lock contention) |
| **MSSQL Pagination** | No | No | Yes (FreeTDS workaround) |
| **IPC Cache Streaming** | No | No | Yes (fetch-once, process-many) |
| **QC Scanner** | Yes | Yes | Yes (enhanced, Celery-integrated) |
| **Documentation** | None | 140 lines | 1,500+ lines + CLAUDE.md |
| **Mapping Join Optimization** | Per-batch SQL | In-memory hash join | Per-batch SQL |
| **Memory Efficiency** | O(batch_size) | O(all_mappings + batch) | O(batch_size) |

---

## Recommendations

1. **For production deployment**: `shubhamk_rework` is the clear choice — it has 3-layer source DB protection, parameterized SQL, 174 tests, structured logging, graceful shutdown, and operational CLI tools.

2. **If staying on `dev`/`dev_opt`**: Immediately address:
   - Replace all f-string SQL with parameterized queries
   - Remove hardcoded passwords
   - Add source DB read-only enforcement
   - Enable authentication (`DISABLE_AUTHENTICATION=false`)
   - Restrict CORS origins
   - Add test coverage

3. **Performance trade-off**: If mapping table joins are the bottleneck, `dev_opt`'s in-memory hash join approach could be ported into `shubhamk_rework` as an optional mode (load mapping cache when RAM allows, fall back to SQL when it doesn't).

4. **MSSQL deployments**: Only `shubhamk_rework` handles FreeTDS cursor timeout issues via paginated streaming — critical for large MSSQL source databases.
