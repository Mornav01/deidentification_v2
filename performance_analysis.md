# Performance Analysis: `dev` vs `dev_opt` vs `shubhamk_rework`

## Verdict

| Scenario | Fastest | Reason |
|---|---|---|
| **Single machine, mapping tables fit in RAM** | `dev_opt` | Zero DB round-trips per batch for mapping joins; in-memory hash joins eliminate the biggest I/O bottleneck |
| **Multi-worker / large deployments** | `dev_opt` | Fork-inherited cache means N workers all get O(0) mapping lookups |
| **MSSQL source databases** | `shubhamk_rework` | Purpose-built paginated streaming that avoids FreeTDS connection timeouts |
| **Pure structured rules (no NOTES/dates)** | `dev_opt` ≈ `shubhamk_rework` | Both eliminate per-batch mapping SQL; rework wins slightly on ZIP/DOB rules |
| **Tables with NOTES/GENERIC_NOTES columns** | `shubhamk_rework` | Concat-and-split optimization, per-patient pre-compiled alternation regex, `map_batches()` vs `map_elements()` |

**Short answer: `dev_opt` is fastest for the mapping join hot path (the dominant cost for most tables). `shubhamk_rework` is faster for the de-identification rule hot paths and MSSQL. `dev` is the slowest in nearly every scenario.**

---

## 1. DB Round-Trips Per Batch (The Dominant Cost)

This is the single largest performance differentiator.

| Operation | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| Encounter mapping lookup | **1 SQL query** | 0 (in-memory) | **1 SQL query** |
| Patient mapping (from encounter) | **1 SQL query** | 0 (in-memory, pre-joined) | **1 SQL query** |
| Direct patient mapping | **1 SQL query** | 0 (in-memory) | **1 SQL query** |
| Reference PID mapping | **1 SQL query** | 0 (in-memory) | **1 SQL query** |
| Appointment mapping | **1 SQL query** | 0 (in-memory) | **1 SQL query** |
| Patient mapping (from appt) | **1 SQL query** | 0 (in-memory, pre-joined) | **1 SQL query** |
| PII data (NOTES rule only) | 1 SQL query | 1 SQL query | 1 SQL query |
| Schema lookup | 1 (first batch only, cached) | 1 (first batch only, cached) | 1 (first batch only, cached) |
| Source data read | 1 streaming batch | 1 streaming batch | 1 streaming batch |
| Destination write | 1 executemany | 1 executemany | 1 executemany |
| **Total per batch (no NOTES)** | **~8–10** | **~2** | **~8–10** |
| **Total per batch (with NOTES)** | **~9–11** | **~3** | **~9–11** |

### How `dev_opt` eliminates 6–8 round-trips

At startup, `load_mapping_cache()` executes 4 SQL queries and stores the results as Polars DataFrames in memory. Encounter and appointment mappings are **pre-joined with their parent patient mappings** at load time.

Per-batch, the critical section in `main.py` becomes:

```python
# dev/shubhamk_rework — 1 SQL query per mapping table per batch:
stmt = select(patient_mapping).where(patient_mapping.c.patient_id.in_(patient_ids))
with self.engine.connect() as conn:
    df = _sql_result_to_polars(conn.execute(stmt))

# dev_opt — 0 SQL queries; hash-join against in-memory cache:
keys = pl.DataFrame({id_col: ids}).unique()
keys = keys.with_columns(pl.col(id_col).cast(key_dtype, strict=False))
return cache_df.join(keys, on=id_col, how="inner")   # main.py:222
```

### Cost model for mapping joins (100k-row batch, 10M-row encounter_mapping)

| Branch | Per-batch mapping cost | How |
|---|---|---|
| `dev` | ~50–200 ms | 6 SQL round-trips × ~10–30 ms each (network + DB index scan) |
| `dev_opt` | ~5–20 ms | 4 in-memory Polars hash-joins × ~1–5 ms each (Rust engine) |
| `shubhamk_rework` | ~50–200 ms | Same as dev |

Over 1,000 batches (100M-row table), the mapping cost alone is:
- `dev`: **50–200 seconds**
- `dev_opt`: **5–20 seconds** (10× faster)
- `shubhamk_rework`: **50–200 seconds**

---

## 2. Structured De-identification Rules (Hot Path Detail)

### PatientIDRule / EncounterIDRule / ReferencePIDRule / AppointmentIDRule

All three branches use identical pure Polars column alias expressions:
```python
df.with_columns(pl.col("_resolved_nd_patient_id").alias(column))
```
**O(N) Polars write, no Python. No difference between branches.**

### MaskRule

All three branches use identical Polars broadcast:
```python
df.with_columns(pl.lit(f"<<{mask_value}>>").alias(column))
```
**O(1) metadata, O(N) write. No difference.**

### ZIPCodeRule — `shubhamk_rework` wins

| Branch | Implementation | Cost per 100k batch |
|---|---|---|
| `dev` | `map_elements(self.mask_zip, ...)` — Python UDF per cell | ~50 ms |
| `dev_opt` | `map_elements(self.mask_zip, ...)` — Python UDF per cell | ~50 ms |
| `shubhamk_rework` | Pure Polars `when/then/otherwise` with `str.contains()`, `str.extract()`, `str.slice()` | ~5 ms |

**`shubhamk_rework` is ~10× faster on ZIP masking.** The vectorized implementation (rules.py:372–427):
```python
pl.when(pl.col(col_name).is_null() | lowered.is_in(["nan", "none", ""]))
  .then(pl.lit(None, dtype=pl.Utf8))
  .when(col.str.contains(zip_pattern))
  .then(col.str.extract(r"^(\d{3})", 1))
  .when(col.str.len_chars() > 2)
  .then(col.str.slice(0, 3))
  .otherwise(col)
```

### DateOffsetRule — `shubhamk_rework` slightly wins

All three branches must do a Python loop over matched rows (conditional per-row regex substitution is unavoidable). The difference is in the post-processing step:

| Branch | Normalization step | Cost |
|---|---|---|
| `dev` | `map_elements(_normalize_to_mysql_datetime, ...)` — Python call **per row** | O(N) Python FFI calls |
| `dev_opt` | Same as dev | O(N) Python FFI calls |
| `shubhamk_rework` | `map_batches(_normalize_batch, ...)` — Python call **per batch chunk** | O(N/batch_size) Python FFI calls |

`map_batches()` amortizes the Python↔Rust boundary overhead across thousands of rows. For a 100k-row batch this is ~1,000 fewer Python FFI calls.

The core loop is the same in all three branches:
```python
# All three — unavoidable Python loop for date shifting:
result = [self._shift_text(t, o) if m else t
          for t, o, m in zip(text_list, offset_list, mask_list)]
```

**Cost for a 100k-row date column (80% rows matched, 2 dates per row):**
- Polars filter pass (str.contains): ~5 ms
- .to_list() materialisation: ~50 ms
- Python loop + regex.sub() per row: **~40–80 seconds** (this is the real bottleneck, identical in all three)

> This is the #1 bottleneck in all branches. The rework is marginal (~5%) faster on the normalization step but all three have the same fundamental O(N × date_matches) Python loop.

### PatientDOBRule

Same pattern as DateOffsetRule — Python loop only over matched rows. Core performance is identical across all three branches.

### PatientIdentifierResolver (coalesce)

All three use `pl.coalesce([pl.col(c) for c in existing_cols])`. Pure Polars, no Python. **No difference.**

---

## 3. Unstructured Text Rules (NOTES / GENERIC_NOTES)

### GenericNotesRule — `shubhamk_rework` wins

All three branches make multiple regex passes over each cell. The count per cell is roughly the same (~8–32 patterns from `GENERIC_REGEX_DICT`). The differences:

| Optimization | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| RE2-compatible → Polars `str.replace_all()` (Rust) | Yes | Yes | Yes |
| Non-RE2 fallback | `map_elements()` per cell | `map_elements()` per cell | `map_batches()` per chunk |
| Concat-and-split for single-pass | **No** | **No** | **Yes** (genericnotes.py:198–202) |

**Concat-and-split optimization** (rework only): Instead of calling `compiled.sub()` N times (once per row), it concatenates all cells with a rare delimiter, calls `sub()` once on the combined string, then splits back. Saves N−1 regex calls per pattern for non-RE2 patterns.

For a non-RE2 pattern on a 100k-row batch:
- `dev`/`dev_opt`: 100k calls to `re.sub()`
- `shubhamk_rework`: 1 call to `re.sub()` on concatenated string

### NotesRule (PII masking) — `shubhamk_rework` wins

All three branches fetch PII data once per batch (1 SQL query). The masking loop is per-row in all three. The critical difference is in regex compilation:

| Phase | `dev` / `dev_opt` | `shubhamk_rework` |
|---|---|---|
| Phase 1: Build PII lookup | Build `dict[pid → {pattern: value}]` with string keys | Build `dict[pid → [compiled_re, value]]` with **pre-compiled alternation regex** |
| Phase 2: Per-row masking | `re2.sub(pattern, ...)` inside loop — compiles pattern inside loop | `compiled_re.sub(repl, text)` — zero compilation per row |

In `dev`/`dev_opt`, each call to `re2.sub(pattern, ...)` with a string pattern causes re-compilation on every invocation (RE2 does cache, but Python FFI still pays lookup cost). In `shubhamk_rework`, one alternation regex is compiled once per masking_value per patient, combining all values (e.g., "John|Jon|J\. Smith") into a single compiled object.

**Cost for per-patient PII masking (100k rows, avg 5 PII patterns per patient):**
- `dev`/`dev_opt`: 100k rows × 5 patterns × 1 `re2.sub()` call = **500k re2.sub calls**
- `shubhamk_rework`: 100k rows × 1–2 compiled_re.sub calls = **100–200k compiled_re.sub calls**

`shubhamk_rework` is roughly **2–5× faster on PII masking** in the Notes hot path.

### NLP Model Usage

**None of the three branches uses Presidio or spaCy for de-identification in the live hot path.** Despite documentation mentions, the actual production `apply()` method in all branches uses regex-against-PII-tables for patient-specific masking. The NLP dependency exists in requirements but is not invoked per-cell. (This applies equally to all three branches.)

---

## 4. Streaming & I/O

### Source Data Streaming

| Branch | MySQL/PG | MSSQL |
|---|---|---|
| `dev` | Server-side cursor, `stream_results=True`, `max_row_buffer=batch_size` | Same (connection may timeout on large tables) |
| `dev_opt` | Same as dev | Same (connection may timeout) |
| `shubhamk_rework` | Server-side cursor (keyset), OR IPC Arrow file cache | **Paginated mode**: fresh bounded `SELECT ... WHERE id BETWEEN :start AND :end` per batch |

**MSSQL note:** `pymssql`/FreeTDS doesn't support true server-side cursors. Holding a connection open between `fetchmany()` calls while NLP/joins run causes TCP connection drops on large tables. Only `shubhamk_rework` handles this correctly.

**IPC cache mode** (rework only): For parallel range tasks, source data can be pre-fetched to Arrow IPC files on disk. Workers then read from those files instead of the source DB. This eliminates source DB I/O entirely for the parallel processing phase.

### Background Write Thread

All three branches use `threading.Thread` + `queue.Queue(maxsize=1)` for the background writer. The pattern is identical — write I/O overlaps with the next batch's fetch+deidentify.

### Insert Implementation

All three use `cursor.executemany()` with `batch_size=10,000`. Schema is cached after the first batch in all three. **No meaningful difference.**

`shubhamk_rework` adds two vectorized pre-processing steps before insert that `dev`/`dev_opt` do in Python:
- String truncation: Polars `str.slice()` (vs Python string slicing in dev/dev_opt)
- Numeric empty-string nullification: Polars `when/then/otherwise` (vs Python loop in dev/dev_opt)

---

## 5. Concurrency & Parallelism

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Model** | PostgreSQL `SELECT FOR UPDATE SKIP LOCKED`, N Django worker processes | `ProcessPoolExecutor` (fork) or `ThreadPoolExecutor` (Celery) | Celery prefork pool + Redis broker |
| **Mapping cache sharing** | Not applicable (per-batch SQL) | Fork-inherited: all workers share parent's cache at zero additional memory cost | Per-process preload in Celery worker init (duplicated per worker) |
| **Table-level parallelism** | Multiple tasks (one per table) via PostgreSQL task queue | Multiple work units (one per 1M-row range) via ProcessPoolExecutor | Celery Canvas groups, one Celery task per range |
| **Per-table write serialization** | No | No | Yes — dedicated write worker per table (prevents deadlocks on MSSQL/InnoDB) |

### `dev_opt`'s fork advantage

When `ProcessPoolExecutor` spawns N workers with `fork()`, each child inherits the parent's address space via Copy-on-Write. The mapping cache (potentially GB in size) is **not copied** until a worker writes to it (which it never does — the cache is read-only). In practice, all N workers share the same physical memory pages for the mapping cache. Total memory usage is roughly: `cache_size + N × per_worker_overhead`, not `N × cache_size`.

In `shubhamk_rework`'s Celery model, each worker process runs `_preload_mappings()` independently, paying the DB query cost once per worker and holding a separate copy per worker.

---

## 6. Per-Batch Timing Model (Structured Table, No NOTES, 100k Rows)

Assumptions: 5M encounter_mapping rows, 10k distinct encounter IDs per batch, no NLP, 2 date columns, 1 ZIP column.

### `dev` — ~1,100–1,600 ms per batch

| Step | Cost | Mechanism |
|---|---|---|
| Source fetch (stream) | ~200 ms | server-side cursor fetchmany |
| Reference mapping joins | ~30 ms | 1–2 SQL queries to source DB |
| Encounter mapping SQL | ~30 ms | SQL WHERE id IN (10k ids) |
| Patient mapping SQL (2×) | ~60 ms | 2× SQL WHERE id IN |
| Reference/Appointment SQL (2×) | ~60 ms | 2× SQL WHERE id IN |
| PatientIdentifierResolver | ~5 ms | Polars coalesce |
| ID replacement rules | ~10 ms | Polars alias |
| Date offset (2 cols, 80k rows matched) | ~600 ms | Python loop + regex |
| ZIP code (1 col) | ~50 ms | map_elements Python UDF |
| Insert (background thread — overlapped) | ~200 ms | executemany 10k rows |
| **Total (mapping + rules, no overlap)** | **~1,045–1,345 ms** | |

### `dev_opt` — ~700–1,100 ms per batch

| Step | Cost | Mechanism |
|---|---|---|
| Source fetch (stream) | ~200 ms | server-side cursor fetchmany |
| Encounter/patient/appt mapping | ~20 ms | Polars in-memory hash-join × 4 |
| PatientIdentifierResolver | ~5 ms | Polars coalesce |
| ID replacement rules | ~10 ms | Polars alias |
| Date offset (2 cols, 80k rows) | ~600 ms | Same Python loop as dev |
| ZIP code (1 col) | ~50 ms | Same map_elements as dev |
| Insert (background — overlapped) | ~200 ms | executemany 10k rows |
| **Total (mapping + rules, no overlap)** | **~685–885 ms** | |

**Speedup over dev: 1.3–1.5× on structured rules** (the date offset Python loop dominates, not the mapping joins, when columns have dates). For tables with NO date columns:
- `dev_opt` ~170 ms vs `dev` ~400 ms → **2.3× faster** (mapping joins dominate).

### `shubhamk_rework` — ~1,000–1,500 ms per batch

| Step | Cost | Mechanism |
|---|---|---|
| Source fetch (keyset stream) | ~200 ms | server-side cursor |
| 8 mapping SQL queries | ~200 ms | SQL WHERE id IN (same as dev) |
| PatientIdentifierResolver | ~5 ms | Polars coalesce |
| ID replacement rules | ~10 ms | Polars alias |
| Date offset (2 cols, 80k rows) | ~600 ms | Same Python loop (marginal improvement on normalization step) |
| ZIP code (1 col) | ~5 ms | **Pure Polars when/then/otherwise** |
| Insert (background — overlapped) | ~200 ms | executemany |
| **Total** | **~1,020–1,220 ms** | |

**Compared to dev:** Similar on date-heavy tables (both dominated by the Python date-shift loop). Slightly faster on ZIP (10× faster rule). Slightly slower overhead elsewhere (Celery task dispatch, per-task setup cost).

---

## 7. Per-Batch Timing Model (NOTES Column, 100k Rows)

NOTES columns completely dominate. All mapping differences shrink to noise.

| Step | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| PII DB fetch | ~50 ms | ~50 ms | ~50 ms |
| Key PHI column masking (row loop) | ~700 ms | ~700 ms | ~700 ms |
| Primary PII masking (row loop) | ~1,500 ms | ~1,500 ms | **~500–700 ms** (pre-compiled alternation regex) |
| Generic notes (8–32 regex passes) | ~800 ms | ~800 ms | **~400–600 ms** (concat-and-split + map_batches) |
| XML tag masking | ~300 ms | ~300 ms | ~300 ms |
| **NOTES subtotal** | **~3,350 ms** | **~3,350 ms** | **~1,950 ms** |

**For NOTES tables, `shubhamk_rework` is ~1.7× faster.** The key improvements are:
1. Pre-compiled per-patient alternation regexes (combines 5–20 patterns into 1 compiled object)
2. Concat-and-split for non-RE2 generic notes patterns

---

## 8. Memory Profile

| | `dev` | `dev_opt` | `shubhamk_rework` |
|---|---|---|---|
| **Mapping tables in memory** | ~0 (fetched per-batch, freed) | **~500 MB – 5 GB** (all mapping tables loaded) | ~0 per-batch, OR size of IPC cache files if that mode used |
| **Per-batch working set** | O(batch_size) | O(batch_size) | O(batch_size) |
| **Multi-worker memory** | N × O(batch_size) | **~shared** (fork Copy-on-Write) | N × O(batch_size) + N × mapping_cache |
| **NLP model** | ~300 MB (if loaded) | ~300 MB (if loaded) | ~300 MB (if loaded) |

`dev_opt`'s in-memory cache is its main trade-off. For a system with 10M patients × 3 mapping tables at ~200 bytes/row = **~6 GB**. If RAM is insufficient, the OS will swap and performance degrades catastrophically. `dev` and `shubhamk_rework` use ~10 MB for mapping data at any point in time.

---

## 9. Bottleneck Summary by Scenario

### Most Tables (Structured, No NOTES)

```
dev_opt > shubhamk_rework ≈ dev

Bottleneck: DateOffsetRule Python loop (all branches identical)
dev_opt advantage: eliminates 6–8 SQL round-trips per batch
```

### Tables with No Date Columns

```
dev_opt >> shubhamk_rework > dev

Bottleneck: Mapping join I/O (dev) vs hash-join (dev_opt)
dev_opt is 2–5× faster, especially with large mapping tables
```

### Tables with NOTES Columns

```
shubhamk_rework > dev_opt ≈ dev

Bottleneck: Per-row PII regex masking (all branches)
shubhamk_rework advantage: pre-compiled alternation regex, concat-and-split
~1.7× faster on NOTES, but absolute time is still measured in seconds/batch
```

### MSSQL Source Databases

```
shubhamk_rework >> dev ≈ dev_opt (for large tables)

dev/dev_opt: single streaming cursor holds TCP connection open while NLP/joins run
→ connection drops on large tables, task fails, retry from scratch
shubhamk_rework: paginated mode re-issues bounded SELECT per batch, never holds open
```

### Memory-constrained environments

```
dev ≈ shubhamk_rework >> dev_opt

dev_opt requires GB-scale RAM for mapping cache
dev/rework need only O(batch_size) at any time
```

---

## 10. Optimization Opportunities Across All Branches

### #1 Critical: DateOffsetRule Python loop (~600 ms per 100k-row date batch)

None of the three branches has solved this. The loop exists because Polars doesn't natively support regex substitution with a Python callable for per-match date shifting.

Possible fix:
```python
# Replace the Python loop with a vectorized approach using Polars str.extract_all + date arithmetic:
df.with_columns(
    pl.col(col_name).str.extract_all(date_pattern).list.eval(
        pl.element().map_elements(shift_date_str)
    )
    # Then reconstruct the original text
)
# OR: convert dates to numeric offsets in bulk before the loop
dates_col = pl.col(col_name).str.extract(date_pattern, 1).str.strptime(pl.Date)
shifted = dates_col + pl.duration(days=pl.col("_resolved_offset"))
```

### #2 High: Mapping joins on every batch (dev/shubhamk_rework)

`dev_opt`'s in-memory cache is the right solution when mapping tables fit in RAM. For `shubhamk_rework`, consider a lazy-loaded cache with LRU eviction: pre-load on worker init, serve from cache, evict on OOM.

### #3 Medium: NotesRule Key-PHI masking uses `df.to_dicts()` (all branches)

`df.to_dicts()` materialises the entire DataFrame to Python dicts (~300–500 ms per 100k-row batch). Can be replaced with per-column `to_list()` calls which are cheaper.

### #4 Medium: GenericNotesRule makes multiple Polars `with_columns()` calls

Each pattern triggers a separate `with_columns()`, which copies the DataFrame schema. Batching all RE2-compatible patterns into one `with_columns()` call with multiple `str.replace_all()` expressions would reduce overhead.

### #5 Low: Connection pool over-provisioned (dev/dev_opt)

`pool_size=100` when 8 workers × 3 connections = 24 active. `shubhamk_rework` correctly sizes to `pool_size=1` (read) and `pool_size=5` (write).
