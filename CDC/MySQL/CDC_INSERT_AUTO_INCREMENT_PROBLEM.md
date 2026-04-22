# CDC Restore: INSERT Statement & AUTO_INCREMENT Mismatch Problem

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CDC Pipeline Flow                            │
│                                                                     │
│  Client Prod DB                                                     │
│  ┌──────────────┐    Binary log     ┌─────────────┐                │
│  │  mobiledoc   │ ──── files ──────▶│ cdc_parser  │                │
│  │  (live prod) │                   │    .py      │                │
│  └──────────────┘                   └──────┬──────┘                │
│         │                                  │ SQL statements         │
│     mysqldump                              ▼                        │
│    (once, at T)                    ┌─────────────┐                 │
│         │                          │ cdc.change  │                 │
│         ▼                          │    _log     │                 │
│  ┌──────────────┐                  └──────┬──────┘                 │
│  │  mobiledoc   │                         │                        │
│  │  (local dump)│◀────────────────────────┤ cdc_restore.py         │
│  └──────────────┘   fallback lookups      │ (reads + replays)      │
│                                           ▼                        │
│                                   ┌─────────────┐                 │
│                                   │  mobiledoc  │                 │
│                                   │   _staging  │                 │
│                                   │  (delta DB) │                 │
│                                   └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Key components

| Component | Description |
|---|---|
| `mobiledoc` | Local copy of prod — full `mysqldump` taken once at time T |
| `mobiledoc_staging` | **Empty schema** (DDL only, no data). Reset on every pipeline run. CDC events are replayed here to build delta data. |
| `cdc.change_log` | Parsed SQL statements from binary log files |
| `cdc_parser.py` | Reads binary log files via `mysqlbinlog`, writes SQL to `cdc.change_log` |
| `cdc_restore.py` | Reads `cdc.change_log`, replays events into `mobiledoc_staging` |

### Pipeline constraints (real-world)

- **No access to live prod DB** — client only shares binary log files
- **Dump took ~30 minutes** — started at 5:00 PM IST, completed at 5:30 PM IST
- **No `SHOW MASTER STATUS` rights** — cannot capture exact binlog position at dump time
- **Statement-based binary logging** — binary log records SQL statements, not full row images
- **Binary log covers full day** — midnight-to-midnight, not just post-dump events

---

## 2. The Problem: AUTO_INCREMENT Mismatch on INSERT Replay

### 2.1 Why the AUTO_INCREMENT value in the DDL is unreliable

When `mysqldump --single-transaction` runs:

- The **data snapshot** is consistent as of the transaction start (~5:00 PM)
- The **`AUTO_INCREMENT` value** in the `CREATE TABLE` DDL is captured at the moment each table's schema is dumped — which could be anywhere between 5:00 PM and 5:30 PM, while prod is actively inserting rows

```
5:00 PM  ─── dump starts ────────────── 5:30 PM
              ↑ data snapshot here
                                enc table schema dumped at 5:20 PM
                                → AUTO_INCREMENT=13,963,957 in DDL
                                  (but data only goes up to 13,963,756)
                                  ↑ 200-row gap, unknown per table
```

So `mobiledoc_staging` starts with:
- **Data**: rows up to encounterID = 13,963,756 (5:00 PM snapshot) — actually NO DATA, since staging is empty
- **DDL AUTO_INCREMENT**: 13,963,957 (captured at ~5:20 PM during dump)

The **gap between the last data PK and the DDL AUTO_INCREMENT is unknown** and varies per table.

### 2.2 What happens when the binary log is replayed

The 3rd March binary log covers midnight → midnight.

At midnight, prod's `enc` AUTO_INCREMENT was ~13,900,001.

**Phase 1: Binary log events from midnight → 5:00 PM**

These rows ARE in the `mobiledoc` dump (the data snapshot covers them).

```
Binary log event (2:00 AM):
  INSERT into enc Set vmid='abc123', doctorID=123 ...
  → Prod assigned:   encounterID = 13,900,001
  → Staging assigns: encounterID = 13,963,957   ← DDL start value, WRONG
```

Since `mobiledoc_staging` is empty, this INSERT succeeds — no UNIQUE key conflict yet.
The row lands in staging with a **completely wrong `encounterID`**.

When the corresponding UPDATE arrives later:

```
UPDATE enc SET status='arrived' WHERE encounterID=13900001
  → Check staging:   encounterID=13900001 not found
  → Fallback to mobiledoc: found (it was in the 5 PM dump)
  → INSERT from mobiledoc (encounterID=13900001) into staging
  → Apply UPDATE
  → ✓ correct row now in staging

RESULT: staging now has TWO rows for this encounter:
  ┌────────────────┬───────┬─────────────────────────────┐
  │ encounterID    │ vmid  │ source                      │
  ├────────────────┼───────┼─────────────────────────────┤
  │ 13,963,957     │ abc123│ binary log INSERT (wrong ID)│ ← orphan
  │ 13,900,001     │ abc123│ mobiledoc fallback (correct)│ ← correct
  └────────────────┴───────┴─────────────────────────────┘
```

This is the **duplicate** the pipeline produces — not a duplicate key error, but two rows representing the same real encounter.

**Phase 2: Binary log events from 5:30 PM → midnight**

These rows are **NOT** in the `mobiledoc` dump. They are genuinely new.

```
Binary log event (6:00 PM):
  INSERT into enc Set vmid='xyz789', doctorID=123 ...
  → Prod assigned:   encounterID = 13,963,957
  → Staging assigns: encounterID = 13,963,957 + N   ← off by N (burned IDs from Phase 1)

Later the same day:
  UPDATE enc SET status='checked' WHERE encounterID=13963957
  → Check staging:    encounterID=13963957 not found (staging has it as 13963957+N)
  → Fallback to mobiledoc: NOT FOUND (post-dump row)
  → errors_update_prod logged, UPDATE is dropped
  → Staging has the INSERT state only, not the final updated state
```

### 2.3 Summary of failure modes

| Scenario | What happens | Result |
|---|---|---|
| Pre-dump INSERT + later UPDATE | INSERT lands with wrong PK → UPDATE uses mobiledoc fallback → **duplicate row in staging** | Orphan row pollutes staging |
| Pre-dump INSERT + no UPDATE in CDC window | INSERT lands with wrong PK, never corrected | Wrong PK row stays, no fallback triggered |
| Post-dump INSERT + no UPDATE | INSERT into staging (wrong PK but row captured) | Partial data, wrong PK |
| Post-dump INSERT + later UPDATE (same day) | INSERT wrong PK → UPDATE fails (not in staging, not in mobiledoc) → **UPDATE dropped** | INSERT state only, final state lost |
| Any INSERT on table with UNIQUE secondary key (e.g. `enc.vmid`) | If duplicate replayed, `INSERT IGNORE` silently drops; burns AUTO_INCREMENT counter | Silent data loss |

---

## 3. Root Causes

### Root Cause 1: Statement-based binary logging omits the AUTO_INCREMENT PK

MySQL statement-based binary logging records the **SQL statement as written**. If the application does:

```sql
INSERT into enc Set doctorID=123, patientID=456, ...
```

The `encounterID` (AUTO_INCREMENT column) is **never in the statement** — MySQL assigns it at execution time and does not log it. This is by design in statement-based logging.

### Root Cause 2: 30-minute dump window creates unknown AUTO_INCREMENT gap

Because `mysqldump` runs for 30 minutes while prod is active:

- The data snapshot is consistent to ~5:00 PM
- The `AUTO_INCREMENT` value in the DDL can be from any point in the 5:00–5:30 PM window
- The gap between `MAX(pk)` in the data and `AUTO_INCREMENT` in the DDL is **unknown and varies per table**

No amount of careful timestamp-based replay start adjustment can fix this without knowing the exact gap.

### Root Cause 3: No binlog position captured at dump time

Without a binlog file + position tied to the dump, there is no reliable marker to say "start replaying from here and the AUTO_INCREMENT counters will be aligned."

---

## 4. All Possible Solutions

### Solution A: Switch to ROW-based binary logging ⭐ Most Reliable

**What it is**: MySQL's ROW-based binary logging captures the **full before/after row image** for every DML operation, including AUTO_INCREMENT values.

**Client config change** (`my.cnf`):
```ini
binlog_format           = ROW
binlog_row_image        = FULL    # capture all columns, not just changed ones
```

**What `mysqlbinlog --verbose` produces with ROW format**:
```
### INSERT INTO `mobiledoc`.`enc`
### SET
###   @1=13963757 /* INT meta=0 nullable=0 is_null=0 */   ← encounterID is here!
###   @2=626075   /* INT meta=0 nullable=0 is_null=0 */
###   @3=575090   /* INT meta=0 nullable=0 is_null=0 */
...
```

The prod `encounterID` is **directly in the log**. No alignment problem. No guessing.

**Code changes needed**:
- `cdc_parser.py`: The parser already reads `### @N=value` lines (lines 428–431), but stores them as positional `{"1": "13963757 /* INT ... */", ...}`. Need to:
  1. Strip the `/* ... */` metadata comments from values
  2. Map positional `@N` → column names using `INFORMATION_SCHEMA.COLUMNS` ordered by `ORDINAL_POSITION`
  3. Reconstruct a proper `{"raw_sql": "INSERT INTO enc SET encounterID=13963757, ..."}` or handle a new format

- `cdc_restore.py`: Currently crashes on row-based events because `json.loads(row[3])["raw_sql"]` raises `KeyError` when the stored JSON is positional. Need to handle both formats.

**Pros**:
- Eliminates the PK mismatch problem permanently
- All columns captured (not just the 28 out of 90 that `INSERT ... SET` includes)
- Works regardless of dump timing
- No permissions change needed beyond `my.cnf` edit

**Cons**:
- Requires client to change MySQL config and restart (brief maintenance window)
- ROW-based logs are larger than statement-based (mitigated by `binlog_row_image=FULL` being the default)
- Parser and restore code need updates to handle the new format

---

### Solution B: `mysqldump --master-data=2` flag

**What it is**: Adds the exact binlog file and position at the dump's consistent-snapshot point as a comment in the dump file header — no extra queries, no extra permissions beyond what the dump already uses.

**Client adds one flag**:
```bash
mysqldump --single-transaction --master-data=2 mobiledoc > dump.sql
```

**What it adds to the dump file**:
```sql
-- CHANGE MASTER TO MASTER_LOG_FILE='binarylogs.000123', MASTER_LOG_POS=12345678;
```

**How to use it**:
```python
# Parse from dump file header
import re

def extract_master_data(dump_file_path):
    with open(dump_file_path, 'r') as f:
        for line in f:
            m = re.search(r"MASTER_LOG_FILE='([^']+)', MASTER_LOG_POS=(\d+)", line)
            if m:
                return m.group(1), int(m.group(2))
    return None, None

binlog_file, binlog_pos = extract_master_data('dump.sql')
# Pass binlog_pos to mysqlbinlog: --start-position=12345678
```

**Then in the parser**:
```bash
mysqlbinlog --start-position=12345678 binarylogs.000123
```

**Why this aligns AUTO_INCREMENT**:
- The dump's data snapshot is at position P
- Binary log replay starts from exactly position P
- `mobiledoc_staging` DDL `AUTO_INCREMENT` reflects prod's counter at position P (because both the dump and the counter are captured at the same transaction snapshot point when using `--master-data`)
- First INSERT in replay → staging assigns same ID as prod did at position P+1

**Requires**: `REPLICATION CLIENT` privilege (less than `SUPER`; standard for any replication or CDC setup). The client does not need to allow you to query their live prod — this is embedded in the dump file they already share.

**Pros**:
- No binary log format change needed
- Exact alignment with zero gap
- Embedded in the dump file the client already provides
- Works with existing parser/restore code (just change `--start-datetime` to `--start-position`)

**Cons**:
- Requires `REPLICATION CLIENT` privilege for the dump user
- Still only captures 28/90 columns for `INSERT ... SET` (not a complete row image)

---

### Solution C: Skip pre-dump INSERT events; fix orphan rows

**What it is**: Before replaying an INSERT event, check if the row already exists in `mobiledoc` (the local dump). If it does, skip the INSERT — the UPDATE handler will fetch it from `mobiledoc` when the corresponding UPDATE arrives.

**Code change in `cdc_restore.py`**:
```python
elif op == "INSERT":
    fmt = detect_insert_format(sql)
    ...
    columns, values = parse_set_format(sql)   # or parse_values_format

    # --- NEW: check if row already exists in mobiledoc ---
    unique_key_col, unique_key_val = extract_unique_key(row_table, columns, values, unique_keys_by_table)
    if unique_key_col and unique_key_val:
        exists_in_prod = prod_conn.execute(
            text(f"SELECT 1 FROM `{row_table}` WHERE `{unique_key_col}` = :v"),
            {"v": unique_key_val}
        ).scalar()
        if exists_in_prod:
            stats["insert_skipped_pre_dump"] += 1
            continue   # UPDATE handler will fetch from mobiledoc when needed
    # --- END NEW ---

    # proceed with insert for post-dump rows
    ...
```

**Requires**: Pre-building a lookup of unique non-PK keys per table:
```sql
SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'mobiledoc'
  AND CONSTRAINT_NAME != 'PRIMARY'
  AND CONSTRAINT_NAME IN (
    SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    WHERE CONSTRAINT_TYPE = 'UNIQUE'
  );
```

**Pros**:
- Eliminates orphan/duplicate rows for pre-dump INSERTs
- No client-side changes needed
- Works with current statement-based binary logging
- Can be implemented immediately

**Cons**:
- Does NOT fix post-dump INSERT + same-day UPDATE failure (those UPDATEs are still lost)
- Requires building and maintaining unique key metadata for all 8,000 tables
- Tables with no unique secondary key (only AUTO_INCREMENT PK) cannot be checked — orphan rows remain for those
- Still has the partial-data problem for genuinely new post-dump rows

---

### Solution D: Auto-correct staging AUTO_INCREMENT before replay

**What it is**: After resetting `mobiledoc_staging`, query `mobiledoc` for `MAX(pk)` per table and reset the staging AUTO_INCREMENT to `MAX(pk) + 1`, then only replay events after an estimated post-dump cutoff time.

```sql
-- Example for enc
SELECT MAX(encounterID) FROM mobiledoc.enc;   -- returns 13,963,756

ALTER TABLE mobiledoc_staging.enc AUTO_INCREMENT = 13963757;
```

Then replay binary log from dump completion time (5:30 PM) as `--start-datetime`.

**Problem**: The 30-minute gap (5:00–5:30 PM):
- Rows created between 5:00 and 5:30 PM are in `mobiledoc` (data snapshot = 5:00 PM)
- They ARE in `mobiledoc` but their AUTO_INCREMENT IDs were consumed during the dump window
- Starting from 5:30 PM would miss INSERTs from 5:00–5:30 PM for genuinely new rows

**Pros**:
- No client-side changes
- Fixes the DDL AUTO_INCREMENT gap

**Cons**:
- Does not fix the 30-minute data gap (5:00–5:30 PM new rows missed)
- `--start-datetime` has second-level granularity and timezone ambiguity
- For tables dumped early in the window (5:00 PM), the gap is small; for tables dumped late (5:25 PM), rows from 5:00–5:25 PM could be missed

---

## 5. Comparison Matrix

| Solution | Fixes PK mismatch? | Fixes orphan rows? | Fixes post-dump INSERT+UPDATE? | Client change? | Code change? |
|---|---|---|---|---|---|
| **A: ROW-based logging** | ✅ fully | ✅ yes | ✅ yes | ✅ MySQL config | ✅ parser + restore |
| **B: `--master-data=2`** | ✅ fully | ✅ yes | ✅ yes | ✅ dump flag | Minimal (start-pos) |
| **C: Skip pre-dump INSERTs** | ❌ no | ✅ partial | ❌ no | ❌ none | ✅ restore only |
| **D: Reset AUTO_INCREMENT** | ✅ partial | ✅ partial | ❌ 30-min gap | ❌ none | ✅ restore only |

---

## 6. Recommended Solution

### Immediate term: Solution C (no client change required)

While negotiating the long-term fix with the client, apply Solution C to stop producing orphan/duplicate rows. This is safe and improves data quality immediately.

### Long term: Solution B + A together

**Step 1** — Ask client to add `--master-data=2` to their dump command:
```bash
mysqldump --single-transaction --master-data=2 -u ndadmin -p mobiledoc > mobiledoc_YYYYMMDD.sql
```
This requires only `REPLICATION CLIENT` privilege, which is a standard ask for any CDC/replication setup.

**Step 2** — Parse the binlog file and position from the dump header:
```python
# In cdc_automation.py or a new setup step:
binlog_file, binlog_pos = extract_master_data(dump_file_path)
# Store for use by cdc_parser.py
```

**Step 3** — Replace `--start-datetime` with `--start-position` in `cdc_parser.py`:
```python
cmd = ["mysqlbinlog", "--base64-output=DECODE-ROWS", "--verbose",
       f"--start-position={binlog_pos}", binlog_path]
```

**Step 4 (if possible)** — Ask client to switch to ROW-based logging (`binlog_format=ROW`). This eliminates the statement-based limitation where only 28/90 columns are captured in INSERT events, and permanently removes any PK alignment concern.

---

## 7. Quick Reference: Affected Code Locations

| File | Location | Issue |
|---|---|---|
| `cdc_restore.py` | Line 637–683 | INSERT handler — replays without PK, causes mismatch |
| `cdc_restore.py` | Line 323–338 | `strip_auto_increment_from_insert` — intentionally removes PK |
| `cdc_restore.py` | Line 555 | `json.loads(row[3])["raw_sql"]` — crashes on row-based events |
| `cdc_restore.py` | Line 597–615 | UPDATE fallback — correct but creates duplicates when INSERT already landed wrong row |
| `cdc_parser.py` | Line 275 | `--start-datetime` — needs to become `--start-position` |
| `cdc_parser.py` | Line 428–432 | Row-based `@N=value` parsing — stored positionally, not yet usable by restore |
