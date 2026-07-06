"""
Identity Column Validator (DataFrame-based)
--------------------------------------------
- Gets table list from LOCAL DB (source of truth for which tables exist locally)
- Loads (id_col + biz_key) data from PROD and LOCAL into pandas DataFrames
- Compares with DataFrame merge to find: missing in local, extra in local, mismatches
- Writes two CSVs: summary (one row per table) + detail (sample diff rows)

Usage:
    Set LOCAL_CONFIG, PROD_CONFIG, DB_PAIRS below, then:
        python dent_id_column_validator.py

Requirements:
    pip install pymysql pandas
"""

import os
import pymysql
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Local DB ──────────────────────────────────────────────────────────────────
LOCAL_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "user":     os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASS", ""),
    "port":     int(os.environ.get("MYSQL_PORT", "3306")),
    "charset":  "utf8mb4",
}

# ── Production DB ─────────────────────────────────────────────────────────────
PROD_CONFIG = {
    "host":            os.environ.get("SRC_DB_HOST", ""),
    "user":            os.environ.get("SRC_DB_USER", ""),
    "password":        os.environ.get("SRC_DB_PASS", ""),
    "port":            int(os.environ.get("SRC_DB_PORT", "3306")),
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    300,   # seconds — prevents (2013) Lost connection on large IN queries
    "write_timeout":   300,
}

# ── DB pairs: (prod_db, local_db)  — set local_db=None to use same name ───────
DB_PAIRS = [
    ("mobiledoc",         "mobiledoc_staging"),
    # ("suven",           None),
    # ("biohaven",        None),
]

# ── Settings ──────────────────────────────────────────────────────────────────
SAMPLE_SIZE   = 10       # max diff rows per issue type written to CSV (0 = all)
COMPARE_LIMIT = 0        # max rows fetched per table per side (0 = all)

# ── nd_extracted_date filter (LOCAL only — prod does not have this column) ─────
# Only validate local rows where nd_extracted_date > this date.
# Set to "" or None to skip the filter entirely.
ND_EXTRACTED_AFTER = ""#"2026-05-15"

# ── Results DB (separate DB for storing validation output tables) ──────────────
RESULTS_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),   # change if results DB is on a different server
    "user":     os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASS", ""),
    "port":     int(os.environ.get("MYSQL_PORT", "3306")),
    "charset":  "utf8mb4",
}
RESULTS_DB    = "to_be_deleted_tables"
DETAIL_TABLE  = "id_validation_detail"
SUMMARY_TABLE = "id_validation_summary"

# ── Tables to validate ─────────────────────────────────────────────────────────
# Add / remove table names here. Leave empty [] to auto-discover all
# AUTO_INCREMENT tables from LOCAL DB.
TABLE_FILTER = [
    'cptcode_base', 'oldrxmain_addlinfo', 'allergies', 'annualnotes',
    'assessment_notes_history', 'billingdata', 'cpt_validcodes',
    'doctors', 'edi_inv_cpt', 'edi_inv_diagnosis', 'edi_invoice',
    'encaddendums', 'encounterdata', 'encounters', 'family',
    'hcpcscode_base', 'hl7labdatadetail', 'hl7labnotes', 'hpi',
    'icd_9', 'icd10cm_desc', 'immunizations', 'inpatientvisit',
    'interactionnotes', 'items', 'labdata', 'lablist', 'labloinccodes',
    'ndclookupenteries', 'notes', 'oldrxdetail', 'oldrxmain',
    'patients', 'procedurespl', 'progressnotes_decryptfinal',
    'properties', 'ptinstruction', 'review', 'rx_medication_alert',
    'social', 'structhpi', 'structsocialhistory', 'surgicalhistory',
    'telenc', 'treatmentnotes', 'users', 'vitalshistory', 'structccmr',
    'edi_dfr_info', 'structured_data', 'labdataex', 'insurance',
    'countrycodes', 'progressnotes','enc'
]

# ── Business-key column patterns (case-insensitive substring match) ────────────
BIZ_KEY_PATTERNS = (
    "patientid",   "patient_id",
    "encounterid", "encounter_id",
    "invoiceid",   "invoice_id",
    "ndid",        "psid",
    "claimid",     "claim_id",
    "visitid",     "visit_id",
    "chartid",     "chart_id",
)


# ── Worker count based on available RAM (min 2, max 4) ───────────────────────

def _worker_count():
    try:
        import psutil
        available_gb = psutil.virtual_memory().available / (1024 ** 3)
        count = int(available_gb / 2.0)   # 1 worker per 2 GB available
    except Exception:
        count = 2
    return max(2, min(4, count))


# ── Connection ────────────────────────────────────────────────────────────────

def connect_server(cfg):
    """Open a connection without pre-selecting a database."""
    return pymysql.connect(
        host=cfg["host"], user=cfg["user"], password=cfg["password"],
        port=cfg["port"], charset=cfg.get("charset", "utf8mb4"), autocommit=True,
        connect_timeout=cfg.get("connect_timeout", 30),
        read_timeout=cfg.get("read_timeout",    120),
        write_timeout=cfg.get("write_timeout",   120),
    )


# ── Schema helpers (all queries use explicit schema — no DATABASE()) ───────────

def get_identity_tables(cur, schema, table_filter=None):
    """
    Returns tables with AUTO_INCREMENT columns from schema.

    If table_filter is a non-empty list, only those tables are returned
    (whether or not they have an AUTO_INCREMENT column — id_col will be
    None for tables where none is found).

    Note: TABLE_ROWS > 0 is intentionally omitted — for InnoDB tables
    information_schema.TABLE_ROWS is an estimate and is often 0 even when
    the table contains rows, which would cause valid tables to be silently
    skipped.
    """
    if table_filter:
        phs = ", ".join(["%s"] * len(table_filter))
        cur.execute(
            "SELECT t.TABLE_NAME, c.COLUMN_NAME "
            "FROM information_schema.tables  t "
            "LEFT JOIN information_schema.columns c "
            "  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA "
            " AND c.TABLE_NAME   = t.TABLE_NAME "
            " AND c.EXTRA LIKE '%%auto_increment%%' "
            "WHERE t.TABLE_SCHEMA = %s "
            "  AND t.TABLE_TYPE   = 'BASE TABLE' "
            f"  AND t.TABLE_NAME IN ({phs}) "
            "ORDER BY t.TABLE_NAME",
            [schema] + list(table_filter),
        )
    else:
        cur.execute(
            "SELECT t.TABLE_NAME, c.COLUMN_NAME "
            "FROM information_schema.tables  t "
            "JOIN information_schema.columns c "
            "  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA "
            " AND c.TABLE_NAME   = t.TABLE_NAME "
            "WHERE t.TABLE_SCHEMA = %s "
            "  AND t.TABLE_TYPE   = 'BASE TABLE' "
            "  AND c.EXTRA LIKE '%%auto_increment%%' "
            "ORDER BY t.TABLE_NAME",
            (schema,),
        )
    return [
        {"table": row[0], "id_col": row[1]}
        for row in cur.fetchall()
    ]


def col_exists(cur, schema, table, col_name):
    """Returns True if col_name exists in schema.table."""
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def get_all_columns(cur, schema, table):
    """Returns all column names for a table, ordered by position."""
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    return [row[0] for row in cur.fetchall()]


def get_biz_key_col(cur, schema, table):
    """
    Returns the first column matching a BIZ_KEY_PATTERNS substring, or None.
    Uses explicit schema so DATABASE() is never relied upon.
    """
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    for (col,) in cur.fetchall():
        for pat in BIZ_KEY_PATTERNS:
            if pat in col.lower():
                return col
    return None


# ── DataFrame loader ──────────────────────────────────────────────────────────

def load_df(conn, schema, table, id_col, biz_col, limit=0, id_filter=None, date_after=None):
    """
    Fetch id_col [+ biz_col] from schema.table into a DataFrame.

    Takes a connection object (not a cursor) so it can ping/reconnect before
    each chunk — prevents (2013) Lost connection on large IN-clause queries.

    - limit      : cap rows when id_filter is None (0 = all)
    - id_filter  : list of id values — fetch only WHERE id_col IN (...), chunked at 500.
                   Pass [] for an empty DataFrame without hitting the DB.
    - date_after : adds WHERE nd_extracted_date > date_after (LOCAL only).
    """
    cols     = [id_col] + ([biz_col] if biz_col else [])
    col_expr = ", ".join(f"`{c}`" for c in cols)

    if id_filter is not None:
        if not id_filter:
            return pd.DataFrame(columns=cols)
        CHUNK  = 500   # small enough to avoid packet-size / timeout drops
        frames = []
        for i in range(0, len(id_filter), CHUNK):
            batch = id_filter[i : i + CHUNK]
            phs   = ", ".join(["%s"] * len(batch))
            conn.ping(reconnect=True)   # re-establish if the server dropped us
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {col_expr} FROM `{schema}`.`{table}` "
                    f"WHERE `{id_col}` IN ({phs}) ORDER BY `{id_col}`",
                    batch,
                )
                frames.append(pd.DataFrame(cur.fetchall(), columns=cols))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)

    # Full local load — optional nd_extracted_date filter
    where  = "WHERE `nd_extracted_date` > %s" if date_after else ""
    params = (date_after,)                     if date_after else ()
    lim    = f"LIMIT {limit}"                  if limit > 0  else ""
    conn.ping(reconnect=True)
    with conn.cursor() as cur:
        # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cur.execute(
            f"SELECT {col_expr} FROM `{schema}`.`{table}` {where} ORDER BY `{id_col}` {lim}",
            params,
        )
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ── DataFrame comparator ──────────────────────────────────────────────────────

def compare_dfs(prod_df, local_df, id_col, biz_col):
    """
    Outer-merge on id_col, then classify each row:
      - left_only  → missing in local
      - right_only → extra in local
      - both + biz_col differs → value mismatch

    Returns dict with DataFrames for each category plus matched count.
    """
    merged = prod_df.merge(
        local_df, on=id_col, how="outer",
        suffixes=("_prod", "_local"), indicator=True,
    )

    missing = merged[merged["_merge"] == "left_only"].drop(columns="_merge").reset_index(drop=True)
    extra   = merged[merged["_merge"] == "right_only"].drop(columns="_merge").reset_index(drop=True)
    both    = merged[merged["_merge"] == "both"].drop(columns="_merge").reset_index(drop=True)

    if biz_col and not both.empty:
        pc = f"{biz_col}_prod"
        lc = f"{biz_col}_local"
        # Use direct pandas equality: 14243118 == 14243118.0 → True (avoids
        # the "14243118" != "14243118.0" false mismatch from astype(str)).
        # Treat (NaN, NaN) pairs as equal.
        both_null  = both[pc].isna() & both[lc].isna()
        vals_equal = (both[pc] == both[lc]) | both_null
        mismatch   = both[~vals_equal].reset_index(drop=True)
        matched    = len(both) - len(mismatch)
    else:
        mismatch = pd.DataFrame(columns=[id_col])
        matched  = len(both)

    return {
        "missing_in_local": missing,
        "extra_in_local":   extra,
        "mismatch":         mismatch,
        "matched":          matched,
        "prod_count":       len(prod_df),
        "local_count":      len(local_df),
    }


# ── Per-table worker (runs in its own thread with dedicated connections) ───────

def _process_one_table(table_name, prod_db, local_db):
    """
    All work for a single table.  Opens its own prod + local connections so
    threads never share a pymysql connection object.

    Returns:
        {"summary": dict, "detail": [dict, ...], "status": "passed"|"failed"|"skipped"}
    """
    tag = f"[{table_name}]"

    try:
        local_conn = connect_server(LOCAL_CONFIG)
        prod_conn  = connect_server(PROD_CONFIG)
    except Exception as exc:
        print(f"  {tag} [SKIP] connection failed — {exc}")
        return {"summary": _make_skipped_row(prod_db, local_db, table_name, "—",
                                             f"connection: {exc}"),
                "detail": [], "status": "skipped"}

    try:
        # ── Discover id_col ───────────────────────────────────────────────────
        local_conn.ping(reconnect=True)
        with local_conn.cursor() as cur:
            tbl_list = get_identity_tables(cur, local_db, table_filter=[table_name])
        tbl    = tbl_list[0] if tbl_list else {"table": table_name, "id_col": None}
        id_col = tbl["id_col"]

        print(f"\n  ┌─ {tag}  {local_db}.{table_name}")

        if not id_col:
            print(f"  │  {tag} [WARN] No AUTO_INCREMENT column — skipping")
            return {"summary": _make_skipped_row(prod_db, local_db, table_name, "—",
                                                 "No AUTO_INCREMENT column"),
                    "detail": [], "status": "skipped"}

        print(f"  │  {tag} id_col={id_col}")

        # ── Metadata (all_cols, biz_col, date column check) ───────────────────
        local_conn.ping(reconnect=True)
        with local_conn.cursor() as meta_cur:
            try:
                all_cols = get_all_columns(meta_cur, local_db, table_name)
            except Exception as exc:
                all_cols = []
                print(f"  │  {tag} columns=(could not fetch — {exc})")

            try:
                biz_col = get_biz_key_col(meta_cur, local_db, table_name)
            except Exception as exc:
                print(f"  │  {tag} [SKIP] biz_key lookup failed — {exc}")
                return {"summary": _make_skipped_row(prod_db, local_db, table_name,
                                                     "—", str(exc)),
                        "detail": [], "status": "skipped"}

            has_date_col = col_exists(meta_cur, local_db, table_name, "nd_extracted_date")

        if biz_col and biz_col.lower() == id_col.lower():
            biz_col = None

        date_filter = ND_EXTRACTED_AFTER if (has_date_col and ND_EXTRACTED_AFTER) else None
        if date_filter:
            print(f"  │  {tag} nd_extracted_date filter : > {date_filter}")

        # ── Load LOCAL ────────────────────────────────────────────────────────
        print(f"  │  {tag} loading LOCAL …")
        try:
            local_df = load_df(local_conn, local_db, table_name, id_col, biz_col,
                               date_after=date_filter)
            print(f"  │  {tag} LOCAL rows={len(local_df):,}")
        except Exception as exc:
            print(f"  │  {tag} [SKIP] LOCAL load failed — {exc}")
            return {"summary": _make_skipped_row(prod_db, local_db, table_name,
                                                 biz_col or "—", f"LOCAL load: {exc}"),
                    "detail": [], "status": "skipped"}

        # ── Load PROD filtered to local IDs ───────────────────────────────────
        local_ids = local_df[id_col].dropna().tolist()
        print(f"  │  {tag} loading PROD (filtered to {len(local_ids):,} IDs) …")
        try:
            prod_df = load_df(prod_conn, prod_db, table_name, id_col, biz_col,
                              id_filter=local_ids)
            print(f"  │  {tag} PROD rows={len(prod_df):,}")
        except Exception as exc:
            print(f"  │  {tag} [SKIP] PROD load failed — {exc}")
            return {"summary": _make_skipped_row(prod_db, local_db, table_name,
                                                 biz_col or "—", f"PROD load: {exc}"),
                    "detail": [], "status": "skipped"}

        # ── Compare ───────────────────────────────────────────────────────────
        diff = compare_dfs(prod_df, local_df, id_col, biz_col)
        has_diff = (
            not diff["missing_in_local"].empty or
            not diff["extra_in_local"].empty   or
            not diff["mismatch"].empty
        )
        status = "FAILED" if has_diff else "PASSED"

        detail_rows = []
        if has_diff:
            print(f"  └─ {tag} FAILED"
                  f"  missing={len(diff['missing_in_local'])}"
                  f"  extra={len(diff['extra_in_local'])}"
                  f"  mismatch={len(diff['mismatch'])}"
                  f"  matched={diff['matched']:,}")
            _collect_detail(detail_rows, diff, prod_db, local_db, table_name, id_col, biz_col)
        else:
            print(f"  └─ {tag} PASSED  matched={diff['matched']:,}")

        summary_row = {
            "prod_db":          prod_db,
            "local_db":         local_db,
            "table":            table_name,
            "id_col":           id_col,
            "biz_col":          biz_col or "—",
            "all_columns":      ", ".join(all_cols),
            "prod_rows":        diff["prod_count"],
            "local_rows":       diff["local_count"],
            "matched":          diff["matched"],
            "missing_in_local": len(diff["missing_in_local"]),
            "extra_in_local":   len(diff["extra_in_local"]),
            "mismatch":         len(diff["mismatch"]),
            "status":           status,
            "note":             f"prod filtered to {len(local_ids):,} local IDs",
        }
        return {"summary": summary_row, "detail": detail_rows,
                "status": status.lower()}

    finally:
        try: local_conn.close()
        except Exception: pass
        try: prod_conn.close()
        except Exception: pass


# ── Core logic ────────────────────────────────────────────────────────────────

def validate():
    workers = _worker_count()

    print(f"\n{'='*65}")
    print(f"  Identity Column Validator")
    print(f"  PROD    : {PROD_CONFIG['host']}")
    print(f"  LOCAL   : {LOCAL_CONFIG['host']}")
    print(f"  Workers : {workers}  (based on available RAM, min 2 max 4)")
    if TABLE_FILTER:
        print(f"  Tables  : {len(TABLE_FILTER)} from TABLE_FILTER input")
    else:
        print(f"  Tables  : auto-discover from LOCAL DB")
    print(f"{'='*65}")

    results_conn = connect_server(RESULTS_CONFIG)

    all_summary   = []
    all_detail    = []
    total_passed  = 0
    total_failed  = 0
    total_skipped = 0

    for prod_db, local_db in DB_PAIRS:
        local_db = local_db or prod_db

        print(f"\n{'─'*65}")
        print(f"  DB pair  PROD={prod_db}  LOCAL={local_db}")
        print(f"{'─'*65}")

        # ── Build deduplicated table list ─────────────────────────────────────
        if TABLE_FILTER:
            seen         = set()
            unique_names = [t for t in TABLE_FILTER
                            if not (seen.add(t) or t in seen - {t})]
            # simpler dedup that preserves order
            seen2, unique_names = set(), []
            for t in TABLE_FILTER:
                if t not in seen2:
                    seen2.add(t)
                    unique_names.append(t)
            print(f"  [DISCOVERY] {len(unique_names)} unique table(s) from TABLE_FILTER")
        else:
            # Auto-discover from LOCAL with a short-lived connection
            disc_conn = connect_server(LOCAL_CONFIG)
            try:
                with disc_conn.cursor() as cur:
                    rows = get_identity_tables(cur, local_db)
                unique_names = [r["table"] for r in rows]
            finally:
                disc_conn.close()
            print(f"  [DISCOVERY] Found {len(unique_names)} AUTO_INCREMENT table(s)"
                  f" in LOCAL/{local_db}")

        if not unique_names:
            print(f"  [WARN] No tables found — check TABLE_FILTER or DB connection")
            continue

        # ── Fan out: one future per table ─────────────────────────────────────
        future_to_table = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for tname in unique_names:
                future = executor.submit(_process_one_table, tname, prod_db, local_db)
                future_to_table[future] = tname

            for future in as_completed(future_to_table):
                tname = future_to_table[future]
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"  [ERROR] {tname} raised unexpected exception: {exc}")
                    all_summary.append(
                        _make_skipped_row(prod_db, local_db, tname, "—", f"exception: {exc}")
                    )
                    total_skipped += 1
                    continue

                if result["summary"]:
                    all_summary.append(result["summary"])
                all_detail.extend(result["detail"])

                s = result["status"]
                if s == "passed":
                    total_passed  += 1
                elif s == "failed":
                    total_failed  += 1
                else:
                    total_skipped += 1

    total = total_passed + total_failed + total_skipped
    print(f"\n{'='*65}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*65}")
    print(f"  Tables checked : {total}")
    print(f"  PASSED         : {total_passed}")
    print(f"  FAILED         : {total_failed}")
    print(f"  SKIPPED        : {total_skipped}")

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _save_csv(all_summary, all_detail)
    _save_db(results_conn, RESULTS_DB, all_summary, all_detail, run_ts)

    results_conn.close()

    return all_summary, all_detail


# ── Detail row builder ────────────────────────────────────────────────────────

def _collect_detail(all_detail, diff, prod_db, local_db, table, id_col, biz_col):
    """Collect ALL diff rows — DB gets every record; CSV applies SAMPLE_SIZE at write time."""
    pc = f"{biz_col}_prod"  if biz_col else None
    lc = f"{biz_col}_local" if biz_col else None

    for _, row in diff["missing_in_local"].iterrows():
        all_detail.append({
            "prod_db":   prod_db,   "local_db":  local_db,
            "table":     table,     "id_col":    id_col,
            "id_value":  row[id_col],
            "biz_col":   biz_col or "—",
            "prod_val":  row[pc] if pc and pc in row.index else "—",
            "local_val": "— MISSING —",
            "issue":     "missing in local",
        })

    for _, row in diff["extra_in_local"].iterrows():
        all_detail.append({
            "prod_db":   prod_db,   "local_db":  local_db,
            "table":     table,     "id_col":    id_col,
            "id_value":  row[id_col],
            "biz_col":   biz_col or "—",
            "prod_val":  "— NOT IN PROD —",
            "local_val": row[lc] if lc and lc in row.index else "—",
            "issue":     "extra in local",
        })

    for _, row in diff["mismatch"].iterrows():
        all_detail.append({
            "prod_db":   prod_db,   "local_db":  local_db,
            "table":     table,     "id_col":    id_col,
            "id_value":  row[id_col],
            "biz_col":   biz_col or "—",
            "prod_val":  row[pc] if pc and pc in row.index else "—",
            "local_val": row[lc] if lc and lc in row.index else "—",
            "issue":     "value mismatch",
        })


def _make_skipped_row(prod_db, local_db, table, biz_col, note):
    return {
        "prod_db":          prod_db,
        "local_db":         local_db,
        "table":            table,
        "id_col":           "—",
        "biz_col":          biz_col,
        "all_columns":      "",
        "prod_rows":        "—",
        "local_rows":       "—",
        "matched":          0,
        "missing_in_local": 0,
        "extra_in_local":   0,
        "mismatch":         0,
        "status":           "SKIPPED",
        "note":             note,
    }



# ── CSV writer ────────────────────────────────────────────────────────────────

def _save_csv(summary_rows, detail_rows):
    ts           = datetime.now().strftime("%Y%m%d_%H%M")
    summary_path = f"id_validation_summary_{ts}.csv"
    detail_path  = f"id_validation_detail_{ts}.csv"

    summary_cols = [
        "prod_db", "local_db", "table", "id_col", "biz_col", "all_columns",
        "prod_rows", "local_rows", "matched",
        "missing_in_local", "extra_in_local", "mismatch",
        "status", "note",
    ]
    pd.DataFrame(summary_rows, columns=summary_cols).to_csv(summary_path, index=False)
    print(f"\n  Summary CSV → {os.path.abspath(summary_path)}")

    # Apply SAMPLE_SIZE per (table, issue) group so the CSV stays manageable
    detail_df = pd.DataFrame(detail_rows)
    if SAMPLE_SIZE > 0 and not detail_df.empty:
        detail_df = (
            detail_df
            .groupby(["table", "issue"], group_keys=False)
            .apply(lambda g: g.head(SAMPLE_SIZE))
            .reset_index(drop=True)
        )
    detail_df.to_csv(detail_path, index=False)
    print(f"  Detail  CSV → {os.path.abspath(detail_path)}"
          + (f"  (capped at {SAMPLE_SIZE}/issue/table)" if SAMPLE_SIZE > 0 else ""))


# ── DB writer ─────────────────────────────────────────────────────────────────

def _ensure_result_tables(conn, db):
    """Create validation result tables in db if they don't exist."""
    with conn.cursor() as cur:
        # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query,python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{db}`.`{SUMMARY_TABLE}` (
                id                INT AUTO_INCREMENT PRIMARY KEY,
                run_ts            DATETIME     NOT NULL,
                prod_db           VARCHAR(100),
                local_db          VARCHAR(100),
                table_name        VARCHAR(100),
                id_col            VARCHAR(100),
                biz_col           VARCHAR(100),
                prod_rows         INT,
                local_rows        INT,
                matched           INT,
                missing_in_local  INT,
                extra_in_local    INT,
                mismatch          INT,
                status            VARCHAR(20),
                note              TEXT,
                INDEX idx_run (run_ts),
                INDEX idx_table (local_db, table_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query,python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{db}`.`{DETAIL_TABLE}` (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                run_ts      DATETIME     NOT NULL,
                prod_db     VARCHAR(100),
                local_db    VARCHAR(100),
                table_name  VARCHAR(100),
                id_col      VARCHAR(100),
                id_value    VARCHAR(255),
                biz_col     VARCHAR(100),
                prod_val    TEXT,
                local_val   TEXT,
                issue       VARCHAR(50),
                fix_status  VARCHAR(20) DEFAULT 'pending',
                INDEX idx_run   (run_ts),
                INDEX idx_table (local_db, table_name),
                INDEX idx_issue (issue),
                INDEX idx_fix   (fix_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    print(f"  Result tables ensured: {db}.{SUMMARY_TABLE}, {db}.{DETAIL_TABLE}")


def _save_db(local_conn, results_db, summary_rows, detail_rows, run_ts):
    """Insert all summary and detail rows into the local results tables."""
    try:
        _ensure_result_tables(local_conn, results_db)
    except Exception as exc:
        print(f"  [WARN] Could not create result tables — {exc}")
        return

    with local_conn.cursor() as cur:
        # Summary rows
        for row in summary_rows:
            try:
                cur.execute(
                    f"INSERT INTO `{results_db}`.`{SUMMARY_TABLE}` "
                    "(run_ts, prod_db, local_db, table_name, id_col, biz_col, "
                    " prod_rows, local_rows, matched, "
                    " missing_in_local, extra_in_local, mismatch, status, note) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        run_ts,
                        row.get("prod_db"),  row.get("local_db"),
                        row.get("table"),    row.get("id_col"),
                        row.get("biz_col"),
                        row.get("prod_rows")  if str(row.get("prod_rows",  "")).isdigit() else None,
                        row.get("local_rows") if str(row.get("local_rows", "")).isdigit() else None,
                        row.get("matched"),
                        row.get("missing_in_local"), row.get("extra_in_local"),
                        row.get("mismatch"),  row.get("status"), row.get("note"),
                    ),
                )
            except Exception as exc:
                print(f"  [WARN] Summary insert failed for {row.get('table')}: {exc}")

        # Detail rows — ALL diff records, both prod_val and local_val present
        for row in detail_rows:
            try:
                cur.execute(
                    f"INSERT INTO `{results_db}`.`{DETAIL_TABLE}` "
                    "(run_ts, prod_db, local_db, table_name, id_col, "
                    " id_value, biz_col, prod_val, local_val, issue) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        run_ts,
                        row.get("prod_db"),  row.get("local_db"),
                        row.get("table"),    row.get("id_col"),
                        str(row.get("id_value", "")),
                        row.get("biz_col"),
                        str(row.get("prod_val",  "")),
                        str(row.get("local_val", "")),
                        row.get("issue"),
                    ),
                )
            except Exception as exc:
                print(f"  [WARN] Detail insert failed for {row.get('table')} "
                      f"id={row.get('id_value')}: {exc}")

    print(f"  DB summary → {results_db}.{SUMMARY_TABLE}  ({len(summary_rows)} row(s))")
    print(f"  DB detail  → {results_db}.{DETAIL_TABLE}  ({len(detail_rows)} row(s))")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    validate()
