"""Auto-QC orchestrator — run the whole QC framework against a list of tables and emit CSVs.

This is the single entry point an orchestrator (Airflow DAG per vendor: ecw / greenway / athenaone)
calls. Given a loaded ``DeidConfig`` (connections + qc defaults), a **PHI rules CSV** (the same
``table_name,column_name,data_type,rule`` file ``deid generate-config`` produces), and a list of
tables, it runs all four QC checks per table and writes **two CSVs**:

- ``auto_qc_summary.csv``  — one row per table: pass/fail + headline counts per check.
- ``auto_qc_findings.csv`` — one row per individual finding (failing column, delta issue, leaked
  PHI record, failed gate check).

The four checks (per table unless noted):

1. **Part 1 — structured/unstructured scan** (``DbScanner``): per-column ID/mask/date/ZIP/DOB
   detectors + regex residual-PII + master exact-match on notes. Column roles come from the rules CSV.
2. **Part 2 — mapping & count gate** (``run_part2_checks``): run **once per run** (it is a
   mapping-level gate, not per-table); its config comes from ``cfg.qc.part2``. Skipped if unset.
3. **Part 2 — delta-identity** (``run_delta_identity_qc``): row-level source↔dest diff. The business
   key defaults to the table's PATIENT_ID / ENCOUNTER_ID column from the rules CSV.
4. **Part 3 — master PHI audit** (``run_master_phi_audit``): notes vs the PHI master. Runs only when
   the table has note/content columns; needs ``pii_master_conn_str`` for the master feed.

Each check is wrapped so one failing table/check never aborts the run — it is recorded as
ERROR/SKIPPED in the CSVs. The per-check runner functions are module-level so they can be
stubbed in tests without a live database.
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("deid.qc.auto_qc")

# Rule → semantic role (rules come from deid/config/rules_generator.py + the QC DectorMapping).
_NOTE_RULES = ("NOTES", "GENERIC_NOTES")
_PATIENT_RULES = ("PATIENT_ID", "REFERENCE_PID")
_DATE_RULES = ("DATE_OFFSET", "PATIENT_DOB")

_GATE_TABLE = "(mapping-count-gate)"

SUMMARY_FIELDS = [
    "table", "overall_status",
    "part1_status", "part1_failed_columns", "part1_source_rows", "part1_dest_rows",
    "delta_status", "delta_missing_in_dest", "delta_extra_in_dest", "delta_value_mismatch",
    "master_status", "master_fail_count", "master_coverage_gaps",
    "errors",
]
FINDINGS_FIELDS = ["table", "part", "check", "column", "status", "count", "detail"]


# ── rules CSV → per-table roles ──────────────────────────────────────────────────


@dataclass
class TableRoles:
    """Column roles for one table, derived from the PHI rules CSV."""
    table: str
    columns: dict[str, str] = field(default_factory=dict)      # column -> rule (rule non-empty)
    patient_id_col: Optional[str] = None
    encounter_id_col: Optional[str] = None
    note_cols: list[str] = field(default_factory=list)
    name_cols: list[str] = field(default_factory=list)         # MASK columns (candidate names)
    date_cols: list[str] = field(default_factory=list)


def parse_rules_csv(rules_csv: str, tables: Optional[list[str]] = None) -> dict[str, TableRoles]:
    """Parse the ``table_name,column_name,data_type,rule`` CSV into per-table ``TableRoles``.

    ``tables`` (optional) restricts and orders the output; otherwise every table in the CSV is
    returned in sorted order. The CSV needs ``table_name,column_name,rule``; any extra columns
    (e.g. ``data_type`` from ``generate-config``) are ignored. Rows with a blank ``rule`` are skipped.
    """
    path = Path(rules_csv)
    if not path.exists():
        raise FileNotFoundError(f"PHI rules CSV not found: {rules_csv}")

    by_table: dict[str, TableRoles] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        missing = {"table_name", "column_name", "rule"} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"rules CSV missing columns: {sorted(missing)} (got {reader.fieldnames})")
        for row in reader:
            table = (row.get("table_name") or "").strip()
            # The deid pipeline force-lowercases every dest column (process.py), so match that:
            # lowercase the rules-CSV column names here to stay consistent for mixed-case sources
            # (e.g. MSSQL). Table name is left as-is — the pipeline preserves its case for the dest
            # table, so QC must query it with the same case (matters on case-sensitive MySQL/Linux).
            col = (row.get("column_name") or "").strip().lower()
            rule = (row.get("rule") or "").strip().upper()
            if not table or not col or not rule:
                continue
            roles = by_table.setdefault(table, TableRoles(table=table))
            roles.columns[col] = rule
            if rule in _PATIENT_RULES and roles.patient_id_col is None:
                roles.patient_id_col = col
            elif rule == "ENCOUNTER_ID" and roles.encounter_id_col is None:
                roles.encounter_id_col = col
            # Guard against duplicate CSV rows for the same column (e.g. 'notes' listed twice)
            # so we never emit `SELECT notes, notes` downstream.
            if rule in _NOTE_RULES and col not in roles.note_cols:
                roles.note_cols.append(col)
            if rule == "MASK" and col not in roles.name_cols:
                roles.name_cols.append(col)
            if rule in _DATE_RULES and col not in roles.date_cols:
                roles.date_cols.append(col)

    if tables:
        wanted = [t.strip() for t in tables if t.strip()]
        missing_tables = [t for t in wanted if t not in by_table]
        if missing_tables:
            logger.warning("[auto-qc] tables not found in rules CSV (skipped): %s", missing_tables)
        return {t: by_table[t] for t in wanted if t in by_table}
    return {t: by_table[t] for t in sorted(by_table)}


def build_table_config(roles: TableRoles) -> dict:
    """Build the ``TableDetailsForUI`` dict the Part-1 ``DbScanner`` consumes from ``roles``."""
    from deid.qc.builders import DectorMapping

    columns_details = []
    for col, rule in roles.columns.items():
        if rule not in DectorMapping:
            continue
        columns_details.append({
            "column_name": col,
            "is_phi": True,
            "de_identification_rule": rule,
            "mask_value": col.upper(),
        })
    return {
        "columns_details": columns_details,
        "ignore_rows": {},
        "batch_size": 0,
        "reference_patient_id_column": roles.patient_id_col,
        "reference_enc_id_column": roles.encounter_id_col,
    }


# ── per-check runners (module-level so tests can monkeypatch them) ────────────────


def _build_qc_config(cfg, pii_master_conn_str: Optional[str], residual_backend: str) -> dict:
    mp = dict(getattr(cfg.qc, "master_phi", {}) or {})
    return {
        "residual_pii_backend": residual_backend or "regex",
        "pii_master_conn_str": pii_master_conn_str or mp.get("pii_master_conn_str"),
        "pii_columns": mp.get("pii_columns"),
    }


def _mapping_db_config(cfg) -> dict:
    pat = None
    try:
        pat = cfg.mapping_tables.get("patient")
    except Exception:
        pat = None
    return {
        "connection_str": cfg.mappings_connection_string,
        "patient_identifier_columns": list(getattr(pat, "identifier_columns", []) or []) if pat else [],
    }


def run_part1(cfg, roles: TableRoles, qc_config: dict, mapping_db_config: dict) -> dict:
    """Part-1 structured/unstructured scan for one table."""
    from deid.qc.scanner import DbScanner

    table_config = build_table_config(roles)
    if not table_config["columns_details"]:
        return {"status": "SKIPPED", "reason": "no QC-able columns in rules CSV", "result": None}
    scanner = DbScanner(
        source_connection_string=cfg.source_db.connection_string(),
        dest_connection_string=cfg.destination_db.connection_string(),
        mapping_db_config=mapping_db_config,
        qc_config=qc_config,
    )
    result = scanner.scan_table(roles.table, table_config)
    passed = result["final_qc_result"]["is_qc_passed"]
    return {"status": "PASS" if passed else "FAIL", "result": result}


def run_delta(cfg, roles: TableRoles, delta_after: Optional[str] = None) -> dict:
    """Part-2 delta-identity row-level diff for one table."""
    from deid.qc.delta_identity import DeltaIdentityConfig, run_delta_identity_qc

    d = dict(getattr(cfg.qc, "delta_identity", {}) or {})
    d["tables"] = [roles.table]
    # Row-presence (missing/extra on the stable row id) is the meaningful cross-env check for
    # prod↔de-identified output. The patient/encounter business key is REPLACED by de-identification,
    # so comparing its value would flag every de-identified row as a mismatch. Only compare values
    # when the user explicitly configured a (preserved) biz_key_col in qc.delta_identity; otherwise
    # disable value comparison by clearing the auto-discovery patterns (→ biz_col resolves to None).
    if not d.get("biz_key_col"):
        d["biz_key_patterns"] = []
    if delta_after:
        d["delta_after"] = delta_after
    # qc_results_db_url="" → don't persist to qc_results.db: the CSVs are auto-QC's deliverable, and
    # concurrent SQLite writes under max_workers>1 would risk spurious "database is locked" errors.
    results = run_delta_identity_qc(
        source_conn_str=cfg.source_db.connection_string(),
        dest_conn_str=cfg.destination_db.connection_string(),
        cfg=DeltaIdentityConfig.from_dict(d),
        qc_results_db_url="",
    )
    if not results:
        return {"status": "ERROR", "reason": "delta QC returned no result", "result": None}
    r = results[0]
    return {"status": "PASS" if r["is_qc_passed"] else "FAIL", "result": r}


def run_master(cfg, roles: TableRoles, pii_master_conn_str: Optional[str], residual_backend: str) -> dict:
    """Part-3 master-referenced unstructured audit for one table (only when it has note columns)."""
    from deid.qc.master_phi import MasterPhiConfig, make_pii_loader, run_master_phi_audit

    if not roles.note_cols:
        return {"status": "SKIPPED", "reason": "no note/content columns", "result": None}
    # The join key to the PHI master (pii_data_table.nd_patient_id) is the dest table's
    # patient-reference column — the deid pipeline wrote the ND surrogate into it. Dest tables
    # never carry a literal ``nd_patient_id`` column, so without a patient reference we cannot
    # link a note to its patient's PHI and must skip.
    join_col = roles.patient_id_col
    if not join_col:
        return {"status": "SKIPPED", "reason": "no patient-reference column to join to pii_data_table", "result": None}
    shared = dict(getattr(cfg.qc, "master_phi", {}) or {})
    shared.pop("tables", None)
    conn = pii_master_conn_str or shared.pop("pii_master_conn_str", None)
    content_cols = list(dict.fromkeys(roles.note_cols))  # dedup, preserve order
    merged = {**shared, "dest_table": roles.table, "content_cols": content_cols,
              "nd_patient_id_col": join_col}
    if roles.name_cols and not merged.get("name_columns"):
        merged["name_columns"] = list(dict.fromkeys(roles.name_cols))
    if residual_backend and not merged.get("residual_pii_backend"):
        merged["residual_pii_backend"] = residual_backend
    mcfg = MasterPhiConfig.from_dict(merged)
    loader = make_pii_loader(conn, mcfg.pii_columns) if conn else (lambda ids: {})
    # qc_results_db_url="" → CSVs are the deliverable; avoids concurrent-SQLite contention (see run_delta).
    report = run_master_phi_audit(
        cfg.destination_db.connection_string(), mcfg, loader,
        qc_results_db_url="",
    )
    ok = report["fail_count"] == 0 and report["coverage_gaps"] == 0
    return {"status": "PASS" if ok else "FAIL", "result": report}


def run_gate(cfg) -> dict:
    """Part-2 mapping & count gate — run once per run (mapping-level, not per-table)."""
    from deid.qc.mapping_count import Part2Config, run_part2_checks

    part2 = dict(getattr(cfg.qc, "part2", {}) or {})
    if not part2:
        return {"status": "SKIPPED", "reason": "qc.part2 not configured", "checks": []}
    checks = run_part2_checks(
        source_conn_str=cfg.source_db.connection_string(),
        dest_conn_str=cfg.destination_db.connection_string(),
        mapping_conn_str=cfg.mappings_connection_string,
        cfg=Part2Config.from_dict(part2),
    )
    failed = any(c.get("status") in ("fail", "error") for c in checks)
    return {"status": "FAIL" if failed else "PASS", "checks": checks}


def _safe(fn: Callable, *args, **kwargs) -> dict:
    """Run a check, converting any exception into an ERROR record so the run continues."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # a single check must never abort the whole run
        logger.exception("[auto-qc] check %s errored: %s", getattr(fn, "__name__", fn), exc)
        return {"status": "ERROR", "reason": str(exc), "result": None}


def _existing_dest_tables(cfg) -> Optional[set]:
    """Lowercased names of tables present in the destination DB, or None if it can't be determined.

    Returning None (on any reflection error) means "don't skip anything" — the per-check ``_safe``
    wrappers still guard against a genuinely missing table.
    """
    try:
        from sqlalchemy import create_engine, inspect
        engine = create_engine(cfg.destination_db.connection_string())
        try:
            names = {t.lower() for t in inspect(engine).get_table_names()}
        finally:
            engine.dispose()
        # An empty reflection is ambiguous (truly empty vs. couldn't reflect) — treat as
        # "unknown" and skip nothing, so we never skip every table on a reflection hiccup.
        return names or None
    except Exception as exc:
        logger.warning("[auto-qc] could not list destination tables (%s) — running all", exc)
        return None


def _skipped_table_record(roles: TableRoles, reason: str) -> dict:
    skip = {"status": "SKIPPED", "reason": reason, "result": None}
    return {"roles": roles, "part1": dict(skip), "delta": dict(skip), "master": dict(skip)}


def _qc_one_table(cfg, roles: TableRoles, qc_config: dict, mapping_db_config: dict, *,
                  delta_after: Optional[str], pii_master_conn_str: Optional[str],
                  residual_pii_backend: str) -> dict:
    """Run all per-table checks for one table (each isolated by ``_safe``)."""
    logger.info("[auto-qc] %s: running checks...", roles.table)
    return {
        "roles": roles,
        "part1": _safe(run_part1, cfg, roles, qc_config, mapping_db_config),
        "delta": _safe(run_delta, cfg, roles, delta_after=delta_after),
        "master": _safe(run_master, cfg, roles, pii_master_conn_str, residual_pii_backend),
    }


# ── CSV row assembly (pure — unit-tested directly) ────────────────────────────────


def _overall(statuses: list[str]) -> str:
    if "FAIL" in statuses:
        return "FAIL"
    if all(s == "SKIPPED" for s in statuses):
        return "SKIPPED"
    if "ERROR" in statuses:
        return "PARTIAL" if "PASS" in statuses else "ERROR"
    return "PASS"


def _remarks_str(remarks: dict) -> str:
    parts = []
    for key, val in (remarks or {}).items():
        if isinstance(val, list):
            if val:
                parts.append(f"{key}={val[:5]}")
        elif val:
            parts.append(f"{key}={val}")
    return "; ".join(parts)


def _part_label(roles: Optional[TableRoles], column: str) -> str:
    rule = (roles.columns.get(column) if roles else None) or ""
    return "part1_unstructured" if rule in _NOTE_RULES else "part1_structured"


def build_summary_rows(per_table: dict, gate: dict) -> list[dict]:
    rows = []
    for table, rec in per_table.items():
        p1, d, m = rec["part1"], rec["delta"], rec["master"]
        p1res, dres, mres = p1.get("result") or {}, d.get("result") or {}, m.get("result") or {}
        errs = [
            f"{name}:{check['reason']}"
            for name, check in (("part1", p1), ("delta", d), ("master", m))
            if check["status"] == "ERROR" and check.get("reason")
        ]
        failed_cols = ";".join(
            c for c, r in (p1res.get("ColumnsQCResult", {}) or {}).items() if r.get("failed_count", 0) > 0
        )
        rows.append({
            "table": table,
            "overall_status": _overall([p1["status"], d["status"], m["status"]]),
            "part1_status": p1["status"],
            "part1_failed_columns": failed_cols,
            "part1_source_rows": p1res.get("source_rows_count", ""),
            "part1_dest_rows": p1res.get("dest_rows_count", ""),
            "delta_status": d["status"],
            "delta_missing_in_dest": dres.get("missing_in_dest", ""),
            "delta_extra_in_dest": dres.get("extra_in_dest", ""),
            "delta_value_mismatch": dres.get("value_mismatch", ""),
            "master_status": m["status"],
            "master_fail_count": mres.get("fail_count", ""),
            "master_coverage_gaps": mres.get("coverage_gaps", ""),
            "errors": " | ".join(errs),
        })
    rows.append({
        "table": _GATE_TABLE,
        "overall_status": gate["status"],
        "part1_status": "", "part1_failed_columns": "", "part1_source_rows": "", "part1_dest_rows": "",
        "delta_status": "", "delta_missing_in_dest": "", "delta_extra_in_dest": "", "delta_value_mismatch": "",
        "master_status": "", "master_fail_count": "", "master_coverage_gaps": "",
        "errors": gate.get("reason", ""),
    })
    return rows


def _finding(table, part, check, column, status, count, detail) -> dict:
    return {"table": table, "part": part, "check": check, "column": column,
            "status": status, "count": count, "detail": detail}


def build_findings_rows(per_table: dict, gate: dict) -> list[dict]:
    rows = []
    for table, rec in per_table.items():
        roles = rec.get("roles")

        p1 = rec["part1"]
        res = p1.get("result")
        if p1["status"] == "ERROR":
            rows.append(_finding(table, "part1", "(error)", "", "ERROR", "", p1.get("reason", "")))
        elif res:
            src = res.get("source_rows_count", 0)
            dst = res.get("dest_rows_count", 0)
            ignored = res.get("ignore_rows_count", 0)
            if src != dst + ignored:
                rows.append(_finding(table, "part1_rowcount", "row_count", "", "FAIL", src,
                                     f"source={src} dest={dst} ignored={ignored}"))
            for col, r in (res.get("ColumnsQCResult", {}) or {}).items():
                remarks = r.get("remarks", {}) or {}
                if r.get("failed_count", 0) > 0:
                    # For unstructured notes, lead with the failing row ids so a reviewer can pull them.
                    exact_fail = remarks.get("exact_match_failures") if isinstance(remarks, dict) else None
                    if exact_fail:
                        ids = ", ".join(str(x.get("nd_auto_increment_id")) for x in exact_fail[:50])
                        detail = f"nd_auto_increment_id=[{ids}]"
                    else:
                        detail = _remarks_str(remarks)
                    rows.append(_finding(table, _part_label(roles, col), "phi_detected", col,
                                         "FAIL", r["failed_count"], detail))
                # Residual-regex hits are advisory (never a FAIL) — list the row ids for manual review.
                advisory = remarks.get("residual_advisory") if isinstance(remarks, dict) else None
                if advisory:
                    ids = ", ".join(str(x.get("nd_auto_increment_id")) for x in advisory[:50])
                    rows.append(_finding(table, _part_label(roles, col), "residual_pii_advisory", col,
                                         "ADVISORY", len(advisory), f"nd_auto_increment_id=[{ids}]"))

        d = rec["delta"]
        dr = d.get("result")
        if d["status"] == "ERROR":
            rows.append(_finding(table, "delta_identity", "(error)", "", "ERROR", "", d.get("reason", "")))
        elif dr:
            for issue in ("missing_in_dest", "extra_in_dest", "value_mismatch"):
                n = dr.get(issue, 0)
                if n:
                    rows.append(_finding(table, "delta_identity", issue, dr.get("biz_key_col", ""),
                                         "FAIL", n, (dr.get("reason") or "")[:300]))

        m = rec["master"]
        mr = m.get("result")
        if m["status"] == "ERROR":
            rows.append(_finding(table, "master_phi", "(error)", "", "ERROR", "", m.get("reason", "")))
        elif mr:
            if mr.get("coverage_gaps"):
                rows.append(_finding(table, "master_phi", "coverage_gap", "", "FAIL",
                                     mr["coverage_gaps"], "NULL/empty content where text expected"))
            for fdet in mr.get("failure_detail", []) or []:
                detail = (f"phi_hits={fdet.get('phi_hits')} mask_hits={fdet.get('mask_hits')} "
                          f"surrogate_missing={fdet.get('surrogate_missing')}")
                rows.append(_finding(table, "master_phi", "phi_or_mask", str(fdet.get("nd_patient_id", "")),
                                     "FAIL", "", detail))

    for c in gate.get("checks", []) or []:
        if c.get("status") in ("fail", "error"):
            rows.append(_finding(_GATE_TABLE, "gate", c.get("check_name", ""), str(c.get("entity", "")),
                                 str(c.get("status", "")).upper(), c.get("actual", ""), c.get("details", "")))
    return rows


def _write_csvs(out_dir: str, summary_rows: list[dict], findings_rows: list[dict],
                timestamp: str = "") -> tuple[Path, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    suffix = f"_{timestamp}" if timestamp else ""
    summary_path = out / f"auto_qc_summary{suffix}.csv"
    findings_path = out / f"auto_qc_findings{suffix}.csv"
    for path, fields, data in (
        (summary_path, SUMMARY_FIELDS, summary_rows),
        (findings_path, FINDINGS_FIELDS, findings_rows),
    ):
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
    return summary_path, findings_path


# ── orchestrator ──────────────────────────────────────────────────────────────────


def run_auto_qc(
    cfg,
    rules_csv: str,
    tables: Optional[list[str]] = None,
    *,
    out_dir: str = ".",
    max_workers: int = 1,
    pii_master_conn_str: Optional[str] = None,
    residual_pii_backend: str = "regex",
    delta_after: Optional[str] = None,
    include_gate: bool = True,
    timestamp: str = "",
) -> dict:
    """Run all four QC checks over ``tables`` and write the summary + findings CSVs.

    ``cfg`` is a loaded ``DeidConfig`` (connections + ``qc`` defaults). ``rules_csv`` is the PHI
    rules CSV that supplies each table's column roles. ``max_workers`` > 1 runs the per-table checks
    concurrently in a thread pool (the checks are DB-I/O bound and Polars releases the GIL, matching
    the deid pipeline's ThreadPoolExecutor idiom); each table opens its own DB connections so there
    is no shared-engine contention. Returns the two CSV paths, the in-memory rows, and per-table map.
    """
    roles_by_table = parse_rules_csv(rules_csv, tables)
    if not roles_by_table:
        raise ValueError("No tables to QC — none of the requested tables were found in the rules CSV.")

    qc_config = _build_qc_config(cfg, pii_master_conn_str, residual_pii_backend)
    mapping_db_config = _mapping_db_config(cfg)
    existing = _existing_dest_tables(cfg)

    def _one(table, roles):
        if existing is not None and table.lower() not in existing:
            logger.warning("[auto-qc] %s: not present in destination — skipping", table)
            return _skipped_table_record(roles, "table not present in destination")
        return _qc_one_table(
            cfg, roles, qc_config, mapping_db_config,
            delta_after=delta_after, pii_master_conn_str=pii_master_conn_str,
            residual_pii_backend=residual_pii_backend,
        )

    items = list(roles_by_table.items())
    per_table: dict[str, dict] = {}
    if max_workers and max_workers > 1 and len(items) > 1:
        from concurrent.futures import ThreadPoolExecutor
        workers = min(max_workers, len(items))
        logger.info("[auto-qc] running %d table(s) with max_workers=%d", len(items), workers)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = dict(zip(
                (t for t, _ in items),
                ex.map(lambda kv: _one(kv[0], kv[1]), items),
            ))
        per_table = {t: results[t] for t, _ in items}  # restore input order
    else:
        per_table = {t: _one(t, roles) for t, roles in items}

    gate = _safe(run_gate, cfg) if include_gate else {"status": "SKIPPED", "reason": "gate disabled", "checks": []}

    summary_rows = build_summary_rows(per_table, gate)
    findings_rows = build_findings_rows(per_table, gate)
    summary_path, findings_path = _write_csvs(out_dir, summary_rows, findings_rows, timestamp)

    n_fail = sum(1 for r in summary_rows if r["overall_status"] in ("FAIL", "ERROR", "PARTIAL"))
    logger.info("[auto-qc] complete: %d table(s), %d failing/errored. summary=%s findings=%s",
                len(per_table), n_fail, summary_path, findings_path)
    return {
        "summary_csv": str(summary_path),
        "findings_csv": str(findings_path),
        "summary": summary_rows,
        "findings": findings_rows,
        "per_table": per_table,
        "gate": gate,
    }
