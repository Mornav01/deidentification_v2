"""Part 3 — Unstructured Data Audit (post-pipeline, master-referenced).

Implements the QC Framework's Part 3. **Key principle:** reference the PHI master table directly —
do not trust pipeline logic as a proxy for correctness. For each de-identified unstructured record we
independently verify, against the master's known PHI values, that:

- **PHI presence scan** — no raw PHI entity (name, dob, address, phone, etc.) appears in the text.
  Exact match is case-insensitive; names also match on parts (first / last / combinations).
- **Masking outcome verification** — no regex-matchable phone, no 5-digit ZIP, no URL, no facility
  name remains; and (optionally) the de-identified surrogate id is present.

The scan functions are pure (text + values in, hits out) so they are unit-testable without a DB.
``run_master_phi_audit`` wires them to a sampled dest table + a master-PHI loader.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

from sqlalchemy import text

from deid.core.dbPkg.dbhandler import NDDBHandler

logger = logging.getLogger("deid.qc.master_phi")

# Masking-outcome regexes (doc Part 3 §3).
_PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
_ZIP5_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
_URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
# Residual full street address: number + 1-3 words + street-type suffix (doc Part 3 §3 Address/ZIP).
_ADDRESS_RE = re.compile(
    r"\b\d{1,6}\s+(?:[A-Za-z0-9.]+\s+){0,3}"
    r"(?:street|st|avenue|ave|road|rd|boulevard|blvd|lane|ln|drive|dr|court|ct|way|place|pl|"
    r"terrace|ter|circle|cir|highway|hwy|parkway|pkwy)\b\.?",
    re.IGNORECASE,
)
# Date tokens in free text (ISO YYYY-MM-DD and common M/D/Y). Used by the dates-in-notes check.
_DATE_TOKEN_RE = re.compile(r"\b(\d{4}-\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}/\d{2,4})\b")

# Master columns that are names (partial-match) vs everything else (exact substring).
_NAME_HINTS = ("name", "fname", "lname", "ufname", "ulname", "mname", "middle")
# Master columns that hold dates (used by the dates-in-notes leak check when date_columns unset).
_DATE_HINTS = ("dob", "date", "datetime", "dos", "admit", "discharge", "birth")
_MIN_PART_LEN = 3  # ignore name fragments shorter than this to reduce false positives
_PLAUSIBLE_MIN_YEAR = 1900


@dataclass
class MasterPhiConfig:
    dest_table: str
    content_cols: list[str]
    nd_patient_id_col: str = "nd_patient_id"
    pii_columns: Optional[list[str]] = None      # master cols to scan; None → all non-id cols
    name_columns: Optional[list[str]] = None      # subset treated as names (partial match)
    date_columns: Optional[list[str]] = None      # master date cols; None → auto by name (_DATE_HINTS)
    facility_names: list[str] = field(default_factory=list)
    surrogate_id_col: Optional[str] = None        # if set, assert this surrogate is present in text
    check_phone: bool = True
    check_zip5: bool = True
    check_url: bool = True
    check_address: bool = True                    # residual full street-address regex
    check_dates_in_notes: bool = True             # flag leaked master dates + implausible dates in text
    # Part 3 masking-outcome: assert an expected mask token replaced the PHI. Opt-in (can false-flag
    # notes that never referenced the patient); best when every note is known to mention its patient.
    expected_mask_tokens: list[str] = field(default_factory=list)
    require_mask_token: bool = False
    sample_size: int = 500
    max_failures: int = 200

    @classmethod
    def from_dict(cls, d: dict | None) -> "MasterPhiConfig":
        """Build from a dict, ignoring unknown keys (e.g. shared ``pii_master_conn_str``)."""
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in (d or {}).items() if k in field_names})


# ── pure scan functions ─────────────────────────────────────────────────────────


def _is_name_col(col: str, name_columns: Optional[list[str]]) -> bool:
    if name_columns is not None:
        return col in name_columns
    cl = col.lower()
    return any(h in cl for h in _NAME_HINTS)


def _is_date_col(col: str, date_columns: Optional[list[str]]) -> bool:
    if date_columns is not None:
        return col in date_columns
    cl = col.lower()
    return any(h in cl for h in _DATE_HINTS)


def _parse_date_token(tok: str):
    """Parse a matched date token → datetime, or None. Accepts YYYY-M-D and M/D/YYYY."""
    from datetime import datetime as _dt
    tok = tok.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return _dt.strptime(tok, fmt)
        except ValueError:
            continue
    return None


def scan_phi_presence(text_value: str, phi: dict, name_columns: Optional[list[str]] = None) -> list[dict]:
    """Return hits where a master PHI value appears in ``text_value``.

    ``phi`` = {master_column: value}. Exact values match case-insensitively as substrings; name
    columns additionally match on individual parts (first/last/etc.) of length ≥ _MIN_PART_LEN.
    """
    hits = []
    low = (text_value or "").lower()
    if not low:
        return hits
    for col, val in phi.items():
        if val is None:
            continue
        sval = str(val).strip()
        if not sval:
            continue
        if sval.lower() in low:
            hits.append({"column": col, "phi": sval, "kind": "exact"})
            continue
        if _is_name_col(col, name_columns):
            for part in re.split(r"\s+", sval):
                if len(part) >= _MIN_PART_LEN and part.lower() in low:
                    hits.append({"column": col, "phi": part, "kind": "name_part"})
                    break
    return hits


def scan_masking_outcomes(text_value: str, cfg: MasterPhiConfig) -> list[dict]:
    """Return hits for residual phone / 5-digit ZIP / URL / facility / full-address patterns."""
    hits = []
    txt = text_value or ""
    if cfg.check_phone and _PHONE_RE.search(txt):
        hits.append({"kind": "phone", "match": _PHONE_RE.search(txt).group(0)})
    if cfg.check_zip5 and _ZIP5_RE.search(txt):
        hits.append({"kind": "zip5", "match": _ZIP5_RE.search(txt).group(0)})
    if cfg.check_url and _URL_RE.search(txt):
        hits.append({"kind": "url", "match": _URL_RE.search(txt).group(0)})
    if cfg.check_address and _ADDRESS_RE.search(txt):
        hits.append({"kind": "address", "match": _ADDRESS_RE.search(txt).group(0)})
    low = txt.lower()
    for fac in cfg.facility_names:
        if fac and fac.lower() in low:
            hits.append({"kind": "facility", "match": fac})
    return hits


def scan_dates_in_notes(text_value: str, master_date_values: list, today: datetime) -> list[dict]:
    """Doc Part 3 "Dates in notes" — flag date tokens in text that either leak a raw master date
    or fall outside the plausible window [1900, today].

    We cannot verify the per-date offset without the pre-de-id text, so this checks the two things
    that *are* verifiable post-hoc: a raw master date appearing verbatim, and implausible dates.
    """
    hits = []
    txt = text_value or ""
    if not txt:
        return hits
    master_norm = set()
    for v in master_date_values:
        if v is None:
            continue
        d = _parse_date_token(str(v).split(" ")[0])
        if d is not None:
            master_norm.add(d.date())
    for m in _DATE_TOKEN_RE.finditer(txt):
        tok = m.group(0)
        d = _parse_date_token(tok)
        if d is None:
            continue
        if d.date() in master_norm:
            hits.append({"kind": "date_leak", "match": tok})
        elif d > today or d.year < _PLAUSIBLE_MIN_YEAR:
            hits.append({"kind": "date_implausible", "match": tok})
    return hits


def scan_mask_token(text_value: str, expected_tokens: list[str]) -> bool:
    """True if any expected mask token (e.g. ``<<PATIENT_NAME>>``) appears in the text."""
    low = (text_value or "").lower()
    return any(tok and tok.lower() in low for tok in expected_tokens)


def audit_record(text_value: str, phi: dict, cfg: MasterPhiConfig, today: Optional[datetime] = None) -> dict:
    """Audit one record's text against its master PHI. Returns {passed, phi_hits, mask_hits}.

    ``mask_hits`` aggregates all residual-pattern, dates-in-notes, and (opt-in) mask-token-missing
    findings so the caller only needs the two lists.
    """
    today = today or datetime.now()
    phi_hits = scan_phi_presence(text_value, phi, cfg.name_columns)
    mask_hits = scan_masking_outcomes(text_value, cfg)

    if cfg.check_dates_in_notes:
        date_values = [v for col, v in phi.items() if _is_date_col(col, cfg.date_columns)]
        mask_hits.extend(scan_dates_in_notes(text_value, date_values, today))

    # Opt-in: a name is present in the master and the note is non-empty, so a mask token is expected.
    if cfg.require_mask_token and cfg.expected_mask_tokens and (text_value or "").strip():
        has_name = any(_is_name_col(col, cfg.name_columns) and v for col, v in phi.items())
        if has_name and not scan_mask_token(text_value, cfg.expected_mask_tokens):
            mask_hits.append({"kind": "mask_token_missing", "match": ""})

    return {"passed": not phi_hits and not mask_hits, "phi_hits": phi_hits, "mask_hits": mask_hits}


# ── master-PHI loader ────────────────────────────────────────────────────────────


def make_pii_loader(pii_master_conn_str: str, pii_columns: Optional[list[str]] = None) -> Callable[[list], dict]:
    """Return a loader ``nd_ids -> {nd_patient_id: {col: value}}`` backed by the PII master table.

    Uses ``PIITable`` (the same loader the pipeline's NotesRule uses), so the audit references the
    master independently of pipeline output.
    """
    from deid.core.process_df.unstruct.notes import PIITable

    def _loader(nd_ids: list) -> dict:
        if not nd_ids:
            return {}
        df = PIITable()._get_table("pii_data_table", pii_master_conn_str, nd_ids)
        out: dict = {}
        cols = pii_columns or [c for c in df.columns if not c.lower().endswith("id")]
        for row in df.iter_rows(named=True):
            nd = row.get("nd_patient_id")
            out[nd] = {c: row.get(c) for c in cols if c in row}
        return out

    return _loader


# ── entrypoint ────────────────────────────────────────────────────────────────


def run_master_phi_audit(
    dest_conn_str: str,
    cfg: MasterPhiConfig,
    phi_loader: Callable[[list], dict],
    audit_timestamp: str = "",
    qc_results_db_url: str = "",
) -> dict:
    """Sample de-identified records, load their master PHI, audit each, and return the report.

    ``phi_loader`` maps a list of nd_patient_ids → {nd_patient_id: {col: value}} (see
    ``make_pii_loader``). Injectable so the audit is testable without a live master DB.
    """
    dest = NDDBHandler(dest_conn_str)
    qi = dest._qi
    try:
        select_cols = [cfg.nd_patient_id_col] + list(cfg.content_cols)
        if cfg.surrogate_id_col and cfg.surrogate_id_col not in select_cols:
            select_cols.append(cfg.surrogate_id_col)
        col_expr = ", ".join(qi(c) for c in select_cols)
        limit_sql = "" if cfg.sample_size <= 0 else (
            f" LIMIT {int(cfg.sample_size)}" if dest.engine.dialect.name != "mssql"
            else ""  # MSSQL sampling handled via TOP below
        )
        top_sql = f"TOP {int(cfg.sample_size)} " if (dest.engine.dialect.name == "mssql" and cfg.sample_size > 0) else ""
        sql = f"SELECT {top_sql}{col_expr} FROM {qi(cfg.dest_table)}{limit_sql}"
        with dest.engine.connect() as conn:
            rows = [dict(r._mapping) for r in conn.execute(text(sql))]
    finally:
        dest.close()

    nd_ids = [r[cfg.nd_patient_id_col] for r in rows if r.get(cfg.nd_patient_id_col) is not None]
    phi_by_nd = phi_loader(list(dict.fromkeys(nd_ids)))

    total = len(rows)
    entities_checked = sum(len(v) for v in phi_by_nd.values())
    passed = 0
    failures: list[dict] = []
    coverage_gaps = 0

    for r in rows:
        nd = r.get(cfg.nd_patient_id_col)
        combined = "\n".join(str(r.get(c) or "") for c in cfg.content_cols).strip()
        if not combined:
            coverage_gaps += 1  # NULL/empty text where content was expected
        phi = phi_by_nd.get(nd, {})
        res = audit_record(combined, phi, cfg)

        # Surrogate-present check (doc: MRN/IDs → surrogate substituted).
        surrogate_missing = False
        if cfg.surrogate_id_col:
            surrogate = str(r.get(cfg.surrogate_id_col) or "")
            if surrogate and combined and surrogate not in combined:
                surrogate_missing = True

        if res["passed"] and not surrogate_missing:
            passed += 1
        else:
            if len(failures) < cfg.max_failures:
                failures.append({
                    "nd_patient_id": nd,
                    "phi_hits": res["phi_hits"],
                    "mask_hits": res["mask_hits"],
                    "surrogate_missing": surrogate_missing,
                })

    report = {
        "audit_run_timestamp": audit_timestamp,
        "table_name": cfg.dest_table,
        "total_records_audited": total,
        "phi_entities_checked": entities_checked,
        "pass_count": passed,
        "fail_count": total - passed,
        "coverage_gaps": coverage_gaps,
        "failure_detail": failures,
    }
    logger.info(
        "[Part3] %s: %d records, %d PHI entities, pass=%d fail=%d coverage_gaps=%d",
        cfg.dest_table, total, entities_checked, passed, total - passed, coverage_gaps,
    )
    if qc_results_db_url:
        _persist(qc_results_db_url, report)
    return report


def _persist(db_url: str, report: dict) -> None:
    import json

    from sqlalchemy.orm import Session

    from deid.models.base import create_qc_results_engine, create_all_qc_results_tables
    from deid.models.qc_results import QCUnstructuredAuditResult

    engine = create_qc_results_engine(db_url)
    create_all_qc_results_tables(engine)
    with Session(engine) as session:
        session.add(QCUnstructuredAuditResult(
            table_name=report["table_name"],
            total_records_audited=report["total_records_audited"],
            phi_entities_checked=report["phi_entities_checked"],
            pass_count=report["pass_count"],
            fail_count=report["fail_count"],
            coverage_gaps=report["coverage_gaps"],
            failure_detail=json.dumps(report["failure_detail"], default=str),
        ))
        session.commit()
    engine.dispose()
    logger.info("[Part3] Persisted audit for %s to %s", report["table_name"], db_url)
