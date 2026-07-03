"""Part 2 — Mapping & Count Checks (pre-pipeline, blocking).

Implements the QC Framework's Part 2 suite. These run **before** de-identification and, when
``blocking`` is set, halt the pipeline on any failure (``run_pre_pipeline_gate`` raises
``Part2Blocked``). All checks are config-driven and skipped when their config is absent.

Checks (see ``QC_Framework_Proposition.pdf`` Part 2 + the auto_qc_rule_checklist):
- ``mapping_to_table_count``   — row count in each mapping table == row count in its target table.
- ``patient_encounter_count``  — distinct-encounters-per-patient distribution matches source↔dest.
- ``encounter_row_count``      — per-encounter row-count distribution matches source↔dest, per FK table.
- ``mapping_uniqueness``       — each patient_id → one nd_patient_id; each encounter_id → one nd_encounter_id.
- ``offset_range``             — patient offset within [offset_min, offset_max] (default [-38, 38]).
- ``mapping_id_format``        — nd_patient_id / nd_encounter_id length + prefix on the mapping tables.

Identity-agnostic equivalence: source (prod) and dest (de-identified) use different id values, so the
per-patient / per-encounter checks compare the **sorted multiset of counts** rather than aligning ids.
With a 1:1 mapping these distributions are identical iff no rows were dropped/duplicated.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text

from deid.core.dbPkg.dbhandler import NDDBHandler

logger = logging.getLogger("deid.qc.mapping_count")


class Part2Blocked(Exception):
    """Raised by the pre-pipeline gate when a blocking Part-2 check fails."""

    def __init__(self, failures: list[dict]):
        self.failures = failures
        names = ", ".join(f"{f['check_name']}[{f.get('entity','')}]" for f in failures)
        super().__init__(f"Part 2 QC blocked the pipeline — {len(failures)} failing check(s): {names}")


@dataclass
class Part2Config:
    # A. mapping table ↔ target table row-count equality: [(mapping_table, target_table), ...]
    mapping_target_pairs: list[tuple[str, str]] = field(default_factory=list)
    # B. per-patient distinct-encounter count (source vs dest)
    patient_encounter_table: Optional[str] = None
    patient_col: str = "patientid"
    encounter_col: str = "encounterid"
    # C. per-encounter row counts across FK tables (source vs dest)
    encounter_fk_tables: list[str] = field(default_factory=list)
    encounter_fk_col: str = "encounterid"
    # D. mapping 1:1 uniqueness
    patient_mapping_table: str = "patient_mapping_table"
    encounter_mapping_table: str = "encounter_mapping_table"
    patient_map_src_col: str = "patient_id"
    patient_map_nd_col: str = "nd_patient_id"
    encounter_map_src_col: str = "encounter_id"
    encounter_map_nd_col: str = "nd_encounter_id"
    check_uniqueness: bool = True
    # E. offset range
    offset_col: str = "offset"
    offset_min: int = -38
    offset_max: int = 38
    check_offset_range: bool = True
    # F. mapping id format
    nd_patient_len: Optional[int] = None
    nd_encounter_len: Optional[int] = None
    nd_encounter_prefix: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict | None) -> "Part2Config":
        d = dict(d or {})
        pairs = [tuple(p) for p in d.pop("mapping_target_pairs", [])]
        return cls(mapping_target_pairs=pairs, **d)


def _result(check_name, status, *, entity="", blocking=True, expected="", actual="", delta="", details="") -> dict:
    return {
        "check_name": check_name, "entity": str(entity), "status": status, "blocking": blocking,
        "expected": str(expected), "actual": str(actual), "delta": str(delta), "details": details,
    }


def _scalar(handler: NDDBHandler, sql: str, params: dict | None = None) -> int:
    with handler.engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _count(handler: NDDBHandler, table: str) -> int:
    qi = handler._qi
    nolock = " WITH (NOLOCK)" if handler.engine.dialect.name == "mssql" else ""
    return _scalar(handler, f"SELECT COUNT(*) FROM {qi(table)}{nolock}")


def _group_counts(handler: NDDBHandler, table: str, group_col: str, agg_expr: str) -> list[int]:
    """Return the sorted list of ``agg_expr`` per ``group_col`` (identity-agnostic distribution)."""
    qi = handler._qi
    nolock = " WITH (NOLOCK)" if handler.engine.dialect.name == "mssql" else ""
    sql = (
        f"SELECT {agg_expr} AS c FROM {qi(table)}{nolock} "
        f"WHERE {qi(group_col)} IS NOT NULL GROUP BY {qi(group_col)}"
    )
    with handler.engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return sorted(int(r[0]) for r in rows)


# ── individual checks ──────────────────────────────────────────────────────────


def check_mapping_to_table_counts(mapping_handler, dest_handler, cfg: Part2Config) -> list[dict]:
    out = []
    for mapping_table, target_table in cfg.mapping_target_pairs:
        try:
            m = _count(mapping_handler, mapping_table)
            t = _count(dest_handler, target_table)
            status = "pass" if m == t else "fail"
            out.append(_result(
                "mapping_to_table_count", status, entity=f"{mapping_table}->{target_table}",
                expected=m, actual=t, delta=t - m,
            ))
        except Exception as exc:
            out.append(_result("mapping_to_table_count", "error", entity=f"{mapping_table}->{target_table}", details=str(exc)))
    return out


def check_patient_encounter_counts(source_handler, dest_handler, cfg: Part2Config) -> list[dict]:
    if not cfg.patient_encounter_table:
        return [_result("patient_encounter_count", "skipped", details="patient_encounter_table not set")]
    try:
        agg = f"COUNT(DISTINCT {dest_handler._qi(cfg.encounter_col)})"
        src = _group_counts(source_handler, cfg.patient_encounter_table, cfg.patient_col, agg)
        dst = _group_counts(dest_handler, cfg.patient_encounter_table, cfg.patient_col, agg)
        status = "pass" if src == dst else "fail"
        details = "" if status == "pass" else f"distribution differs (source {len(src)} patients, dest {len(dst)})"
        return [_result("patient_encounter_count", status, entity=cfg.patient_encounter_table,
                        expected=sum(src), actual=sum(dst), delta=sum(dst) - sum(src), details=details)]
    except Exception as exc:
        return [_result("patient_encounter_count", "error", entity=cfg.patient_encounter_table, details=str(exc))]


def check_encounter_row_counts(source_handler, dest_handler, cfg: Part2Config) -> list[dict]:
    out = []
    for table in cfg.encounter_fk_tables:
        try:
            agg = "COUNT(*)"
            src = _group_counts(source_handler, table, cfg.encounter_fk_col, agg)
            dst = _group_counts(dest_handler, table, cfg.encounter_fk_col, agg)
            status = "pass" if src == dst else "fail"
            details = "" if status == "pass" else f"per-encounter row-count distribution differs on {table}"
            out.append(_result("encounter_row_count", status, entity=table,
                               expected=sum(src), actual=sum(dst), delta=sum(dst) - sum(src), details=details))
        except Exception as exc:
            out.append(_result("encounter_row_count", "error", entity=table, details=str(exc)))
    return out


def _uniqueness_violations(handler: NDDBHandler, table: str, src_col: str, nd_col: str) -> int:
    qi = handler._qi
    nolock = " WITH (NOLOCK)" if handler.engine.dialect.name == "mssql" else ""
    sql = (
        f"SELECT COUNT(*) FROM (SELECT {qi(src_col)} FROM {qi(table)}{nolock} "
        f"GROUP BY {qi(src_col)} HAVING COUNT(DISTINCT {qi(nd_col)}) > 1) t"
    )
    return _scalar(handler, sql)


def check_mapping_uniqueness(mapping_handler, cfg: Part2Config) -> list[dict]:
    if not cfg.check_uniqueness:
        return []
    out = []
    for table, src_col, nd_col, label in (
        (cfg.patient_mapping_table, cfg.patient_map_src_col, cfg.patient_map_nd_col, "patient"),
        (cfg.encounter_mapping_table, cfg.encounter_map_src_col, cfg.encounter_map_nd_col, "encounter"),
    ):
        try:
            v = _uniqueness_violations(mapping_handler, table, src_col, nd_col)
            status = "pass" if v == 0 else "fail"
            out.append(_result("mapping_uniqueness", status, entity=f"{label}:{table}",
                               expected=0, actual=v, delta=v,
                               details="" if v == 0 else f"{v} {src_col} mapped to >1 {nd_col}"))
        except Exception as exc:
            out.append(_result("mapping_uniqueness", "error", entity=f"{label}:{table}", details=str(exc)))
    return out


def check_offset_range(mapping_handler, cfg: Part2Config) -> list[dict]:
    if not cfg.check_offset_range:
        return []
    qi = mapping_handler._qi
    table = cfg.patient_mapping_table
    try:
        sql = (
            f"SELECT COUNT(*) FROM {qi(table)} "
            f"WHERE {qi(cfg.offset_col)} IS NOT NULL "
            f"AND ({qi(cfg.offset_col)} < :lo OR {qi(cfg.offset_col)} > :hi)"
        )
        v = _scalar(mapping_handler, sql, {"lo": cfg.offset_min, "hi": cfg.offset_max})
        status = "pass" if v == 0 else "fail"
        return [_result("offset_range", status, entity=table, expected=0, actual=v, delta=v,
                        details="" if v == 0 else f"{v} offsets outside [{cfg.offset_min},{cfg.offset_max}]")]
    except Exception as exc:
        return [_result("offset_range", "error", entity=table, details=str(exc))]


def check_mapping_id_format(mapping_handler, cfg: Part2Config) -> list[dict]:
    qi = mapping_handler._qi
    len_fn = "LEN" if mapping_handler.engine.dialect.name == "mssql" else "LENGTH"
    out = []

    def _fmt_check(table, col, length, prefix, label):
        conds, params = [], {}
        if length is not None:
            conds.append(f"{len_fn}(CAST({qi(col)} AS CHAR)) <> :len")
            params["len"] = length
        if prefix is not None:
            conds.append(f"CAST({qi(col)} AS CHAR) NOT LIKE :pref")
            params["pref"] = f"{prefix}%"
        if not conds:
            return None
        where = " OR ".join(conds)
        sql = f"SELECT COUNT(*) FROM {qi(table)} WHERE {qi(col)} IS NOT NULL AND ({where})"
        try:
            v = _scalar(mapping_handler, sql, params)
            status = "pass" if v == 0 else "fail"
            return _result("mapping_id_format", status, entity=f"{label}:{col}", expected=0, actual=v, delta=v,
                           details="" if v == 0 else f"{v} rows fail len={length}/prefix={prefix}")
        except Exception as exc:
            return _result("mapping_id_format", "error", entity=f"{label}:{col}", details=str(exc))

    r = _fmt_check(cfg.patient_mapping_table, cfg.patient_map_nd_col, cfg.nd_patient_len, None, "patient")
    if r:
        out.append(r)
    r = _fmt_check(cfg.encounter_mapping_table, cfg.encounter_map_nd_col, cfg.nd_encounter_len, cfg.nd_encounter_prefix, "encounter")
    if r:
        out.append(r)
    return out


# ── aggregate + gate ────────────────────────────────────────────────────────────


def run_part2_checks(
    source_conn_str: str,
    dest_conn_str: str,
    mapping_conn_str: str,
    cfg: Part2Config,
    qc_results_db_url: str = "",
) -> list[dict]:
    """Run all configured Part-2 checks and (optionally) persist. Returns one dict per check."""
    source = NDDBHandler(source_conn_str, read_only=True)
    dest = NDDBHandler(dest_conn_str)
    mapping = NDDBHandler(mapping_conn_str, read_only=True)
    try:
        results: list[dict] = []
        results += check_mapping_to_table_counts(mapping, dest, cfg)
        results += check_patient_encounter_counts(source, dest, cfg)
        results += check_encounter_row_counts(source, dest, cfg)
        results += check_mapping_uniqueness(mapping, cfg)
        results += check_offset_range(mapping, cfg)
        results += check_mapping_id_format(mapping, cfg)
    finally:
        source.close()
        dest.close()
        mapping.close()

    for r in results:
        logger.info("[Part2] %s [%s]: %s (expected=%s actual=%s)",
                    r["check_name"], r["entity"], r["status"].upper(), r["expected"], r["actual"])
    if qc_results_db_url:
        _persist(qc_results_db_url, results)
    return results


def run_pre_pipeline_gate(
    source_conn_str: str,
    dest_conn_str: str,
    mapping_conn_str: str,
    cfg: Part2Config,
    qc_results_db_url: str = "",
    blocking: bool = True,
) -> list[dict]:
    """Run Part-2 checks; if ``blocking`` and any blocking check fails, raise ``Part2Blocked``.

    ``error`` and ``fail`` on a blocking check both block (fail-closed) — a check we could not run
    is not evidence the data is sound.
    """
    results = run_part2_checks(source_conn_str, dest_conn_str, mapping_conn_str, cfg, qc_results_db_url)
    failures = [r for r in results if r["blocking"] and r["status"] in ("fail", "error")]
    if failures and blocking:
        raise Part2Blocked(failures)
    return results


def _persist(db_url: str, results: list[dict]) -> None:
    from sqlalchemy.orm import Session

    from deid.models.base import create_qc_results_engine, create_all_qc_results_tables
    from deid.models.qc_results import QCPart2Result

    engine = create_qc_results_engine(db_url)
    create_all_qc_results_tables(engine)
    with Session(engine) as session:
        for r in results:
            session.add(QCPart2Result(
                check_name=r["check_name"], entity=r["entity"], status=r["status"],
                blocking=r["blocking"], expected=r["expected"], actual=r["actual"],
                delta=r["delta"], details=r["details"],
            ))
        session.commit()
    engine.dispose()
    logger.info("[Part2] Persisted %d check result(s) to %s", len(results), db_url)
