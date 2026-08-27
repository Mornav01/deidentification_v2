"""Unified QC audit report — consolidates Parts 1–3 from qc_results.db.

Reads the four QC result tables (structured per-table, Part-2 mapping/count, delta-identity, and the
Part-3 unstructured audit) and assembles the QC Framework "Audit Output": timestamps, totals,
pass/fail counts, failure detail, and coverage gaps. ``render_markdown`` produces a human report.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

from deid.models.base import create_qc_results_engine, create_all_qc_results_tables
from deid.models.qc_results import (
    QCTableResult,
    QCPart2Result,
    QCDeltaIdentityResult,
    QCUnstructuredAuditResult,
)


def build_audit_report(qc_results_db_url: str) -> dict:
    """Read qc_results.db and return a consolidated report dict for all three framework parts."""
    engine = create_qc_results_engine(qc_results_db_url)
    create_all_qc_results_tables(engine)
    existing = set(inspect(engine).get_table_names())
    report: dict = {"generated_at": datetime.now(timezone.utc).isoformat()}
    with Session(engine) as session:
        # Part 1 — structured per-table
        p1 = list(session.scalars(select(QCTableResult))) if "qc_table_results" in existing else []
        report["part1_structured"] = {
            "tables_checked": len(p1),
            "passed": sum(1 for r in p1 if r.is_qc_passed),
            "failed": sum(1 for r in p1 if not r.is_qc_passed),
            "failures": [{"table": r.table_name, "reason": r.reason} for r in p1 if not r.is_qc_passed],
        }
        # Part 2 — mapping & count
        p2 = list(session.scalars(select(QCPart2Result))) if "qc_part2_results" in existing else []
        report["part2_mapping_count"] = {
            "checks": len(p2),
            "passed": sum(1 for r in p2 if r.status == "pass"),
            "failed": sum(1 for r in p2 if r.status in ("fail", "error")),
            "failures": [
                {"check": r.check_name, "entity": r.entity, "expected": r.expected,
                 "actual": r.actual, "delta": r.delta, "details": r.details}
                for r in p2 if r.status in ("fail", "error")
            ],
        }
        # Part 2 (row-level) — delta identity
        di = list(session.scalars(select(QCDeltaIdentityResult))) if "qc_delta_identity_results" in existing else []
        report["delta_identity"] = {
            "tables_checked": len(di),
            "passed": sum(1 for r in di if r.is_qc_passed),
            "failed": sum(1 for r in di if not r.is_qc_passed),
            "failures": [
                {"table": r.table_name, "missing_in_dest": r.missing_in_dest,
                 "extra_in_dest": r.extra_in_dest, "value_mismatch": r.value_mismatch}
                for r in di if not r.is_qc_passed
            ],
        }
        # Part 3 — unstructured audit
        p3 = list(session.scalars(select(QCUnstructuredAuditResult))) if "qc_unstructured_audit_results" in existing else []
        report["part3_unstructured"] = {
            "tables_audited": len(p3),
            "total_records_audited": sum(r.total_records_audited for r in p3),
            "phi_entities_checked": sum(r.phi_entities_checked for r in p3),
            "pass_count": sum(r.pass_count for r in p3),
            "fail_count": sum(r.fail_count for r in p3),
            "coverage_gaps": sum(r.coverage_gaps for r in p3),
            "quarantine": [
                {"table": r.table_name, "failures": json.loads(r.failure_detail or "[]")}
                for r in p3 if r.fail_count
            ],
        }
    engine.dispose()

    report["overall_passed"] = (
        report["part1_structured"]["failed"] == 0
        and report["part2_mapping_count"]["failed"] == 0
        and report["delta_identity"]["failed"] == 0
        and report["part3_unstructured"]["fail_count"] == 0
        and report["part3_unstructured"]["coverage_gaps"] == 0
    )
    return report


def render_markdown(report: dict) -> str:
    p1, p2 = report["part1_structured"], report["part2_mapping_count"]
    di, p3 = report["delta_identity"], report["part3_unstructured"]
    lines = [
        "# QC Audit Report",
        f"_Generated: {report['generated_at']}_",
        f"**Overall: {'✅ PASSED' if report['overall_passed'] else '❌ FAILED'}**",
        "",
        "| Part | Scope | Passed | Failed |",
        "|---|---|---|---|",
        f"| 1 — Structured | {p1['tables_checked']} tables | {p1['passed']} | {p1['failed']} |",
        f"| 2 — Mapping & Count | {p2['checks']} checks | {p2['passed']} | {p2['failed']} |",
        f"| 2 — Delta identity | {di['tables_checked']} tables | {di['passed']} | {di['failed']} |",
        f"| 3 — Unstructured | {p3['tables_audited']} tables, {p3['total_records_audited']} records | {p3['pass_count']} | {p3['fail_count']} |",
        "",
        f"PHI entities checked: {p3['phi_entities_checked']} · Coverage gaps: {p3['coverage_gaps']}",
    ]
    if not report["overall_passed"]:
        lines.append("\n## Failures")
        for f in p2["failures"]:
            lines.append(f"- **Part2** {f['check']} [{f['entity']}]: expected {f['expected']}, got {f['actual']} — {f['details']}")
        for f in di["failures"]:
            lines.append(f"- **Delta** {f['table']}: missing={f['missing_in_dest']} extra={f['extra_in_dest']} mismatch={f['value_mismatch']}")
        for f in p1["failures"]:
            lines.append(f"- **Part1** {f['table']}: {f['reason']}")
        for q in p3["quarantine"]:
            lines.append(f"- **Part3** {q['table']}: {len(q['failures'])} record(s) quarantined")
    return "\n".join(lines)
