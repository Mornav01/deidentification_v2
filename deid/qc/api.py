"""Public QC API — one import surface for the whole QC Framework.

This module is the intended entry point for orchestrators (e.g. an Airflow DAG). Every QC "task" is
an importable function that takes plain arguments (a loaded ``DeidConfig`` or connection strings +
a config dataclass), performs the work, persists results to ``qc_results.db``, and **returns a
structured result** (dict / list of dicts) — it does no printing and no ``sys.exit``. Callers inspect
the return value (or catch ``Part2Blocked``) to decide pass/fail and raise their own alerts.

Airflow example:

    from deid.config.loader import load_config
    from deid.qc.api import (
        run_part2_from_config, run_delta_identity_from_config, run_master_phi_from_config,
        build_audit_report, Part2Blocked,
    )

    cfg = load_config("config.yaml")

    def part2_task():                       # blocking gate — raises to fail the task
        run_part2_from_config(cfg)

    def delta_task():
        results = run_delta_identity_from_config(cfg)
        if any(not r["is_qc_passed"] for r in results):
            raise ValueError("delta-identity QC failed")   # Airflow marks task failed → alert

    def audit_task():
        reports = run_master_phi_from_config(cfg)
        if any(r["fail_count"] or r["coverage_gaps"] for r in reports):
            raise ValueError("Part 3 audit found PHI / coverage gaps")

The low-level functions and config dataclasses are re-exported here too, so a caller can build
configs by hand instead of via YAML.
"""
from __future__ import annotations

from typing import Optional

# ── low-level re-exports (build configs by hand + call directly) ────────────────
from deid.qc.mapping_count import (
    Part2Config,
    Part2Blocked,
    run_part2_checks,
    run_pre_pipeline_gate,
)
from deid.qc.delta_identity import DeltaIdentityConfig, run_delta_identity_qc
from deid.qc.master_phi import MasterPhiConfig, make_pii_loader, run_master_phi_audit
from deid.qc.coverage import CoverageConfig, check_coverage
from deid.qc.report import build_audit_report, render_markdown

__all__ = [
    # config-driven task functions (recommended for orchestrators)
    "run_part2_from_config",
    "run_delta_identity_from_config",
    "run_master_phi_from_config",
    "build_audit_report",
    "render_markdown",
    # low-level building blocks
    "Part2Config", "Part2Blocked", "run_part2_checks", "run_pre_pipeline_gate",
    "DeltaIdentityConfig", "run_delta_identity_qc",
    "MasterPhiConfig", "make_pii_loader", "run_master_phi_audit",
    "CoverageConfig", "check_coverage",
]


# ── Part 2 — mapping & count gate ───────────────────────────────────────────────


def run_part2_from_config(cfg, *, blocking: Optional[bool] = None, qc_results_db_url: str = "") -> list[dict]:
    """Run the Part-2 mapping & count checks declared in ``cfg.qc.part2``.

    Returns [] when ``cfg.qc.part2`` is empty. Raises ``Part2Blocked`` when a blocking check fails
    and blocking is on (defaults to ``cfg.qc.part2_blocking``). Otherwise returns the per-check list.
    """
    part2 = dict(getattr(cfg.qc, "part2", {}) or {})
    if not part2:
        return []
    block = bool(getattr(cfg.qc, "part2_blocking", True)) if blocking is None else blocking
    return run_pre_pipeline_gate(
        source_conn_str=cfg.source_db.connection_string(),
        dest_conn_str=cfg.destination_db.connection_string(),
        mapping_conn_str=cfg.mappings_connection_string,
        cfg=Part2Config.from_dict(part2),
        qc_results_db_url=qc_results_db_url or cfg.resolved_qc_results_db_url,
        blocking=block,
    )


# ── Part 2 (row-level) — delta identity ─────────────────────────────────────────


def run_delta_identity_from_config(
    cfg, *, tables: Optional[list[str]] = None, delta_after: Optional[str] = None,
    qc_results_db_url: str = "",
) -> list[dict]:
    """Run the delta-identity QC using ``cfg.qc.delta_identity`` (overridable by args). One dict per table."""
    di = dict(getattr(cfg.qc, "delta_identity", {}) or {})
    if tables:
        di["tables"] = tables
    if delta_after:
        di["delta_after"] = delta_after
    if not di.get("tables"):
        raise ValueError("No tables to check — set qc.delta_identity.tables or pass tables=[...].")
    return run_delta_identity_qc(
        source_conn_str=cfg.source_db.connection_string(),
        dest_conn_str=cfg.destination_db.connection_string(),
        cfg=DeltaIdentityConfig.from_dict(di),
        qc_results_db_url=qc_results_db_url or cfg.resolved_qc_results_db_url,
    )


# ── Part 3 — master-referenced unstructured audit ───────────────────────────────


def run_master_phi_from_config(cfg, *, table: Optional[str] = None, qc_results_db_url: str = "") -> list[dict]:
    """Run the Part-3 audit for the tables in ``cfg.qc.master_phi.tables`` (or just ``table``).

    Top-level ``master_phi`` keys (except ``tables``/``pii_master_conn_str``) are shared defaults
    merged into each table entry. Returns one audit report dict per audited table.
    """
    mp = dict(getattr(cfg.qc, "master_phi", {}) or {})
    entries = list(mp.get("tables") or [])
    if table:
        entries = [t for t in entries if t.get("dest_table") == table]
    if not entries:
        raise ValueError("No tables to audit — set qc.master_phi.tables (or pass a matching table=).")
    conn_str = mp.get("pii_master_conn_str")
    shared = {k: v for k, v in mp.items() if k not in ("tables", "pii_master_conn_str")}
    dest_conn = cfg.destination_db.connection_string()
    db_url = qc_results_db_url or cfg.resolved_qc_results_db_url

    reports: list[dict] = []
    for entry in entries:
        merged = {**shared, **entry}
        if not merged.get("dest_table") or not merged.get("content_cols"):
            continue
        mcfg = MasterPhiConfig.from_dict(merged)
        loader = make_pii_loader(conn_str, mcfg.pii_columns) if conn_str else (lambda ids: {})
        reports.append(run_master_phi_audit(dest_conn, mcfg, loader, qc_results_db_url=db_url))
    return reports
