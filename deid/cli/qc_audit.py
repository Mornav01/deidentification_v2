"""CLI command: deid qc-audit — Part 3 master-referenced unstructured PHI audit.

Runs the post-pipeline audit (``deid.qc.master_phi``) over the tables declared in ``qc.master_phi``.
For each configured table it scans de-identified note text against the PHI master and residual-PHI
regexes, then persists a per-table audit (with a quarantine list) to ``qc_results.db``.

Config shape (config.yaml):

    qc:
      master_phi:
        pii_master_conn_str: "mysql+pymysql://user:pass@host/master_oct"
        pii_columns: [users_ufname, users_ulname, users_upphone]   # shared default
        facility_names: [Northwest]                                 # shared default
        sample_size: 500                                            # 0 = whole table
        tables:
          - dest_table: rwe_ad_mci_lab
            content_cols: [content]
            name_columns: [users_ufname, users_ulname]
          - dest_table: progressnotes
            content_cols: [note_text]

Top-level keys (everything except ``tables``) are shared defaults merged into each table entry;
a table entry may override any of them.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("deid.cli.qc_audit")


def qc_audit_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Audit only this dest_table"),
    report: bool = typer.Option(False, "--report", help="Print the consolidated audit report after the run"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Run the Part-3 master-referenced unstructured PHI audit."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    from deid.config.loader import load_config
    from deid.qc.api import run_master_phi_from_config

    cfg = load_config(config_path)
    if not (getattr(cfg.qc, "master_phi", {}) or {}).get("pii_master_conn_str"):
        typer.echo("Warning: qc.master_phi.pii_master_conn_str not set — presence scan disabled, "
                   "only residual-PHI regexes will run.", err=True)
    try:
        reports = run_master_phi_from_config(cfg, table=table)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)

    total_fail = 0
    for rep in reports:
        total_fail += rep["fail_count"]
        status = "PASS" if rep["fail_count"] == 0 and rep["coverage_gaps"] == 0 else "FAIL"
        typer.echo(
            f"  [{status}] {rep['table_name']}: audited={rep['total_records_audited']} "
            f"pass={rep['pass_count']} fail={rep['fail_count']} coverage_gaps={rep['coverage_gaps']}"
        )

    if report:
        from deid.qc.report import build_audit_report, render_markdown
        typer.echo("\n" + render_markdown(build_audit_report(cfg.resolved_qc_results_db_url)))

    typer.echo(f"Part 3 audit complete: {len(reports)} table(s), {total_fail} failing record(s).")
    if total_fail:
        raise typer.Exit(code=1)
