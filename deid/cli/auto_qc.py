"""CLI command: deid auto-qc — run the full QC framework over a table list, emit two CSVs.

Given a config.yaml (connections + qc defaults) and a PHI rules CSV, runs Part 1 (structured/
unstructured scan), Part 2 (mapping/count gate + delta-identity), and Part 3 (master PHI audit)
for each table, then writes ``auto_qc_summary.csv`` + ``auto_qc_findings.csv`` to the output dir.
Exits non-zero when any table fails or errors (so an Airflow BashOperator marks the task failed).
"""
from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("deid.cli.auto_qc")


def _read_tables_file(path: str) -> list[str]:
    """Read a headerless single-column CSV of table names (first column, blanks skipped).

    Matches the format the other deid steps consume (e.g. state_deid_init_v2.py).
    """
    with open(path, newline="") as f:
        return [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]


def auto_qc_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    rules_csv: str = typer.Option(..., "--rules-csv", "-r", help="Path to the PHI rules CSV (table_name,column_name,rule)"),
    tables: Optional[str] = typer.Option(None, "--tables", "-t", help="Comma-separated tables to QC (default: all tables in the rules CSV)"),
    tables_filepath: Optional[str] = typer.Option(None, "--tables-filepath", help="Headerless single-column CSV of table names (overrides --tables)"),
    out_dir: str = typer.Option(".", "--out-dir", "-o", help="Directory to write the summary + findings CSVs"),
    max_workers: int = typer.Option(1, "--max-workers", help="Number of tables to QC concurrently (thread pool)"),
    pii_master_conn_str: Optional[str] = typer.Option(None, "--pii-master-conn-str", help="Override the PHI master connection (Part 3 + notes exact-match)"),
    residual_pii_backend: str = typer.Option("regex", "--residual-pii-backend", help="Residual-PII scanner backend: regex | none"),
    delta_after: Optional[str] = typer.Option(None, "--delta-after", help="Delta-identity: only check dest rows with delta_col > this"),
    no_gate: bool = typer.Option(False, "--no-gate", help="Skip the Part-2 mapping & count gate"),
    stamp: bool = typer.Option(False, "--stamp", help="Append a UTC timestamp to the CSV filenames"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Run auto-QC over a list of tables and write summary + findings CSVs."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)
    if not Path(rules_csv).exists():
        typer.echo(f"Error: Rules CSV not found: {rules_csv}", err=True)
        raise typer.Exit(code=1)
    if tables_filepath and not Path(tables_filepath).exists():
        typer.echo(f"Error: Tables file not found: {tables_filepath}", err=True)
        raise typer.Exit(code=1)

    from deid.config.loader import load_config
    from deid.qc.api import run_auto_qc_from_config

    cfg = load_config(config_path)
    if tables_filepath:
        table_list = _read_tables_file(tables_filepath) or None
    elif tables:
        table_list = [t.strip() for t in tables.split(",") if t.strip()]
    else:
        table_list = None
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") if stamp else ""

    try:
        out = run_auto_qc_from_config(
            cfg, rules_csv, table_list,
            out_dir=out_dir,
            max_workers=max_workers,
            pii_master_conn_str=pii_master_conn_str,
            residual_pii_backend=residual_pii_backend,
            delta_after=delta_after,
            include_gate=not no_gate,
            timestamp=timestamp,
        )
    except (ValueError, FileNotFoundError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)

    failing = [r for r in out["summary"] if r["overall_status"] in ("FAIL", "ERROR", "PARTIAL")]
    for r in out["summary"]:
        typer.echo(f"  [{r['overall_status']:<8}] {r['table']}  "
                   f"(part1={r['part1_status']} delta={r['delta_status']} master={r['master_status']})")
    typer.echo(f"Auto-QC complete: {len(out['summary'])} row(s), {len(failing)} failing/errored.")
    typer.echo(f"  summary : {out['summary_csv']}")
    typer.echo(f"  findings: {out['findings_csv']}  ({len(out['findings'])} finding(s))")
    if failing:
        raise typer.Exit(code=1)
