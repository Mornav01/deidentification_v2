"""CLI command: deid qc-delta — run the delta-identity QC (cross-env row-level diff).

Manual trigger (Trigger B) for the Part-2 row-level check. The automatic trigger (Trigger A) is the
call embedded in the CDC merge flow. Both call ``deid.qc.delta_identity.run_delta_identity_qc``.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("deid.cli.qc_delta")


def qc_delta_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    tables: Optional[str] = typer.Option(None, "--tables", "-t", help="Comma-separated table list (overrides config)"),
    delta_after: Optional[str] = typer.Option(None, "--delta-after", help="Only check dest rows with delta_col > this (overrides config)"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Run the delta-identity QC comparing source (prod) vs dest on a shared row key."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    from deid.config.loader import load_config
    from deid.qc.api import run_delta_identity_from_config

    cfg = load_config(config_path)
    table_list = [t.strip() for t in tables.split(",") if t.strip()] if tables else None
    try:
        results = run_delta_identity_from_config(cfg, tables=table_list, delta_after=delta_after)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)
    n_fail = sum(1 for r in results if not r["is_qc_passed"])
    for r in results:
        status = "PASS" if r["is_qc_passed"] else "FAIL"
        typer.echo(
            f"  [{status}] {r['table_name']}: missing={r['missing_in_dest']} "
            f"extra={r['extra_in_dest']} mismatch={r['value_mismatch']} matched={r['matched_count']}"
        )
    typer.echo(f"Delta-identity QC complete: {len(results)} table(s), {n_fail} failing.")
    if n_fail:
        raise typer.Exit(code=1)
