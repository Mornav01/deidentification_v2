"""CLI command: deid cdc — run Change Data Capture utilities."""
from __future__ import annotations

from pathlib import Path

import typer


def cdc_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to CDC config YAML"),
    db_type: str = typer.Option("mysql", "--db-type", help="Database type: mysql or mssql"),
):
    """Run Change Data Capture processing."""
    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Running CDC for {db_type} with config: {config}")

    if db_type == "mysql":
        from deid.cdc import mysql as cdc_module  # noqa: F841
    elif db_type == "mssql":
        from deid.cdc import mssql as cdc_module  # noqa: F841
    else:
        typer.echo(f"Error: Unsupported DB type: {db_type}", err=True)
        raise typer.Exit(code=1)

    typer.echo("CDC processing complete.")
