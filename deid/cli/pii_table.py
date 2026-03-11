"""CLI command: deid pii-table — create PII tables and generate pii_config."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
import yaml
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("deid.cli")

DEFAULT_PII_CONFIG_FILENAME = "pii_config.yaml"


def run_pii_table(
    config_path: str,
    pii_config_output: str | None = None,
    config_only: bool = False,
) -> str:
    """Core logic: create PII tables and write pii_config to a YAML file.

    Args:
        config_path: Path to config.yaml.
        pii_config_output: Output path for pii_config YAML. Defaults to
            pii_config.yaml in the same directory as config.yaml.
        config_only: If True, skip all DB operations and only write pii_config.yaml.
            Requires pii_tables_config to be set explicitly in config.yaml.

    Returns:
        Path to the generated pii_config YAML file.
    """
    from deid.config.loader import load_config
    from deid.config.pii_generator import generate_pii_config, generate_pii_tables_config

    from sqlalchemy import create_engine, inspect as sa_inspect

    config = load_config(config_path)

    if not config.pii_db:
        typer.echo("Error: pii_db is not configured in config.yaml", err=True)
        raise typer.Exit(code=1)

    dest_url = config.pii_db["master_connection_str"]
    existing: set = set()

    # Auto-detect config_only: if all PII tables already exist, skip all DB operations.
    # Use explicitly configured table names if available, otherwise check the default.
    if not config_only:
        tables_to_check = (
            list(config.pii_tables_config.keys())
            if config.pii_tables_config
            else ["pii_data_table"]
        )
        dest_engine = create_engine(dest_url)
        existing = set(sa_inspect(dest_engine).get_table_names())
        dest_engine.dispose()
        if all(t in existing for t in tables_to_check):
            print("PII tables already exist — skipping DB operations.")
            config_only = True

    # Resolve pii_tables_config: explicit > table-rules > source-DB introspection.
    # Source DB introspection is skipped when config_only (tables already exist or flag set).
    pii_tables_config = config.pii_tables_config
    if not pii_tables_config and not config_only:
        pii_tables_config = generate_pii_tables_config(
            config.tables or [], source_db=config.source_db,
        )
        if pii_tables_config:
            print(f"Auto-generated pii_tables_config from source: {list(pii_tables_config.keys())}")
        else:
            typer.echo(
                "Error: No PII source tables could be identified. "
                "Provide pii_tables_config in config.yaml.",
                err=True,
            )
            raise typer.Exit(code=1)

    # Generate pii_config (mask/dob/combine rules).
    pii_config = config.pii_config
    if not pii_config:
        if not pii_tables_config:
            typer.echo(
                "Error: pii_tables_config must be set in config.yaml to generate pii_config "
                "when PII tables already exist or --config-only is used.",
                err=True,
            )
            raise typer.Exit(code=1)
        pii_config = generate_pii_config(pii_tables_config)
        if pii_config:
            print(f"Auto-generated pii_config keys: {list(pii_config.keys())}")

    if not config_only:
        from deid.core.dbPkg.phi_table.create_table import PIITable

        needed = [t for t in pii_tables_config if t not in existing]
        print(f"Creating PII tables: {', '.join(needed)}")
        pii_manager = PIITable(
            src_db_url=config.source_db.connection_string(),
            dest_db_url=dest_url,
            pii_tables_config=pii_tables_config,
        )
        pii_manager.generate_pii_tables()
        print("PII tables generated successfully.")

    # Write pii_config to file.
    if pii_config_output is None:
        pii_config_output = str(Path(config_path).parent / DEFAULT_PII_CONFIG_FILENAME)

    with open(pii_config_output, "w") as f:
        yaml.dump(pii_config, f, default_flow_style=False)

    print(f"pii_config written to: {pii_config_output}")
    return pii_config_output


def pii_table_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    pii_config_output: Optional[str] = typer.Option(
        None, "--pii-config-output", "-o",
        help="Output path for pii_config YAML (default: pii_config.yaml next to config)",
    ),
    config_only: bool = typer.Option(
        False, "--config-only",
        help="Only generate pii_config.yaml — skip all DB operations. "
             "Requires pii_tables_config in config.yaml.",
    ),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Create PII tables in the PII DB and generate pii_config YAML."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    run_pii_table(str(config_path), pii_config_output=pii_config_output, config_only=config_only)
