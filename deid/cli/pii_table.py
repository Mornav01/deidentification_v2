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


def run_pii_table(config_path: str, pii_config_output: str | None = None) -> str:
    """Core logic: create PII tables and write pii_config to a YAML file.

    Args:
        config_path: Path to config.yaml.
        pii_config_output: Output path for pii_config YAML. Defaults to
            pii_config.yaml in the same directory as config.yaml.

    Returns:
        Path to the generated pii_config YAML file.
    """
    from sqlalchemy import create_engine, inspect as sa_inspect

    from deid.config.loader import load_config
    from deid.config.pii_generator import generate_pii_config, generate_pii_tables_config
    from deid.core.dbPkg.phi_table.create_table import PIITable

    config = load_config(config_path)

    if not config.pii_db:
        typer.echo("Error: pii_db is not configured in config.yaml", err=True)
        raise typer.Exit(code=1)

    dest_url = config.pii_db["master_connection_str"]

    # Resolve pii_tables_config: explicit > table-rules > source-DB introspection.
    pii_tables_config = config.pii_tables_config
    if not pii_tables_config:
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
        pii_config = generate_pii_config(pii_tables_config)
        if pii_config:
            print(f"Auto-generated pii_config keys: {list(pii_config.keys())}")

    # Check which tables already exist.
    dest_engine = create_engine(dest_url)
    existing = set(sa_inspect(dest_engine).get_table_names())
    dest_engine.dispose()

    needed = [t for t in pii_tables_config if t not in existing]
    if not needed:
        print("PII tables already exist — skipping creation.")
    else:
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
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Create PII tables in the PII DB and generate pii_config YAML."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    run_pii_table(str(config_path), pii_config_output=pii_config_output)
