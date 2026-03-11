"""CLI command: deid generate-config — generate rules CSV from source DB."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("deid.cli")


def run_generate_config(
    config_path: str,
    schema: str | None = None,
    tables: list[str] | None = None,
    output: str | None = None,
) -> str:
    """Core logic: introspect source DB and generate rules CSV.

    Args:
        config_path: Path to config.yaml.
        schema: Database schema name (optional).
        tables: Specific tables to include (optional, default: all).
        output: Output CSV path. If None, uses config.rules_csv or 'config_rules.csv'.

    Returns:
        Path to the generated CSV file.
    """
    from deid.config.loader import load_config
    from deid.config.rules_generator import extract_schema, write_csv

    config = load_config(config_path)

    output_path = output or config.rules_csv or "config_rules.csv"

    source_db = config.source_db
    print(f"Source DB: {source_db.type.value} '{source_db.database}' at {source_db.host}:{source_db.port}")

    rows = extract_schema(source_db, tables=tables, schema=schema)

    # Print summary
    table_set = set(r["table_name"] for r in rows)
    assigned = [r for r in rows if r["rule"]]
    unassigned = [r for r in rows if not r["rule"]]
    print(f"\nFound {len(rows)} columns across {len(table_set)} tables.")
    print(f"  Auto-assigned: {len(assigned)} columns")
    print(f"  Unassigned:    {len(unassigned)} columns")

    if assigned:
        rule_counts: dict[str, int] = {}
        for r in assigned:
            rule_counts[r["rule"]] = rule_counts.get(r["rule"], 0) + 1
        print("\n  Rule breakdown:")
        for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
            print(f"    {rule:<20s} {count}")

    write_csv(rows, output_path)
    print(f"\nConfig CSV written to: {output_path}")
    print("Review the 'rule' column and fill in any unassigned columns before use.")
    return output_path


def generate_config_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Database schema name"),
    tables: Optional[list[str]] = typer.Option(None, "--tables", "-t", help="Only include these tables"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output CSV path (default: from config.rules_csv or config_rules.csv)"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Generate a rules CSV by introspecting the source database."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    run_generate_config(str(config_path), schema=schema, tables=tables, output=output)
