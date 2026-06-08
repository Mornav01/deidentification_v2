"""CLI command: deid mapping — create and populate mapping tables."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("deid.cli")


def run_mapping(config_path: str, mappings_db: str | None = None) -> dict:
    """Core logic: create mappings DB and populate patient/encounter/appointment mappings.

    Args:
        config_path: Path to config.yaml.
        mappings_db: Override mappings DB path (default: from config).

    Returns:
        Summary dict with counts.
    """
    from deid.config.loader import load_config
    from deid.core.dbPkg.dbhandler import NDDBHandler
    from deid.core.mapping_populator import populate_mappings
    from deid.models.base import create_all_mappings_tables, create_mappings_engine

    config = load_config(config_path)
    mappings_db_path = mappings_db or config.mappings_db_path

    print(f"Source DB:    {config.source_db.type.value} '{config.source_db.database}' "
          f"at {config.source_db.host}:{config.source_db.port}")
    print(f"Mappings DB:  {mappings_db_path}")
    print(f"Tables:       {len(config.tables or [])}")

    mappings_engine = create_mappings_engine(mappings_db_path)
    create_all_mappings_tables(mappings_engine)

    source = NDDBHandler(config.source_db.connection_string(), read_only=True)

    # Derive the identifier column name used in patient_mapping_table from config.
    # identifier_columns[0] is the primary column in the mapping table that holds
    # the source patient ID. Falls back to "patient_id" for backward compatibility.
    _pat_cfg = config.mapping_tables.get("patient") if config.mapping_tables else None
    source_id_column = (
        _pat_cfg.identifier_columns[0]
        if _pat_cfg and _pat_cfg.identifier_columns
        else "patient_id"
    )

    try:
        print("\nScanning source tables for IDs...")
        summary = populate_mappings(
            source,
            config.tables,
            mappings_engine,
            patient_id_prefix=config.deidentification.patient_id_prefix,
            max_offset=config.deidentification.date_offset_days,
            random_seed=config.deidentification.random_seed,
            source_id_column=source_id_column,
        )
    finally:
        source.close()
        mappings_engine.dispose()

    print("\nMapping population complete.")
    print(f"  Patients:     {summary['patients_found']} found, {summary['patients_created']} created")
    print(f"  Encounters:   {summary['encounters_found']} found, {summary['encounters_created']} created")
    print(f"  Appointments: {summary['appointments_found']} found, {summary['appointments_created']} created")

    return summary


def mapping_command(
    config: str = typer.Option(..., "--config", "-c", help="Path to config.yaml"),
    mappings_db: Optional[str] = typer.Option(None, "--mappings-db", help="Override mappings DB path"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    """Create and populate mapping tables (patient, encounter, appointment)."""
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f"Error: Config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    run_mapping(str(config_path), mappings_db=mappings_db)
