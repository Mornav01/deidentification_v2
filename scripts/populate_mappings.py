#!/usr/bin/env python
"""Populate mappings.db by scanning source tables for distinct patient/encounter/appointment IDs.

Usage:
    python scripts/populate_mappings.py --config config.yaml
    python scripts/populate_mappings.py --config config.yaml --mappings-db ./custom_mappings.db
"""
from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv
load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Populate mappings.db by scanning source tables for distinct patient/encounter/appointment IDs.",
    )
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument(
        "--mappings-db",
        default=None,
        help="Override mappings DB path (default: from config)",
    )

    args = parser.parse_args()

    # ── Load config ──────────────────────────────────────────────────────────
    from deid.config.loader import load_config

    config = load_config(args.config)

    mappings_db_path = args.mappings_db or config.mappings_db_path

    print(f"Source DB:    {config.source_db.type.value} '{config.source_db.database}' "
          f"at {config.source_db.host}:{config.source_db.port}")
    print(f"Mappings DB:  {mappings_db_path}")
    print(f"Tables:       {len(config.tables or [])}")

    # ── Create mappings engine + tables ──────────────────────────────────────
    from deid.models.base import create_mappings_engine, create_all_mappings_tables

    mappings_engine = create_mappings_engine(mappings_db_path)
    create_all_mappings_tables(mappings_engine)

    # ── Create source handler ────────────────────────────────────────────────
    from deid.core.dbPkg.dbhandler import NDDBHandler

    source = NDDBHandler(config.source_db.connection_string(), read_only=True)

    # ── Populate mappings ────────────────────────────────────────────────────
    try:
        from deid.core.mapping_populator import populate_mappings

        print("\nScanning source tables for IDs...")
        summary = populate_mappings(
            source,
            config.tables,
            mappings_engine,
            patient_id_prefix=config.deidentification.patient_id_prefix,
            max_offset=config.deidentification.date_offset_days,
        )
    except Exception:
        print("\nError during mapping population:", file=sys.stderr)
        raise
    finally:
        source.close()
        mappings_engine.dispose()

    # ── Print summary ────────────────────────────────────────────────────────
    print("\nMapping population complete.")
    print(f"  Patients:     {summary['patients_found']} found, {summary['patients_created']} created")
    print(f"  Encounters:   {summary['encounters_found']} found, {summary['encounters_created']} created")
    print(f"  Appointments: {summary['appointments_found']} found, {summary['appointments_created']} created")


if __name__ == "__main__":
    main()
