#!/usr/bin/env python
"""Populate mappings.db — standalone wrapper for `deid mapping`.

Usage:
    python scripts/populate_mappings.py --config config.yaml
    python scripts/populate_mappings.py --config config.yaml --mappings-db ./custom_mappings.db
"""
from __future__ import annotations

import argparse

from dotenv import load_dotenv
load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Populate mappings.db by scanning source tables for distinct patient/encounter/appointment IDs.",
    )
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--mappings-db", default=None, help="Override mappings DB path (default: from config)")

    args = parser.parse_args()

    from deid.cli.mapping import run_mapping
    run_mapping(args.config, mappings_db=args.mappings_db)


if __name__ == "__main__":
    main()
