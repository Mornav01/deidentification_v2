#!/usr/bin/env python
"""Generate a config rules CSV — standalone wrapper for `deid generate-config`.

Usage:
    python scripts/generate_config_csv.py --config config.yaml -o config_rules.csv
    python scripts/generate_config_csv.py --config config.yaml --tables patients encounters -o config_rules.csv
"""
from __future__ import annotations

import argparse

from dotenv import load_dotenv
load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Extract database schema to a config CSV for de-identification rules.",
    )
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--schema", default=None, help="Database schema (for databases that support it)")
    parser.add_argument("--tables", nargs="+", help="Only include these tables (default: all)")
    parser.add_argument("--output", "-o", default=None, help="Output CSV path (default: from config.rules_csv or config_rules.csv)")

    args = parser.parse_args()

    from deid.cli.generate_config import run_generate_config
    run_generate_config(args.config, schema=args.schema, tables=args.tables, output=args.output)


if __name__ == "__main__":
    main()
