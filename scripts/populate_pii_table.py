#!/usr/bin/env python
"""Create PII tables — standalone wrapper for `deid pii-table`.

Usage:
    python scripts/populate_pii_table.py --config config.yaml
    python scripts/populate_pii_table.py --config config.yaml --pii-config-output ./pii_config.yaml
"""
from __future__ import annotations

import argparse

from dotenv import load_dotenv
load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Create PII tables in the PII DB and generate pii_config YAML.",
    )
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--pii-config-output", default=None, help="Output path for pii_config YAML")

    args = parser.parse_args()

    from deid.cli.pii_table import run_pii_table
    run_pii_table(args.config, pii_config_output=args.pii_config_output)


if __name__ == "__main__":
    main()
