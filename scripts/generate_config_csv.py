#!/usr/bin/env python
"""Query the source database from config.yaml and generate a config CSV.

Auto-assigns obvious de-identification rules based on column name patterns.
The 'rule' column can be reviewed and edited by the user before use.

Usage:
    python scripts/generate_config_csv.py --config config.yaml -o config_rules.csv

    # Filter to specific tables:
    python scripts/generate_config_csv.py --config config.yaml --tables patients encounters -o config_rules.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Any

try:
    import re2 as re
except ImportError:
    try:
        import regex as re  # type: ignore[no-redef]
    except ImportError:
        import re  # type: ignore[no-redef]

import yaml
from sqlalchemy import create_engine, event, inspect

from deid.config.schema import DbConfig
from deid.config.rules_generator import auto_assign_rule

from dotenv import load_dotenv
load_dotenv()


_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _interpolate_env_vars(obj: Any) -> Any:
    """Recursively replace ${VAR_NAME} with os.environ[VAR_NAME]."""
    if isinstance(obj, str):
        def _replacer(match):
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"Environment variable '{var}' not set (referenced in config)")
            return val
        return _ENV_VAR_PATTERN.sub(_replacer, obj)
    elif isinstance(obj, dict):
        return {k: _interpolate_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_interpolate_env_vars(item) for item in obj]
    return obj


def load_source_db(config_path: str) -> DbConfig:
    """Load only the source_db section from config.yaml."""
    with open(config_path) as f:
        raw = yaml.safe_load(f)
    interpolated = _interpolate_env_vars(raw)
    return DbConfig(**interpolated["source_db"])


def _make_readonly_engine(source_db: DbConfig):
    """Create a read-only SQLAlchemy engine. No writes will reach the source DB."""
    conn_str = source_db.connection_string()
    engine = create_engine(conn_str)

    @event.listens_for(engine, "begin")
    def _set_readonly(conn):
        # MySQL / MariaDB
        if source_db.type.value == "mysql":
            conn.exec_driver_sql("SET SESSION TRANSACTION READ ONLY")
        # PostgreSQL
        elif source_db.type.value == "postgresql":
            conn.exec_driver_sql("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        # MSSQL — no session-level read-only; rely on metadata-only queries

    return engine


def extract_schema(source_db: DbConfig, tables: list[str] | None = None, schema: str | None = None) -> list[dict]:
    """Connect to database (read-only) and extract table/column metadata with auto-assigned rules."""
    engine = _make_readonly_engine(source_db)
    insp = inspect(engine)

    all_tables = insp.get_table_names(schema=schema)
    if tables:
        missing = set(tables) - set(all_tables)
        if missing:
            print(f"Warning: tables not found in database: {missing}", file=sys.stderr)
        all_tables = [t for t in all_tables if t in set(tables)]

    rows = []
    for table_name in sorted(all_tables):
        columns = insp.get_columns(table_name, schema=schema)
        for col in columns:
            data_type = str(col["type"])
            rows.append({
                "table_name": table_name,
                "column_name": col["name"],
                "data_type": data_type,
                "rule": auto_assign_rule(col["name"], data_type),
            })

    engine.dispose()
    return rows


def write_csv(rows: list[dict], output_path: str) -> None:
    fieldnames = ["table_name", "column_name", "data_type", "rule"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict]) -> None:
    tables = set(r["table_name"] for r in rows)
    assigned = [r for r in rows if r["rule"]]
    unassigned = [r for r in rows if not r["rule"]]

    rule_counts: dict[str, int] = {}
    for r in assigned:
        rule_counts[r["rule"]] = rule_counts.get(r["rule"], 0) + 1

    print(f"\nFound {len(rows)} columns across {len(tables)} tables.")
    print(f"  Auto-assigned: {len(assigned)} columns")
    print(f"  Unassigned:    {len(unassigned)} columns")
    if rule_counts:
        print("\n  Rule breakdown:")
        for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
            print(f"    {rule:<20s} {count}")


def main():
    parser = argparse.ArgumentParser(description="Extract database schema to a config CSV for de-identification rules.")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--schema", default=None, help="Database schema (for databases that support it)")
    parser.add_argument("--tables", nargs="+", help="Only include these tables (default: all)")
    parser.add_argument("--output", "-o", default="config_rules.csv", help="Output CSV path (default: config_rules.csv)")

    args = parser.parse_args()

    source_db = load_source_db(args.config)
    print(f"Connecting to {source_db.type.value} database '{source_db.database}' at {source_db.host}:{source_db.port}...")

    rows = extract_schema(source_db, tables=args.tables, schema=args.schema)
    print_summary(rows)

    write_csv(rows, args.output)
    print(f"\nConfig CSV written to: {args.output}")
    print("Review the 'rule' column and fill in any unassigned columns before use.")


if __name__ == "__main__":
    main()
