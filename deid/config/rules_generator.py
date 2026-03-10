"""Auto-generate a config_rules CSV by introspecting the source database.

Reusable by both `scripts/generate_config_csv.py` (CLI) and the config
loader (auto-generate when the CSV doesn't exist yet).
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from pydantic import validate_call

try:
    import re2 as re
except ImportError:
    try:
        import regex as re  # type: ignore[no-redef]
    except ImportError:
        import re  # type: ignore[no-redef]

from sqlalchemy import inspect

from deid.config.schema import DbConfig
from deid.core.dbPkg.dbhandler import create_read_only_engine

logger = logging.getLogger("deid.config")

# ── Auto-assignment patterns (first match wins) ─────────────────────────────
COLUMN_RULES = [
    (re.compile(r"(?i)^patient_?id$"), "PATIENT_ID"),
    (re.compile(r"(?i)^pat_?id$"), "PATIENT_ID"),
    (re.compile(r"(?i)^pid$"), "PATIENT_ID"),
    (re.compile(r"(?i)^encounter_?id$"), "ENCOUNTER_ID"),
    (re.compile(r"(?i)^enc_?id$"), "ENCOUNTER_ID"),
    (re.compile(r"(?i)^visit_?id$"), "ENCOUNTER_ID"),
    (re.compile(r"(?i)^appo?intment_?id$"), "APPOINTMENT_ID"),
    (re.compile(r"(?i)^appt_?id$"), "APPOINTMENT_ID"),
    (re.compile(r"(?i)(^|_)(dob|date_?of_?birth|birth_?date|patientdob)($|_)"), "PATIENT_DOB"),
    (re.compile(r"(?i)(^|_)(date|datetime|_dt|_date|timestamp|_time|_ts)($|_)"), "DATE_OFFSET"),
    (re.compile(r"(?i)(date|time)$"), "DATE_OFFSET"),
    (re.compile(r"(?i)(^|_)(zip|zip_?code|postal_?code|zipcode)($|_)"), "ZIP_CODE"),
    (re.compile(r"(?i)(^|_)(first_?name|last_?name|middle_?name|patient_?name|full_?name|fname|lname|mname)($|_)"), "MASK"),
    (re.compile(r"(?i)(^|_)(maiden_?name|preferred_?name|nick_?name|display_?name)($|_)"), "MASK"),
    (re.compile(r"(?i)(^|_)(ssn|social_?security|tax_?id|tin)($|_)"), "MASK"),
    (re.compile(r"(?i)(^|_)(phone|fax|cell|mobile|home_?phone|work_?phone|phone_?number)($|_)"), "MASK"),
    (re.compile(r"(?i)(^|_)(email|e_?mail|email_?address)($|_)"), "MASK"),
    (re.compile(r"(?i)(^|_)(address|addr|street|address_?line|city|state|county)($|_)"), "MASK"),
    (re.compile(r"(?i)(^|_)(notes?|comment|narrative|description|free_?text|remarks|memo|clinical_?notes?)($|_)"), "GENERIC_NOTES"),
    (re.compile(r"(?i)(^|_)(doc_?content|document_?text|document_?body|doc_?text|doc_?body|bin_?content|blob_?content|text_?content|content_?text|report_?text|clinical_?text|note_?text)($|_)"), "GENERIC_NOTES"),
]

DATE_TYPE_PATTERN = re.compile(r"(?i)(DATE|TIME|TIMESTAMP)")
LARGE_TEXT_TYPE_PATTERN = re.compile(r"(?i)(LONGTEXT|MEDIUMTEXT|NTEXT|NVARCHAR\s*\(\s*MAX\s*\)|(?<!TINY)TEXT\b)")


@validate_call(config=dict(arbitrary_types_allowed=True))
def auto_assign_rule(column_name: str, data_type: str) -> str:
    """Return a rule string if the column name/type matches known patterns, else ''."""
    for pattern, rule in COLUMN_RULES:
        if pattern.search(column_name):
            return rule
    if DATE_TYPE_PATTERN.search(data_type):
        return "DATE_OFFSET"
    # Large text columns likely contain free-text that needs de-identification
    if LARGE_TEXT_TYPE_PATTERN.search(data_type):
        return "GENERIC_NOTES"
    return ""


@validate_call(config=dict(arbitrary_types_allowed=True))
def extract_schema(source_db: DbConfig, tables: list[str] | None = None) -> list[dict]:
    """Connect to source DB (read-only) and extract table/column metadata with auto-assigned rules."""
    engine = create_read_only_engine(source_db.connection_string())
    insp = inspect(engine)

    all_tables = insp.get_table_names()
    if tables:
        all_tables = [t for t in all_tables if t in set(tables)]

    rows = []
    for table_name in sorted(all_tables):
        columns = insp.get_columns(table_name)
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


@validate_call(config=dict(arbitrary_types_allowed=True))
def write_csv(rows: list[dict], output_path: str) -> None:
    """Write extracted schema rows to a CSV file."""
    fieldnames = ["table_name", "column_name", "data_type", "rule"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


@validate_call(config=dict(arbitrary_types_allowed=True))
def generate_rules_csv(source_db: DbConfig, output_path: str) -> str:
    """Generate a config_rules CSV from the source database schema.

    Returns the path to the generated CSV.
    """
    logger.info("rules_csv not found at '%s' — generating from source database...", output_path)
    rows = extract_schema(source_db)

    tables = set(r["table_name"] for r in rows)
    assigned = [r for r in rows if r["rule"]]
    logger.info(
        "Found %d columns across %d tables (%d auto-assigned rules).",
        len(rows), len(tables), len(assigned),
    )

    write_csv(rows, output_path)
    logger.info("Config CSV written to: %s", output_path)
    return output_path
