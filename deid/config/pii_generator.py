"""Auto-generate pii_tables_config and pii_config from table rules.

Mirrors the pattern of rules_generator.py — introspects configured tables
to find PII columns (MASK, PATIENT_DOB) and builds the config needed by
the PII table generator and notes de-identification.

Source DB access is strictly read-only.
"""
from __future__ import annotations

import logging
from pydantic import validate_call

try:
    import re2 as re
except ImportError:
    try:
        import regex as re  # type: ignore[no-redef]
    except ImportError:
        import re  # type: ignore[no-redef]

logger = logging.getLogger("deid.config")

# Rules whose columns carry PII values useful for notes masking.
_PII_RULES = {"MASK", "PATIENT_DOB"}

# ── Column-name → masking-value mapping (first match wins) ──────────────
_MASKING_PATTERNS = [
    (re.compile(r"(?i)(^|_)(first_?name|fname)($|_)"), "((FIRST_NAME))", "mask"),
    (re.compile(r"(?i)(^|_)(last_?name|lname)($|_)"), "((LAST_NAME))", "mask"),
    (re.compile(r"(?i)(^|_)(middle_?name|mname)($|_)"), "((MIDDLE_NAME))", "mask"),
    (re.compile(r"(?i)(^|_)(maiden_?name)($|_)"), "((MAIDEN_NAME))", "mask"),
    (re.compile(r"(?i)(^|_)(preferred_?name|nick_?name|display_?name)($|_)"), "((NAME))", "mask"),
    (re.compile(r"(?i)(^|_)(patient_?name|full_?name)($|_)"), "((PATIENT_NAME))", "mask"),
    (re.compile(r"(?i)(^|_)(ssn|social_?security|tax_?id|tin)($|_)"), "((SSN))", "mask"),
    (re.compile(r"(?i)(^|_)(phone|fax|cell|mobile|home_?phone|work_?phone|phone_?number)($|_)"), "((PHONE))", "mask"),
    (re.compile(r"(?i)(^|_)(email|e_?mail|email_?address)($|_)"), "((EMAIL))", "mask"),
    (re.compile(r"(?i)(^|_)(address|addr|street|address_?line)($|_)"), "((ADDRESS))", "mask"),
    (re.compile(r"(?i)(^|_)(city)($|_)"), "((CITY))", "mask"),
    (re.compile(r"(?i)(^|_)(state)($|_)"), "((STATE))", "mask"),
    (re.compile(r"(?i)(^|_)(county)($|_)"), "((COUNTY))", "mask"),
    (re.compile(r"(?i)(^|_)(dob|date_?of_?birth|birth_?date|patientdob)($|_)"), None, "dob"),
]

# Patterns for first-name / last-name columns (used to build combine rules).
_FIRST_NAME_PAT = re.compile(r"(?i)(^|_)(first_?name|fname)($|_)")
_LAST_NAME_PAT = re.compile(r"(?i)(^|_)(last_?name|lname)($|_)")


@validate_call(config=dict(arbitrary_types_allowed=True))
def _classify_column(column_name: str) -> tuple[str | None, str]:
    """Return (masking_value, category) for a PII column, or (None, '') if unknown."""
    for pattern, masking_value, category in _MASKING_PATTERNS:
        if pattern.search(column_name):
            return masking_value, category
    return None, "mask"


@validate_call(config=dict(arbitrary_types_allowed=True))
def generate_pii_tables_config(
    tables: list,
) -> dict:
    """Build pii_tables_config from configured table rules.

    Scans each table's rules for PATIENT_ID + MASK/PATIENT_DOB columns and
    groups them into a pii_data_table definition.

    Returns dict suitable for PIITable.generate_pii_tables().
    """
    pii_source_tables: dict[str, dict] = {}

    for table_cfg in tables:
        patient_id_col = None
        pii_columns = []

        for col_name, rule in table_cfg.rules.items():
            if rule == "PATIENT_ID":
                patient_id_col = col_name
            elif rule in _PII_RULES:
                pii_columns.append(col_name)

        if patient_id_col and pii_columns:
            pii_source_tables[table_cfg.name] = {
                "primary_col": patient_id_col,
                "other_required_columns": pii_columns,
            }

    if not pii_source_tables:
        return {}

    return {
        "pii_data_table": {
            "primary_column_name": "patient_id",
            "upsert_instead_of_append": True,
            "tables": pii_source_tables,
        },
    }


@validate_call(config=dict(arbitrary_types_allowed=True))
def generate_pii_config(
    pii_tables_config: dict,
) -> dict:
    """Build pii_config (mask/dob/combine) from pii_tables_config.

    Uses the {source_table}_{column} naming convention to produce
    keys that match the generated pii_data_table columns.
    """
    mask: dict[str, dict] = {}
    dob: dict[str, dict] = {}
    first_name_cols: list[str] = []
    last_name_cols: list[str] = []

    for _pii_table_name, pii_table_def in pii_tables_config.items():
        for source_table, source_conf in pii_table_def.get("tables", {}).items():
            for col in source_conf.get("other_required_columns", []):
                prefixed = f"{source_table}_{col}"
                masking_value, category = _classify_column(col)

                if category == "dob":
                    dob[prefixed] = {}
                elif category == "mask" and masking_value:
                    mask[prefixed] = {
                        "masking_value": masking_value,
                        "min_length": 2,
                    }

                if _FIRST_NAME_PAT.search(col):
                    first_name_cols.append(prefixed)
                elif _LAST_NAME_PAT.search(col):
                    last_name_cols.append(prefixed)

    config: dict = {}
    if mask:
        config["mask"] = mask
    if dob:
        config["dob"] = dob

    # Build combine rules for first+last name pairs.
    combine: dict[str, dict] = {}
    for fn_col in first_name_cols:
        # Find matching last-name column from the same source table.
        prefix = fn_col.rsplit("_", 1)[0]  # e.g. "users_fname" → "users"
        # Match by shared table prefix
        for ln_col in last_name_cols:
            ln_prefix = ln_col.rsplit("_", 1)[0]
            if prefix == ln_prefix:
                rule_name = f"{prefix}_full_name"
                combine[rule_name] = {
                    "combine": [fn_col, ln_col],
                    "masking_value": "((PATIENT_NAME))",
                }
                break
    if combine:
        config["combine"] = combine

    return config
