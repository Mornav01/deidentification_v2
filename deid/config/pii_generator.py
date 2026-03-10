"""Auto-generate pii_tables_config and pii_config.

Two strategies (tried in order):
1. From configured table rules — if the deid tables themselves contain
   MASK / PATIENT_DOB columns alongside a PATIENT_ID column.
2. From source-DB introspection — scan ALL source tables for columns
   whose names match PII patterns (names, SSN, DOB, etc.) and a
   patient-ID primary key.  Source access is strictly read-only.
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

# ── Column-name patterns for PII detection ───────────────────────────────
_PATIENT_ID_PAT = re.compile(r"(?i)^(patient_?id|pat_?id|pid)$")

_PII_COLUMN_PATTERNS = [
    re.compile(r"(?i)(^|_)(first_?name|last_?name|middle_?name|patient_?name|full_?name|fname|lname|mname)($|_)"),
    re.compile(r"(?i)(^|_)(maiden_?name|preferred_?name|nick_?name|display_?name)($|_)"),
    re.compile(r"(?i)(^|_)(ssn|social_?security|tax_?id|tin)($|_)"),
    re.compile(r"(?i)(^|_)(phone|fax|cell|mobile|home_?phone|work_?phone|phone_?number)($|_)"),
    re.compile(r"(?i)(^|_)(email|e_?mail|email_?address)($|_)"),
    re.compile(r"(?i)(^|_)(address|addr|street|address_?line|city|state|county)($|_)"),
    re.compile(r"(?i)(^|_)(dob|date_?of_?birth|birth_?date|patientdob)($|_)"),
]

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
def _classify_column(column_name: str) -> tuple[str, str]:
    """Return (masking_value, category) for a column.

    Known PII patterns get specific masking values; unrecognized columns
    get a generic ``((COLUMN_NAME))`` mask so they are still included.
    """
    for pattern, masking_value, category in _MASKING_PATTERNS:
        if pattern.search(column_name):
            if masking_value is None:
                masking_value = f"(({column_name.upper()}))"
            return masking_value, category
    return f"(({column_name.upper()}))", "mask"


# ── Strategy 1: from table rules ─────────────────────────────────────────

@validate_call(config=dict(arbitrary_types_allowed=True))
def _from_table_rules(tables: list) -> dict[str, dict]:
    """Find PII source tables from the configured deid table rules."""
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

    return pii_source_tables


# ── Strategy 2: introspect source DB ─────────────────────────────────────

def _from_source_db(source_db) -> dict[str, dict]:
    """Scan all source tables (read-only) for tables with patient_id + PII columns.

    Only columns matching known PII patterns are included.
    """
    from sqlalchemy import inspect as sa_inspect

    from deid.core.dbPkg.dbhandler import create_read_only_engine

    engine = create_read_only_engine(source_db.connection_string())
    insp = sa_inspect(engine)

    pii_source_tables: dict[str, dict] = {}

    for table_name in insp.get_table_names():
        columns = insp.get_columns(table_name)
        col_names = [c["name"] for c in columns]

        # Find a patient-ID column.
        patient_id_col = None
        for cn in col_names:
            if _PATIENT_ID_PAT.search(cn):
                patient_id_col = cn
                break
        if not patient_id_col:
            continue

        # Only include columns matching PII patterns.
        pii_cols = [
            cn for cn in col_names
            if cn != patient_id_col and _is_pii_column(cn)
        ]
        if pii_cols:
            pii_source_tables[table_name] = {
                "primary_col": patient_id_col,
                "other_required_columns": pii_cols,
            }

    engine.dispose()

    if pii_source_tables:
        logger.info(
            "PII introspection: found %d source table(s) with PII columns: %s",
            len(pii_source_tables),
            ", ".join(pii_source_tables.keys()),
        )

    return pii_source_tables


# ── Public API ────────────────────────────────────────────────────────────

def generate_pii_tables_config(tables: list, source_db=None) -> dict:
    """Build pii_tables_config, trying table rules first then DB introspection.

    Returns dict suitable for PIITable.generate_pii_tables(), or {} if
    no PII source tables could be identified.
    """
    pii_source_tables = _from_table_rules(tables)

    if not pii_source_tables and source_db is not None:
        logger.info(
            "No PII columns found in configured table rules — "
            "introspecting source database..."
        )
        pii_source_tables = _from_source_db(source_db)

    if not pii_source_tables:
        logger.warning(
            "Could not auto-generate pii_tables_config: no tables with "
            "patient_id + PII columns found. Provide pii_tables_config "
            "manually in config.yaml."
        )
        return {}

    return {
        "pii_data_table": {
            "primary_column_name": "patient_id",
            "upsert_instead_of_append": True,
            "tables": pii_source_tables,
        },
    }


@validate_call(config=dict(arbitrary_types_allowed=True))
def generate_pii_config(pii_tables_config: dict) -> dict:
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

    # Build combine rules for first+last name pairs from the same source table.
    combine: dict[str, dict] = {}
    for fn_col in first_name_cols:
        # Extract table prefix: "users_fname" → "users"
        prefix = fn_col.rsplit("_", 1)[0]
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
