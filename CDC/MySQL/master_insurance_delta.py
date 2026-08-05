#!/usr/bin/env python
"""
Master Insurance Delta Script  (new master_pg structure)

This script updates the master_insurance_table in the given master schema
using deltas from the staging schema.

It is a .py version of NOTEBOOK/DENT/master_insurance_delta.ipynb.

New master_pg structure: master_insurance_table is keyed on nd_patient_id (the raw
patient_id column has been dropped — see mapping_migration_to_new_structure_dent.sql).
This script resolves each source patient_id → nd_patient_id via patient_mapping_table
in --mapping_schema, dedups to one row per patient (insurance is one-row-per-patient),
and writes with a single INSERT ... ON DUPLICATE KEY UPDATE keyed on uq_nd_patient_id.
NULL source values never overwrite an existing value (COALESCE).

Usage:
    python master_insurance_delta.py --master_schema "master_pg" \
        --staging_schema "mobiledoc_staging" --mapping_schema "mapping_pg"
"""

import argparse
import logging
import os

from sqlalchemy import create_engine, inspect, text


MYSQL_USER = os.environ.get("DB_USER", "")
MYSQL_PASS = os.environ.get("DB_PASS", "")
MYSQL_HOST = os.environ.get("DB_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("DB_PORT", "3306"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def create_mysql_engine(schema: str):
    """Create SQLAlchemy engine for a given schema."""
    url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{schema}"
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)


def load_patient_map(mapping_schema: str) -> dict:
    """Return {patientid (raw source id) → nd_patient_id} from patient_mapping_table.

    Excluded patients are omitted. mapping_delta keeps them in the mapping table
    (flagged `excluded = 1`) so their nd_patient_id is stable, but their data must
    never reach the master tables. The column guard keeps this working against a
    mapping schema that predates the flag.
    """
    engine = create_mysql_engine(mapping_schema)
    cols = {c["name"] for c in inspect(engine).get_columns("patient_mapping_table")}
    query = "SELECT patientid, nd_patient_id FROM patient_mapping_table"
    if "excluded" in cols:
        query += " WHERE excluded = 0"
    else:
        logger.warning(
            "patient_mapping_table has no `excluded` column — loading every patient. "
            "Run mapping_delta.py once to add it."
        )
    with engine.connect() as conn:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        rows = conn.execute(text(query)).fetchall()
    return {row[0]: row[1] for row in rows}


def run_master_insurance_delta(master_schema: str, staging_schema: str, mapping_schema: str):
    """
    Apply delta updates to master_insurance_table in master_schema from staging_schema,
    keyed on nd_patient_id resolved via mapping_schema.
    """
    logger.info(
        "Starting master insurance delta: master_schema=%s, staging_schema=%s, mapping_schema=%s",
        master_schema,
        staging_schema,
        mapping_schema,
    )

    source_engine = create_mysql_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    # -------------------------------------------------------------------------
    # Load patient_id → nd_patient_id map
    # -------------------------------------------------------------------------
    patient_map = load_patient_map(mapping_schema)
    logger.info("Loaded %d patient_id → nd_patient_id mappings", len(patient_map))

    # -------------------------------------------------------------------------
    # Load insurance source data from staging (hcfa + edi_invoice)
    # -------------------------------------------------------------------------
    logger.info("Loading insurance data from %s.hcfa / %s.edi_invoice", staging_schema, staging_schema)
    with source_engine.connect() as conn:
        query = text(
            """
        SELECT
            b.patientid AS patient_id,
            NULL AS encounter_id,
            a.PName AS hcfa_PName,
            a.InsuredName AS hcfa_InsuredName,
            a.InsuredId AS hcfa_InsuredId,
            a.PAddressStreet AS hcfa_PAddressStreet,
            a.PAddressCity AS hcfa_PAddressCity,
            a.InsuredAddStreet AS hcfa_InsuredAddStreet,
            a.InsuredAddCity AS hcfa_InsuredAddCity,
            a.NameRef AS hcfa_NameRef,
            a.SSN AS hcfa_SSN,
            a.EmpOrSchoolName AS hcfa_EmpOrSchoolName,
            a.InsurancePlan AS hcfa_InsurancePlan,
            a.OtherInsuredName AS hcfa_OtherInsuredName,
            a.OtherEmpName AS hcfa_OtherEmpName,
            a.OtherInsuranceName AS hcfa_OtherInsuranceName,
            a.PayorName1 AS hcfa_PayorName1,
            a.PayorName2 AS hcfa_PayorName2,
            a.PayorName3 AS hcfa_PayorName3,
            a.PayorAddress11 AS hcfa_PayorAddress11,
            a.PayorAddress12 AS hcfa_PayorAddress12,
            a.PayorAddress13 AS hcfa_PayorAddress13,
            a.PayorAddress21 AS hcfa_PayorAddress21,
            a.PayorAddress22 AS hcfa_PayorAddress22,
            a.PayorAddress23 AS hcfa_PayorAddress23,
            a.PayorCity1 AS hcfa_PayorCity1,
            a.PayorZip1 AS hcfa_PayorZip1,
            a.PayorCity2 AS hcfa_PayorCity2,
            a.PayorZip2 AS hcfa_PayorZip2,
            a.PayorCity3 AS hcfa_PayorCity3,
            a.PayorZip3 AS hcfa_PayorZip3
        FROM hcfa a
        INNER JOIN edi_invoice b ON a.invid = b.id
        """
        )
        result = conn.execute(query)
        ins_data = result.fetchall()

    logger.info("Loaded %d insurance source rows", len(ins_data))

    # -------------------------------------------------------------------------
    # Columns written to master_insurance_table (nd_patient_id replaces patient_id)
    # -------------------------------------------------------------------------
    SOURCE_COLUMNS = [
        "patient_id",
        "encounter_id",
        "hcfa_PName",
        "hcfa_InsuredName",
        "hcfa_InsuredId",
        "hcfa_PAddressStreet",
        "hcfa_PAddressCity",
        "hcfa_InsuredAddStreet",
        "hcfa_InsuredAddCity",
        "hcfa_NameRef",
        "hcfa_SSN",
        "hcfa_EmpOrSchoolName",
        "hcfa_InsurancePlan",
        "hcfa_OtherInsuredName",
        "hcfa_OtherEmpName",
        "hcfa_OtherInsuranceName",
        "hcfa_PayorName1",
        "hcfa_PayorName2",
        "hcfa_PayorName3",
        "hcfa_PayorAddress11",
        "hcfa_PayorAddress12",
        "hcfa_PayorAddress13",
        "hcfa_PayorAddress21",
        "hcfa_PayorAddress22",
        "hcfa_PayorAddress23",
        "hcfa_PayorCity1",
        "hcfa_PayorZip1",
        "hcfa_PayorCity2",
        "hcfa_PayorZip2",
        "hcfa_PayorCity3",
        "hcfa_PayorZip3",
    ]
    # Value columns actually stored (patient_id is only used to resolve nd_patient_id)
    INSURANCE_VALUE_COLUMNS = [c for c in SOURCE_COLUMNS if c != "patient_id"]

    # -------------------------------------------------------------------------
    # Resolve nd_patient_id, keep ALL rows (insurance is multi-row per patient)
    # -------------------------------------------------------------------------
    rows_to_insert = []
    nd_patient_ids: set[int] = set()
    unmapped = 0
    for row in ins_data:
        source_record = dict(zip(SOURCE_COLUMNS, row))
        patient_id = source_record.pop("patient_id")
        nd_patient_id = patient_map.get(patient_id)
        if nd_patient_id is None:
            unmapped += 1
            continue
        rows_to_insert.append(
            {"nd_patient_id": nd_patient_id, "patientid": patient_id, **source_record}
        )
        nd_patient_ids.add(nd_patient_id)

    logger.info(
        "Insurance source rows: %d, resolved rows: %d, distinct patients: %d, unmapped (skipped): %d",
        len(ins_data),
        len(rows_to_insert),
        len(nd_patient_ids),
        unmapped,
    )
    if unmapped:
        logger.warning(
            "%d insurance rows had no nd_patient_id mapping and were skipped. "
            "Run mapping_delta before master_insurance_delta so every patient is mapped.",
            unmapped,
        )

    # -------------------------------------------------------------------------
    # Refresh into master_insurance_table (multi-row): for every patient present
    # in this delta, DELETE their existing rows then INSERT the fresh set. Patients
    # not in the delta are untouched. One transaction.
    # -------------------------------------------------------------------------
    if rows_to_insert:
        all_cols = ["nd_patient_id", "patientid"] + INSURANCE_VALUE_COLUMNS
        cols = ", ".join(f"`{k}`" for k in all_cols)
        params = ", ".join(f":{k}" for k in all_cols)
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        insert_query = text(f"INSERT INTO master_insurance_table ({cols}) VALUES ({params})")

        ids = list(nd_patient_ids)
        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            for i in range(0, len(ids), 1000):
                chunk = ids[i:i + 1000]
                placeholders = ", ".join(f":p{j}" for j in range(len(chunk)))
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                conn.execute(
                    text(f"DELETE FROM master_insurance_table WHERE nd_patient_id IN ({placeholders})"),
                    {f"p{j}": v for j, v in enumerate(chunk)},
                )
            conn.execute(insert_query, rows_to_insert)

        logger.info(
            "Refreshed insurance for %d patients (%d rows)", len(ids), len(rows_to_insert)
        )
    else:
        logger.info("No insurance records to write")

    logger.info("Master insurance delta processing complete")


def main():
    parser = argparse.ArgumentParser(
        description="Apply master_insurance_table deltas"
    )
    parser.add_argument(
        "--master_schema",
        required=True,
        help="Master schema name (e.g. 'master_pg')",
    )
    parser.add_argument(
        "--staging_schema",
        required=True,
        help="Staging schema name (e.g. 'mobiledoc_staging')",
    )
    parser.add_argument(
        "--mapping_schema",
        required=True,
        help="Mapping schema name for nd_patient_id resolution (e.g. 'mapping_pg')",
    )

    args = parser.parse_args()

    run_master_insurance_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
        mapping_schema=args.mapping_schema,
    )


if __name__ == "__main__":
    main()
