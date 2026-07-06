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

from sqlalchemy import create_engine, text


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
    """Return {patientid (raw source id) → nd_patient_id} from patient_mapping_table."""
    engine = create_mysql_engine(mapping_schema)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT patientid, nd_patient_id FROM patient_mapping_table")
        ).fetchall()
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
    # Resolve nd_patient_id, dedup to one row per patient (last wins), build payload
    # -------------------------------------------------------------------------
    by_nd_patient: dict[int, dict] = {}
    unmapped = 0
    for row in ins_data:
        source_record = dict(zip(SOURCE_COLUMNS, row))
        patient_id = source_record.pop("patient_id")
        nd_patient_id = patient_map.get(patient_id)
        if nd_patient_id is None:
            unmapped += 1
            continue
        # One row per patient (uq_nd_patient_id); later source rows overwrite earlier.
        by_nd_patient[nd_patient_id] = {
            "nd_patient_id": nd_patient_id,
            "patientid": patient_id,
            **source_record,
        }

    data_for_upsert = list(by_nd_patient.values())
    logger.info(
        "Insurance source rows: %d, distinct mapped patients: %d, unmapped (skipped): %d",
        len(ins_data),
        len(data_for_upsert),
        unmapped,
    )
    if unmapped:
        logger.warning(
            "%d insurance rows had no nd_patient_id mapping and were skipped. "
            "Run mapping_delta before master_insurance_delta so every patient is mapped.",
            unmapped,
        )

    # -------------------------------------------------------------------------
    # Upsert into master_insurance_table (keyed on uq_nd_patient_id)
    # -------------------------------------------------------------------------
    if data_for_upsert:
        # nd_patient_id is the upsert key; patientid + insurance columns are the values.
        value_cols = ["patientid"] + INSURANCE_VALUE_COLUMNS
        all_cols = ["nd_patient_id"] + value_cols
        cols = ", ".join(f"`{k}`" for k in all_cols)
        params = ", ".join(f":{k}" for k in all_cols)
        # NULL source values never clobber an existing stored value.
        updates = ", ".join(
            f"`{k}` = COALESCE(VALUES(`{k}`), `{k}`)" for k in value_cols
        )

        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        upsert_query = text(
            f"""
        INSERT INTO master_insurance_table ({cols})
        VALUES ({params})
        ON DUPLICATE KEY UPDATE {updates}
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(upsert_query, data_for_upsert)

        logger.info("Upserted %d insurance records", len(data_for_upsert))
    else:
        logger.info("No insurance records to upsert")

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
