#!/usr/bin/env python
"""
Master Insurance Delta Script

This script updates the master_insurance_table in the given master schema
using deltas from the staging schema.

It is a .py version of NOTEBOOK/DENT/master_insurance_delta.ipynb.

Usage:
    python master_insurance_delta.py --master_schema "master_oct" --staging_schema "mobiledoc_staging"
"""

import argparse
import logging
import os

from sqlalchemy import MetaData, create_engine, text


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


def run_master_insurance_delta(master_schema: str, staging_schema: str):
    """
    Apply delta updates to master_insurance_table in master_schema from staging_schema.
    """
    logger.info(
        "Starting master insurance delta: master_schema=%s, staging_schema=%s",
        master_schema,
        staging_schema,
    )

    old_engine = create_mysql_engine(master_schema)
    source_engine = create_mysql_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    metadata = MetaData()

    # -------------------------------------------------------------------------
    # Load existing insurance patient IDs from master_insurance_table
    # -------------------------------------------------------------------------
    logger.info(
        "Loading existing patient_ids from %s.master_insurance_table", master_schema
    )
    with old_engine.connect() as conn:
        query = text(
            """
        SELECT DISTINCT
            patient_id
        FROM
            master_insurance_table
        """
        )
        result = conn.execute(query)
        insurance_data = result.fetchall()

    logger.info("Existing insurance rows: %d", len(insurance_data))

    existing_patient_ids = {row[0] for row in insurance_data}
    logger.info(
        "Number of existing Patient IDs in master_insurance_table: %d",
        len(existing_patient_ids),
    )

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
    # Categorize into insert/update lists
    # -------------------------------------------------------------------------
    INSURANCE_COLUMNS = [
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

    data_for_insurance_insert = []
    data_for_insurance_update = []
    total_records = len(ins_data)

    for row in ins_data:
        insurance_record = dict(zip(INSURANCE_COLUMNS, row))
        patient_id = insurance_record["patient_id"]

        if patient_id in existing_patient_ids:
            data_for_insurance_update.append(insurance_record)
        else:
            data_for_insurance_insert.append(insurance_record)

    logger.info(
        "Total Insurance records processed: %d, INSERT: %d, UPDATE: %d",
        total_records,
        len(data_for_insurance_insert),
        len(data_for_insurance_update),
    )

    # -------------------------------------------------------------------------
    # Execute inserts and updates on master_insurance_table
    # -------------------------------------------------------------------------
    if data_for_insurance_insert:
        cols = ", ".join(f"`{k}`" for k in INSURANCE_COLUMNS)
        params = ", ".join(f":{k}" for k in INSURANCE_COLUMNS)

        insert_query = text(
            f"""
        INSERT INTO master_insurance_table ({cols})
        VALUES ({params})
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(insert_query, data_for_insurance_insert)

        logger.info(
            "Inserted %d new insurance records", len(data_for_insurance_insert)
        )

    if data_for_insurance_update:
        set_clauses = [
            f"`{k}` = :{k}" for k in INSURANCE_COLUMNS if k != "patient_id"
        ]
        set_clause_str = ", ".join(set_clauses)

        update_query = text(
            f"""
        UPDATE master_insurance_table
        SET {set_clause_str}
        WHERE patient_id = :patient_id
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(update_query, data_for_insurance_update)

        logger.info(
            "Updated %d existing insurance records",
            len(data_for_insurance_update),
        )

    logger.info("Master insurance delta processing complete")


def main():
    parser = argparse.ArgumentParser(
        description="Apply master_insurance_table deltas"
    )
    parser.add_argument(
        "--master_schema",
        required=True,
        help="Master schema name (e.g. 'master_oct')",
    )
    parser.add_argument(
        "--staging_schema",
        required=True,
        help="Staging schema name (e.g. 'mobiledoc_staging')",
    )

    args = parser.parse_args()

    run_master_insurance_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
    )


if __name__ == "__main__":
    main()

