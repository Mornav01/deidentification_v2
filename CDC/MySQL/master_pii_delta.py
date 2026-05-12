#!/usr/bin/env python
"""
Master PII Delta Script

This script updates the PII master table (pii_data_table) in the given master schema
using deltas from the staging schema.

It is a .py version of NOTEBOOK/DENT/master_pii_delta.ipynb.

Usage:
    python master_pii_delta.py --master_schema "master" --staging_schema "mobiledoc_apr26_staging"
"""

import argparse
import logging
import os
from datetime import datetime, timezone

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


def run_master_pii_delta(master_schema: str, staging_schema: str):
    """
    Apply delta updates to pii_data_table in master_schema from staging_schema.
    """
    logger.info(
        "Starting master PII delta: master_schema=%s, staging_schema=%s",
        master_schema,
        staging_schema,
    )

    old_engine = create_mysql_engine(master_schema)
    source_engine = create_mysql_engine(staging_schema)
    dest_engine = create_mysql_engine(master_schema)

    metadata = MetaData()

    # -------------------------------------------------------------------------
    # Load existing PII patient IDs from master
    # -------------------------------------------------------------------------
    logger.info("Loading existing patient_ids from %s.pii_data_table", master_schema)
    with old_engine.connect() as conn:
        query = text(
            """
        SELECT DISTINCT
            patient_id
        FROM
            pii_data_table
        """
        )
        result = conn.execute(query)
        pii_data = result.fetchall()

    logger.info("Existing PII rows: %d", len(pii_data))

    existing_patient_ids = {row[0] for row in pii_data}
    logger.info(
        "Number of existing Patient IDs in pii_data_table: %d",
        len(existing_patient_ids),
    )

    # -------------------------------------------------------------------------
    # Load source user and patient info from staging
    # -------------------------------------------------------------------------
    logger.info("Loading users data from %s.users", staging_schema)
    with source_engine.connect() as conn:
        query = text(
            """
        SELECT
            uid AS patient_id,
            uname AS users_uname,
            UserType AS users_UserType,
            upwd AS users_upwd,
            umobileno AS users_umobileno,
            upagerno AS users_upagerno,
            ufname AS users_ufname,
            uminitial AS users_uminitial,
            ulname AS users_ulname,
            uemail AS users_uemail,
            upaddress AS users_upaddress,
            upcity AS users_upcity,
            upPhone AS users_upPhone,
            dob AS users_dob,
            ssn AS users_ssn,
            upaddress2 AS users_upaddress2,
            initials AS users_initials,
            ptDob AS users_ptDob,
            upreviousname AS users_upreviousname
        FROM
            users
        """
        )
        result = conn.execute(query)
        users_data = result.fetchall()

    logger.info("Loaded %d user records", len(users_data))

    logger.info("Loading patient data from %s.patients", staging_schema)
    with source_engine.connect() as conn:
        query = text(
            """
        SELECT
            pid AS patient_id,
            controlno as patients_controlno,
            employername AS patients_employername,
            employeraddress AS patients_employeraddress,
            employeraddress2 AS patients_employeraddress2,
            employercity AS patients_employercity,
            employerPhone AS patients_employerPhone,
            insname AS patients_insname,
            insgroupno AS patients_insgroupno,
            inssubscriberno AS patients_inssubscriberno,
            inscopay AS patients_inscopay,
            insname2 AS patients_insname2,
            insgroupno2 AS patients_insgroupno2,
            inssubscriberno2 AS patients_inssubscriberno2,
            inscopay2 AS patients_inscopay2,
            straddress AS patients_straddress,
            city AS patients_city,
            insId AS patients_insId,
            insId2 AS patients_insId2,
            strAddress2 AS patients_strAddress2,
            GrId AS patients_GrId,
            preferred_name AS patients_preferred_name
        FROM
            patients
        """
        )
        result = conn.execute(query)
        patients_data = result.fetchall()

    logger.info("Loaded %d patient records", len(patients_data))

    # -------------------------------------------------------------------------
    # Build combined source data (users + patients)
    # -------------------------------------------------------------------------
    ALL_PII_COLUMNS = [
        "patient_id",
        "users_uname",
        "users_UserType",
        "users_upwd",
        "users_umobileno",
        "users_upagerno",
        "users_ufname",
        "users_uminitial",
        "users_ulname",
        "users_uemail",
        "users_upaddress",
        "users_upcity",
        "users_upPhone",
        "users_dob",
        "users_ssn",
        "users_upaddress2",
        "users_initials",
        "users_ptDob",
        "users_upreviousname",
        "patients_controlno",
        "patients_employername",
        "patients_employeraddress",
        "patients_employeraddress2",
        "patients_employercity",
        "patients_employerPhone",
        "patients_insname",
        "patients_insgroupno",
        "patients_inssubscriberno",
        "patients_inscopay",
        "patients_insname2",
        "patients_insgroupno2",
        "patients_inssubscriberno2",
        "patients_inscopay2",
        "patients_straddress",
        "patients_city",
        "patients_insId",
        "patients_insId2",
        "patients_strAddress2",
        "patients_GrId",
        "patients_preferred_name",
    ]

    NULL_TEMPLATE = {col: None for col in ALL_PII_COLUMNS}
    NULL_TEMPLATE["patient_id"] = None

    combined_source_data: dict[int, dict] = {}

    user_cols = [
        "patient_id",
        "users_uname",
        "users_UserType",
        "users_upwd",
        "users_umobileno",
        "users_upagerno",
        "users_ufname",
        "users_uminitial",
        "users_ulname",
        "users_uemail",
        "users_upaddress",
        "users_upcity",
        "users_upPhone",
        "users_dob",
        "users_ssn",
        "users_upaddress2",
        "users_initials",
        "users_ptDob",
        "users_upreviousname",
    ]

    for row in users_data:
        patient_id = row[0]
        record = NULL_TEMPLATE.copy()
        user_row_dict = dict(zip(user_cols, row))
        record.update(user_row_dict)
        combined_source_data[patient_id] = record

    patient_cols = [
        "patient_id",
        "patients_controlno",
        "patients_employername",
        "patients_employeraddress",
        "patients_employeraddress2",
        "patients_employercity",
        "patients_employerPhone",
        "patients_insname",
        "patients_insgroupno",
        "patients_inssubscriberno",
        "patients_inscopay",
        "patients_insname2",
        "patients_insgroupno2",
        "patients_inssubscriberno2",
        "patients_inscopay2",
        "patients_straddress",
        "patients_city",
        "patients_insId",
        "patients_insId2",
        "patients_strAddress2",
        "patients_GrId",
        "patients_preferred_name",
    ]

    for row in patients_data:
        patient_id = row[0]
        patient_row_dict = dict(zip(patient_cols, row))

        if patient_id not in combined_source_data:
            record = NULL_TEMPLATE.copy()
            record.update(patient_row_dict)
            combined_source_data[patient_id] = record
        else:
            combined_source_data[patient_id].update(patient_row_dict)

    data_for_insert = []
    data_for_update = []

    for patient_id, record in combined_source_data.items():
        if patient_id in existing_patient_ids:
            data_for_update.append(record)
        else:
            data_for_insert.append(record)

    logger.info(
        "Combined records: %d, INSERT: %d, UPDATE: %d",
        len(combined_source_data),
        len(data_for_insert),
        len(data_for_update),
    )

    # -------------------------------------------------------------------------
    # Execute inserts and updates on pii_data_table
    # -------------------------------------------------------------------------
    if data_for_insert:
        cols = ", ".join(f"`{k}`" for k in data_for_insert[0].keys())
        params = ", ".join(f":{k}" for k in data_for_insert[0].keys())

        insert_query = text(
            f"""
        INSERT INTO pii_data_table ({cols})
        VALUES ({params})
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(insert_query, data_for_insert)

        logger.info("Inserted %d new PII records", len(data_for_insert))

    if data_for_update:
        set_clauses = [
            f"`{k}` = COALESCE(:{k}, `{k}`)"
            for k in data_for_update[0].keys()
            if k != "patient_id"
        ]
        set_clause_str = ", ".join(set_clauses)

        update_query = text(
            f"""
        UPDATE pii_data_table
        SET {set_clause_str}
        WHERE patient_id = :patient_id
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(update_query, data_for_update)

        logger.info("Updated %d existing PII records", len(data_for_update))

    logger.info("Master PII delta processing complete")


def main():
    parser = argparse.ArgumentParser(
        description="Apply PII master table deltas (pii_data_table)"
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

    run_master_pii_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
    )


if __name__ == "__main__":
    main()

