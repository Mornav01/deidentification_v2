#!/usr/bin/env python
"""
Master PII Delta Script  (new master_pg structure)

This script updates the PII master table (pii_data_table) in the given master schema
using deltas from the staging schema.

It is a .py version of NOTEBOOK/DENT/master_pii_delta.ipynb.

New master_pg structure: pii_data_table is keyed on nd_patient_id (the raw
patient_id column has been dropped — see mapping_migration_to_new_structure_dent.sql),
matching what the de-id reader expects (pii_table.c.nd_patient_id). This script
therefore resolves each source patient_id → nd_patient_id via patient_mapping_table
in --mapping_schema before writing.

The write is a single INSERT ... ON DUPLICATE KEY UPDATE keyed on the
uq_nd_patient_id unique key, so we no longer pre-load every existing key to decide
insert-vs-update. NULL source values never overwrite an existing value (COALESCE).

Usage:
    python master_pii_delta.py --master_schema "master_pg" \
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
    (flagged `excluded = 1`) so their nd_patient_id is stable, but their PII must
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


def run_master_pii_delta(master_schema: str, staging_schema: str, mapping_schema: str):
    """
    Apply delta updates to pii_data_table in master_schema from staging_schema,
    keyed on nd_patient_id resolved via mapping_schema.
    """
    logger.info(
        "Starting master PII delta: master_schema=%s, staging_schema=%s, mapping_schema=%s",
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
    # Build combined source data (users + patients), keyed by source patient_id
    # -------------------------------------------------------------------------
    # nd_patient_id is the join key; the raw source id is kept as `patientid`
    # (retained in every mapping/master table). PII_COLUMNS excludes both keys.
    PII_COLUMNS = [
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

    NULL_TEMPLATE = {col: None for col in PII_COLUMNS}

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

    combined_source_data: dict[int, dict] = {}

    for row in users_data:
        patient_id = row[0]
        record = NULL_TEMPLATE.copy()
        user_row_dict = dict(zip(user_cols, row))
        user_row_dict.pop("patient_id", None)
        record.update(user_row_dict)
        combined_source_data[patient_id] = record

    for row in patients_data:
        patient_id = row[0]
        patient_row_dict = dict(zip(patient_cols, row))
        patient_row_dict.pop("patient_id", None)

        if patient_id not in combined_source_data:
            record = NULL_TEMPLATE.copy()
            record.update(patient_row_dict)
            combined_source_data[patient_id] = record
        else:
            combined_source_data[patient_id].update(patient_row_dict)

    # -------------------------------------------------------------------------
    # Resolve nd_patient_id and build the write payload
    # -------------------------------------------------------------------------
    data_for_upsert = []
    unmapped = 0
    for patient_id, record in combined_source_data.items():
        nd_patient_id = patient_map.get(patient_id)
        if nd_patient_id is None:
            unmapped += 1
            continue
        record = {"nd_patient_id": nd_patient_id, "patientid": patient_id, **record}
        data_for_upsert.append(record)

    logger.info(
        "Combined source patients: %d, resolved: %d, unmapped (skipped): %d",
        len(combined_source_data),
        len(data_for_upsert),
        unmapped,
    )
    if unmapped:
        logger.warning(
            "%d source patients had no nd_patient_id mapping and were skipped. "
            "Run mapping_delta before master_pii_delta so every patient is mapped.",
            unmapped,
        )

    # -------------------------------------------------------------------------
    # Upsert into pii_data_table (keyed on uq_nd_patient_id)
    # -------------------------------------------------------------------------
    if data_for_upsert:
        # nd_patient_id is the upsert key; patientid + PII columns are the values.
        value_cols = ["patientid"] + PII_COLUMNS
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
        INSERT INTO pii_data_table ({cols})
        VALUES ({params})
        ON DUPLICATE KEY UPDATE {updates}
        """
        )

        with dest_engine.begin() as conn:
            conn.execute(text("SET SESSION sql_mode = '';"))
            conn.execute(upsert_query, data_for_upsert)

        logger.info("Upserted %d PII records", len(data_for_upsert))
    else:
        logger.info("No PII records to upsert")

    logger.info("Master PII delta processing complete")


def main():
    parser = argparse.ArgumentParser(
        description="Apply PII master table deltas (pii_data_table)"
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

    run_master_pii_delta(
        master_schema=args.master_schema,
        staging_schema=args.staging_schema,
        mapping_schema=args.mapping_schema,
    )


if __name__ == "__main__":
    main()
