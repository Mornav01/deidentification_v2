#!/usr/bin/env python
"""
Mapping Delta Script  (new mapping_pg structure)

This script applies patient and encounter mapping deltas into the mapping schema.
It is a .py version of NOTEBOOK/DENT/mapping_delta.ipynb.

It:
- Reads existing patient/encounter mappings from mapping_schema
- Reads source patients/encounters from staging_schema (true delta — staging holds
  only the rows changed in today's CDC window)
- Inserts/updates patient_mapping_table and encounter_mapping_table

Schema (new mapping_pg structure — see mapping_migration_to_new_structure_dent.sql):
  patient_mapping_table(nd_patient_id PK, patientid UNIQUE, offset,
                        registration_date, created_at, updated_at)
  encounter_mapping_table(id PK AI, nd_patient_id, encounter_id, nd_encounter_id UNIQUE,
                          encounter_date, nd_ActiveFlag, created_at, updated_at, patientid)

Notes vs the old `mapping` structure:
  - patient_id (INT) → patientid (BIGINT); nd_patient_id is a plain BIGINT (no
    AUTO_INCREMENT) assigned as MAX(nd_patient_id)+1.
  - encounters link to patients via the nd_patient_id bridge; patientid is kept
    denormalised on the encounter row.
  - reference_mapping / created_by / updated_by are gone.
  - nd_ActiveFlag is written 'Y' on insert.

Usage:
    python mapping_delta.py --mapping_schema "mapping_pg" --staging_schema "mobiledoc_staging"
"""

import argparse
import logging
import os
import random
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Column,
    DATETIME,
    CHAR,
    Integer,
    MetaData,
    Table,
    UniqueConstraint,
    Index,
    create_engine,
    update,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import bindparam


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
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True, connect_args={"init_command": "SET sql_mode=''"})


def run_mapping_delta(mapping_schema: str, staging_schema: str):
    """
    Run mapping delta logic:
    - Update/insert into patient_mapping_table
    - Update/insert into encounter_mapping_table
    """
    logger.info(
        "Starting mapping delta: mapping_schema=%s, staging_schema=%s",
        mapping_schema,
        staging_schema,
    )

    # Engines
    source_engine = create_mysql_engine(staging_schema)
    dest_engine = create_mysql_engine(mapping_schema)  # read + write mappings

    metadata = MetaData()

    # -------------------------------------------------------------------------
    # Ensure mapping tables exist in mapping_schema (new mapping_pg structure).
    # create_all only creates tables that are missing, so on an already-migrated
    # schema this is a no-op and the definitions below simply document the shape.
    # -------------------------------------------------------------------------
    patient_mapping_table = Table(
        "patient_mapping_table",
        metadata,
        Column("nd_patient_id", BigInteger, primary_key=True, autoincrement=False),
        Column("patientid", BigInteger, nullable=False),
        Column("offset", Integer, nullable=False),
        Column("registration_date", DATETIME, nullable=True),
        Column("created_at", DATETIME, nullable=False),
        Column("updated_at", DATETIME, nullable=False),
        UniqueConstraint("patientid", name="uq_patientid"),
        Index("idx_patientid", "patientid"),
    )

    encounter_mapping_table = Table(
        "encounter_mapping_table",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("nd_patient_id", BigInteger, nullable=False),
        Column("encounter_id", BigInteger, nullable=False),
        Column("nd_encounter_id", BigInteger, nullable=False),
        Column("encounter_date", DATETIME, nullable=True),
        Column("nd_ActiveFlag", CHAR(1), nullable=False, server_default="Y"),
        Column("created_at", DATETIME, nullable=False),
        Column("updated_at", DATETIME, nullable=False),
        Column("patientid", BigInteger, nullable=False),
        UniqueConstraint("nd_encounter_id", name="uq_nd_encounter_id"),
        Index("idx_nd_patient_id", "nd_patient_id"),
        Index("idx_source_id", "encounter_id"),
    )

    metadata.create_all(dest_engine)
    logger.info("Ensured patient_mapping_table and encounter_mapping_table exist")

    # -------------------------------------------------------------------------
    # Load existing mapping data from mapping_schema
    #   patient : patientid → [nd_patient_id, offset]
    #   encounter: patientid → {encounter_id: nd_encounter_id, "last_encid": max}
    # The encounter table carries patientid denormalised, so no join is needed.
    # -------------------------------------------------------------------------
    logger.info("Loading existing patient mappings from %s", mapping_schema)
    with dest_engine.connect() as conn:
        pat_rows = conn.execute(
            text("SELECT patientid, nd_patient_id, `offset` FROM patient_mapping_table")
        ).fetchall()
        enc_rows = conn.execute(
            text("SELECT patientid, encounter_id, nd_encounter_id FROM encounter_mapping_table")
        ).fetchall()

    old_pat_ids: dict[int, list] = {}
    old_pat_encids: dict[int, dict] = defaultdict(dict)
    last_patid = 0

    for patientid, nd_patient_id, offset in pat_rows:
        old_pat_ids[patientid] = [nd_patient_id, offset]
        if nd_patient_id > last_patid:
            last_patid = nd_patient_id

    for patientid, encounter_id, nd_encounter_id in enc_rows:
        if nd_encounter_id is None:
            continue
        enc_map = old_pat_encids[patientid]
        if "last_encid" not in enc_map or nd_encounter_id > enc_map["last_encid"]:
            enc_map["last_encid"] = nd_encounter_id
        enc_map[encounter_id] = nd_encounter_id

    logger.info(
        "Existing patient mappings: %d, encounter mappings: %d, last_patid=%s",
        len(old_pat_ids), len(old_pat_encids), last_patid,
    )

    # -------------------------------------------------------------------------
    # Load exclusion data from staging_schema
    # -------------------------------------------------------------------------
    with source_engine.connect() as conn:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        exrows = conn.execute(text(f"""select distinct patientid from {staging_schema}.enc where visittype in ('NP','NPC2','NP-DEMT','NPIV','RESU','NB RESU','RESURainka','RESU ANC','D & B RESU','Blood Draw') union
    select distinct patientid from {staging_schema}.structdemographics sd, {staging_schema}.structdatadetail sdd where sd.detailid = sdd.id and sd.detailid=7062 and value = 'Yes'""")).fetchall()
    patientids = tuple(r[0] for r in exrows)
    logger.info("Loaded %s exclusion patients", len(patientids))

    # Guard against empty tuple — `NOT IN ()` is invalid MySQL syntax
    if patientids:
        pat_excl = f"AND uid NOT IN {patientids}"
        enc_excl = f"AND patientID NOT IN {patientids}"
    else:
        pat_excl = enc_excl = ""

    # -------------------------------------------------------------------------
    # Load delta data from staging_schema
    #   users.uid       → patientid  (raw source patient identifier)
    #   enc.patientID   → patientid
    #   enc.encounterID → encounter_id
    # -------------------------------------------------------------------------
    logger.info("Loading delta patients from %s.users", staging_schema)
    with source_engine.connect() as conn:
        users_data = conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(
                f"""
        SELECT DISTINCT uid AS patientid, cdate AS registration_date
        FROM users
        WHERE UserType = 3 {pat_excl}
        """
            )
        ).fetchall()

    logger.info("Loaded %d user registration rows", len(users_data))

    logger.info("Loading delta encounters from %s.enc", staging_schema)
    with source_engine.connect() as conn:
        enc_data = conn.execute(
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            text(
                f"""
        SELECT DISTINCT
            patientID   AS patientid,
            date        AS encounter_date,
            encounterID AS dent_encounter_id
        FROM enc
        WHERE 1=1 {enc_excl}
        ORDER BY 1, 2, 3
        """
            )
        ).fetchall()

    logger.info("Loaded %d encounter rows", len(enc_data))

    # DB-authoritative MAX to handle deleted rows or out-of-band inserts the
    # in-memory loop may have missed.
    with dest_engine.connect() as conn:
        db_last_patid = conn.execute(
            text("SELECT COALESCE(MAX(nd_patient_id), 0) FROM patient_mapping_table")
        ).scalar() or 0
    last_patid = max(last_patid, int(db_last_patid))
    logger.info("DB-authoritative last_patid=%d", last_patid)

    # -------------------------------------------------------------------------
    # STEP 1: Process & upsert delta patients
    # -------------------------------------------------------------------------
    possible_offsets = list(range(-38, -29)) + list(range(30, 39))

    existing_patient_ids: dict[int, list] = {}

    if users_data:
        patients_to_insert = []
        patients_to_update = []

        for patientid, registration_date in users_data:
            # New patient: not in history and not yet processed in this run
            if patientid not in old_pat_ids and patientid not in existing_patient_ids:
                offset = random.choice(possible_offsets)
                last_patid += 1
                nd_patient_id = last_patid

                patients_to_insert.append(
                    {
                        "nd_patient_id": nd_patient_id,
                        "patientid": patientid,
                        "offset": offset,
                        "registration_date": registration_date,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

                # Update in-memory map for use in Step 2 (Encounters)
                existing_patient_ids[patientid] = [nd_patient_id, offset]
            else:
                patients_to_update.append(
                    {
                        "patientid": patientid,
                        "registration_date": registration_date,
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

        logger.info(
            "Patients to insert: %d, to update: %d",
            len(patients_to_insert),
            len(patients_to_update),
        )

        if patients_to_insert or patients_to_update:
            with dest_engine.connect() as conn:
                trans = conn.begin()
                try:
                    if patients_to_insert:
                        conn.execute(
                            patient_mapping_table.insert(), patients_to_insert
                        )

                    if patients_to_update:
                        stmt = (
                            update(patient_mapping_table)
                            .where(
                                patient_mapping_table.c.patientid == bindparam("b_pid")
                            )
                            .values(
                                {
                                    "registration_date": bindparam("b_reg_date"),
                                    "updated_at": bindparam("b_ts"),
                                }
                            )
                        )

                        bulk_params = [
                            {
                                "b_pid": d["patientid"],
                                "b_reg_date": d["registration_date"],
                                "b_ts": d["updated_at"],
                            }
                            for d in patients_to_update
                        ]

                        conn.execute(stmt, bulk_params)

                    trans.commit()
                    logger.info("Patient upsert completed successfully")
                except SQLAlchemyError as e:
                    trans.rollback()
                    logger.error("Error during patient upsert: %s", e)
                    raise
        else:
            logger.info("No patient records to insert or update")
    else:
        logger.info("No delta patient records found in staging")

    # -------------------------------------------------------------------------
    # STEP 2: Process & upsert delta encounters
    # -------------------------------------------------------------------------
    if enc_data:
        encounters_to_insert = []
        encounters_to_update = []

        # Build a set of ALL nd_encounter_ids already in use (from history) to prevent duplicates
        used_nd_encounter_ids = set()
        for enc_map in old_pat_encids.values():
            for k, v in enc_map.items():
                if k != "last_encid" and v:
                    used_nd_encounter_ids.add(int(v))

        # Runtime tracker: patientid → last nd_encounter_id issued this run
        patient_encounters: dict[int, int] = {}

        # 1. Prepare data for INSERT or UPDATE
        for patientid, encounter_date, dent_encounter_id in enc_data:
            # Unified lookup for the patient mapping (history or this run)
            patient_info = old_pat_ids.get(patientid) or existing_patient_ids.get(patientid)

            if patient_info:
                target_nd_patient_id = patient_info[0]
            else:
                # If we can't find the patient, skip immediately to avoid ghosting
                logger.warning(
                    "No patient mapping found for patientid=%s; skipping encounter_id=%s",
                    patientid,
                    dent_encounter_id,
                )
                continue

            # DELTA CHECK: encounter already exists in history → UPDATE its date
            hist = old_pat_encids.get(patientid)
            if hist and dent_encounter_id in hist:
                encounters_to_update.append(
                    {
                        "patientid": patientid,
                        "encounter_id": dent_encounter_id,
                        "encounter_date": encounter_date,
                        "updated_at": datetime.now(timezone.utc),
                    }
                )
                continue

            # 2. Generate a new nd_encounter_id
            last_encid_hist = hist["last_encid"] if hist else 0
            last_encid_run = patient_encounters.get(patientid, 0)
            max_last_encid = max(last_encid_hist, last_encid_run)

            if max_last_encid > 0:
                # History or current run already has IDs — increment from the max
                nd_encounter_id = max_last_encid + 1
            else:
                # First encounter for this patient — base formula
                nd_encounter_id = (target_nd_patient_id * 10000) + 1

            # Ensure nd_encounter_id is globally unique (avoids duplicate key when a
            # patient's band overflows into the next patient's, or IDs collide in batch)
            while nd_encounter_id in used_nd_encounter_ids:
                nd_encounter_id += 1
            used_nd_encounter_ids.add(nd_encounter_id)

            patient_encounters[patientid] = nd_encounter_id

            encounters_to_insert.append(
                {
                    "nd_patient_id": target_nd_patient_id,
                    "patientid": patientid,
                    "encounter_id": dent_encounter_id,
                    "nd_encounter_id": nd_encounter_id,
                    "encounter_date": encounter_date,
                    "nd_ActiveFlag": "Y",
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            )

        logger.info(
            "Encounters to insert: %d, to update: %d",
            len(encounters_to_insert),
            len(encounters_to_update),
        )

        if encounters_to_insert or encounters_to_update:
            with dest_engine.connect() as conn:
                trans = conn.begin()
                try:
                    if encounters_to_insert:
                        conn.execute(
                            encounter_mapping_table.insert(), encounters_to_insert
                        )
                        logger.info(
                            "Bulk inserted %d encounter records",
                            len(encounters_to_insert),
                        )

                    if encounters_to_update:
                        stmt = (
                            update(encounter_mapping_table)
                            .where(
                                (encounter_mapping_table.c.encounter_id == bindparam("b_enc_id"))
                                & (encounter_mapping_table.c.patientid == bindparam("b_pid"))
                            )
                            .values(
                                {
                                    "encounter_date": bindparam("b_enc_date"),
                                    "updated_at": bindparam("b_updated_at"),
                                }
                            )
                        )

                        bulk_update_data = [
                            {
                                "b_enc_id": row["encounter_id"],
                                "b_pid": row["patientid"],
                                "b_enc_date": row["encounter_date"],
                                "b_updated_at": row["updated_at"],
                            }
                            for row in encounters_to_update
                        ]

                        conn.execute(stmt, bulk_update_data)
                        logger.info(
                            "Bulk updated %d encounter records",
                            len(encounters_to_update),
                        )

                    trans.commit()
                    logger.info("Encounter upsert completed successfully")
                except SQLAlchemyError as e:
                    trans.rollback()
                    logger.error("Error during encounter upsert: %s", e)
                    raise
        else:
            logger.info("No encounter records to insert or update")
    else:
        logger.info("No delta encounter records found in staging")

    logger.info("Mapping delta processing complete")


def main():
    parser = argparse.ArgumentParser(
        description="Apply mapping deltas to patient_mapping_table and encounter_mapping_table"
    )
    parser.add_argument(
        "--mapping_schema",
        required=True,
        help="Mapping schema name (e.g. 'mapping_pg')",
    )
    parser.add_argument(
        "--staging_schema",
        required=True,
        help="Staging schema name (e.g. 'mobiledoc_staging')",
    )

    args = parser.parse_args()

    run_mapping_delta(
        mapping_schema=args.mapping_schema,
        staging_schema=args.staging_schema,
    )


if __name__ == "__main__":
    main()
