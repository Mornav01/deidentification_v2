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
                        registration_date, excluded, excluded_at, created_at, updated_at)
  encounter_mapping_table(id PK AI, nd_patient_id, encounter_id, nd_encounter_id UNIQUE,
                          encounter_date, nd_ActiveFlag, created_at, updated_at, patientid)

Exclusion handling:
  Patients matching the exclusion criteria are NOT dropped from the mapping tables.
  They are mapped like everyone else and flagged `excluded = 1` with `excluded_at`
  recording when the flag was first raised. Consumers (master_pii_delta,
  master_insurance_delta, the de-id mapping preload/joins) filter on `excluded = 0`,
  so excluded patients still never reach the de-identified output.

  Keeping the row means a patient who reappears in a later delta window without
  exclusion criteria reuses their original nd_patient_id instead of being minted a
  new one, and it gives an auditable record of who was excluded and from when.

  The flag is STICKY: once raised it is never cleared by this script. The exclusion
  criteria are evaluated against the staging *delta*, which holds only rows changed
  in today's CDC window — a patient's qualifying visit surfaces in exactly one run,
  so "absent from today's exclusion set" does not mean "no longer qualifies".
  Clearing the flag requires re-evaluating against the full source database.

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
    Boolean,
    Column,
    DATETIME,
    CHAR,
    Integer,
    MetaData,
    Table,
    UniqueConstraint,
    Index,
    create_engine,
    func,
    inspect,
    update,
    text,
)
from sqlalchemy.engine import URL
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
    """Create SQLAlchemy engine for a given schema.

    Built via URL.create (same as deid/config/schema.py DbConfig.connection_string) so
    special characters in DB_PASS (e.g. @) are percent-encoded correctly.
    """
    url = URL.create(
        drivername="mysql+pymysql", username=MYSQL_USER, password=MYSQL_PASS,
        host=MYSQL_HOST, port=MYSQL_PORT, database=schema,
    ).render_as_string(hide_password=False)
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True, connect_args={"init_command": "SET sql_mode=''"})


def ensure_exclusion_columns(engine) -> None:
    """Add `excluded` / `excluded_at` to an existing patient_mapping_table.

    `metadata.create_all` only creates missing *tables*, so on a schema that was
    built before the exclusion flag existed the columns have to be added here.
    Idempotent — a no-op once the columns are present.
    """
    existing = {c["name"] for c in inspect(engine).get_columns("patient_mapping_table")}

    ddl = []
    if "excluded" not in existing:
        ddl.append(
            "ALTER TABLE patient_mapping_table "
            "ADD COLUMN excluded TINYINT(1) NOT NULL DEFAULT 0"
        )
    if "excluded_at" not in existing:
        ddl.append(
            "ALTER TABLE patient_mapping_table ADD COLUMN excluded_at DATETIME NULL"
        )

    if not ddl:
        return

    with engine.begin() as conn:
        for stmt in ddl:
            logger.info("Applying: %s", stmt)
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            conn.execute(text(stmt))
        indexes = {i["name"] for i in inspect(engine).get_indexes("patient_mapping_table")}
        if "idx_excluded" not in indexes:
            conn.execute(text("CREATE INDEX idx_excluded ON patient_mapping_table (excluded)"))
    logger.info("patient_mapping_table exclusion columns are present")


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
        Column("excluded", Boolean, nullable=False, server_default=text("0")),
        Column("excluded_at", DATETIME, nullable=True),
        Column("created_at", DATETIME, nullable=False),
        Column("updated_at", DATETIME, nullable=False),
        UniqueConstraint("patientid", name="uq_patientid"),
        Index("idx_patientid", "patientid"),
        Index("idx_excluded", "excluded"),
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
    ensure_exclusion_columns(dest_engine)
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
            text(
                "SELECT patientid, nd_patient_id, `offset`, excluded "
                "FROM patient_mapping_table"
            )
        ).fetchall()
        enc_rows = conn.execute(
            text("SELECT patientid, encounter_id, nd_encounter_id FROM encounter_mapping_table")
        ).fetchall()

    old_pat_ids: dict[int, list] = {}
    old_pat_encids: dict[int, dict] = defaultdict(dict)
    already_excluded: set[int] = set()
    last_patid = 0

    for patientid, nd_patient_id, offset, excluded in pat_rows:
        old_pat_ids[patientid] = [nd_patient_id, offset]
        if excluded:
            already_excluded.add(patientid)
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
        "Existing patient mappings: %d (of which %d already excluded), "
        "encounter mappings: %d, last_patid=%s",
        len(old_pat_ids), len(already_excluded), len(old_pat_encids), last_patid,
    )

    # -------------------------------------------------------------------------
    # Load exclusion data from staging_schema
    # -------------------------------------------------------------------------
    with source_engine.connect() as conn:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        exrows = conn.execute(text(f"""select distinct patientid from {staging_schema}.enc where visittype in ('NP','NPC2','NP-DEMT','NPIV','RESU','NB RESU','RESURainka','RESU ANC','D & B RESU','Blood Draw') union
    select distinct patientid from {staging_schema}.structdemographics sd, {staging_schema}.structdatadetail sdd where sd.detailid = sdd.id and sd.detailid=7062 and value = 'Yes'""")).fetchall()
    excluded_ids: set[int] = {r[0] for r in exrows if r[0] is not None}
    logger.info("Loaded %s exclusion patients from this delta window", len(excluded_ids))

    # -------------------------------------------------------------------------
    # Load delta data from staging_schema
    #   users.uid       → patientid  (raw source patient identifier)
    #   enc.patientID   → patientid
    #   enc.encounterID → encounter_id
    # -------------------------------------------------------------------------
    logger.info("Loading delta patients from %s.users", staging_schema)
    with source_engine.connect() as conn:
        users_data = conn.execute(
            text(
                """
        SELECT DISTINCT uid AS patientid, cdate AS registration_date
        FROM users
        WHERE UserType = 3
        """
            )
        ).fetchall()

    logger.info("Loaded %d user registration rows", len(users_data))

    logger.info("Loading delta encounters from %s.enc", staging_schema)
    with source_engine.connect() as conn:
        enc_data = conn.execute(
            text(
                """
        SELECT DISTINCT
            patientID   AS patientid,
            date        AS encounter_date,
            encounterID AS dent_encounter_id
        FROM enc
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

    # Patients touched by this delta window: every user row, plus any patient the
    # exclusion query flagged. The latter can qualify purely through enc /
    # structdemographics without their `users` row changing, so they would never be
    # visited by the loop below — and their exclusion would go unrecorded.
    # registration_date is None for those; the update below COALESCEs so a None
    # never wipes a stored date.
    delta_patients: list[tuple] = list(users_data)
    users_delta_ids = {row[0] for row in users_data}
    exclusion_only_ids = excluded_ids - users_delta_ids
    if exclusion_only_ids:
        logger.info(
            "%d excluded patients are not in the users delta — flagging them anyway",
            len(exclusion_only_ids),
        )
        delta_patients.extend((pid, None) for pid in sorted(exclusion_only_ids))

    if delta_patients:
        patients_to_insert = []
        patients_to_update = []
        newly_excluded: set[int] = set()

        for patientid, registration_date in delta_patients:
            is_excluded = patientid in excluded_ids

            # New patient: not in history and not yet processed in this run
            if patientid not in old_pat_ids and patientid not in existing_patient_ids:
                offset = random.choice(possible_offsets)
                last_patid += 1
                nd_patient_id = last_patid

                if is_excluded:
                    newly_excluded.add(patientid)
                    # A patient can appear twice in the delta (DISTINCT uid, cdate);
                    # this keeps the second pass from re-stamping excluded_at.
                    already_excluded.add(patientid)

                patients_to_insert.append(
                    {
                        "nd_patient_id": nd_patient_id,
                        "patientid": patientid,
                        "offset": offset,
                        "registration_date": registration_date,
                        "excluded": is_excluded,
                        "excluded_at": datetime.now(timezone.utc) if is_excluded else None,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

                # Update in-memory map for use in Step 2 (Encounters)
                existing_patient_ids[patientid] = [nd_patient_id, offset]
            else:
                # Sticky: only ever raise the flag. `excluded_ids` is computed from
                # the staging delta, so a patient dropping out of it says nothing
                # about whether they still qualify in the full source.
                flag_now = is_excluded and patientid not in already_excluded
                if flag_now:
                    newly_excluded.add(patientid)
                    already_excluded.add(patientid)

                patients_to_update.append(
                    {
                        "patientid": patientid,
                        "registration_date": registration_date,
                        "updated_at": datetime.now(timezone.utc),
                        "flag_excluded": flag_now,
                    }
                )

        logger.info(
            "Patients to insert: %d, to update: %d, newly excluded: %d",
            len(patients_to_insert),
            len(patients_to_update),
            len(newly_excluded),
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
                        # COALESCE so the exclusion-only rows (registration_date
                        # None) leave the stored registration date intact.
                        base_values = {
                            "registration_date": func.coalesce(
                                bindparam("b_reg_date", type_=DATETIME),
                                patient_mapping_table.c.registration_date,
                            ),
                            "updated_at": bindparam("b_ts"),
                        }
                        where_pid = patient_mapping_table.c.patientid == bindparam("b_pid")

                        plain_stmt = (
                            update(patient_mapping_table).where(where_pid).values(base_values)
                        )
                        # excluded_at is stamped only on the False → True transition,
                        # so it keeps meaning "excluded since".
                        exclude_stmt = (
                            update(patient_mapping_table)
                            .where(where_pid)
                            .values(
                                {
                                    **base_values,
                                    "excluded": True,
                                    "excluded_at": bindparam("b_ts"),
                                }
                            )
                        )

                        for stmt, rows in (
                            (plain_stmt, [d for d in patients_to_update if not d["flag_excluded"]]),
                            (exclude_stmt, [d for d in patients_to_update if d["flag_excluded"]]),
                        ):
                            if not rows:
                                continue
                            conn.execute(
                                stmt,
                                [
                                    {
                                        "b_pid": d["patientid"],
                                        "b_reg_date": d["registration_date"],
                                        "b_ts": d["updated_at"],
                                    }
                                    for d in rows
                                ],
                            )

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
    #
    # Encounters belonging to excluded patients are mapped like any other. Staging
    # only ever holds the current CDC window, so an encounter skipped here would
    # never be offered again — leaving the patient's history permanently unmapped
    # if the exclusion is ever lifted. Gating happens at the patient level via
    # `excluded`, which every downstream consumer filters on.
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
