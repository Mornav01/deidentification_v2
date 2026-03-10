"""Scan table rule configs for ID columns and bulk-insert patient mappings."""
from __future__ import annotations

import logging
import random
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from deid.config.schema import TableConfig
from deid.models.mappings import AppointmentMapping, EncounterMapping, PatientMapping

logger = logging.getLogger("deid.mapping_populator")

_WRITE_BATCH = 5000


def scan_rules_for_id_columns(tables: list[TableConfig]) -> dict[str, Any]:
    """Scan table rules and return a dict grouping columns by their ID rule type.

    Returns a dict with keys:
        patient_id_columns      — {table_name: [col_name, ...]}
        encounter_id_tables     — {table_name: (encounter_col, patient_col)}
        appointment_id_tables   — {table_name: (appt_col, patient_col)}
        reference_pid_columns   — {table_name: [col_name, ...]}
    """
    patient_id_columns: dict[str, list[str]] = {}
    encounter_id_tables: dict[str, tuple[str, str]] = {}
    appointment_id_tables: dict[str, tuple[str, str]] = {}
    reference_pid_columns: dict[str, list[str]] = {}

    for table in tables:
        pid_cols: list[str] = []
        enc_col: str | None = None
        appt_col: str | None = None
        ref_cols: list[str] = []
        first_pid_col: str | None = None

        for col_name, rule in table.rules.items():
            rule_upper = rule.strip().upper()
            if rule_upper == "PATIENT_ID":
                pid_cols.append(col_name)
                if first_pid_col is None:
                    first_pid_col = col_name
            elif rule_upper == "ENCOUNTER_ID":
                enc_col = col_name
            elif rule_upper == "APPOINTMENT_ID":
                appt_col = col_name
            elif rule_upper == "REFERENCE_PID":
                ref_cols.append(col_name)

        if pid_cols:
            patient_id_columns[table.name] = pid_cols

        if enc_col is not None and first_pid_col is not None:
            encounter_id_tables[table.name] = (enc_col, first_pid_col)

        if appt_col is not None and first_pid_col is not None:
            appointment_id_tables[table.name] = (appt_col, first_pid_col)

        if ref_cols:
            reference_pid_columns[table.name] = ref_cols

    logger.debug(
        "scan_rules_for_id_columns: patient_id=%d tables, encounter=%d, appointment=%d, reference_pid=%d",
        len(patient_id_columns),
        len(encounter_id_tables),
        len(appointment_id_tables),
        len(reference_pid_columns),
    )

    return {
        "patient_id_columns": patient_id_columns,
        "encounter_id_tables": encounter_id_tables,
        "appointment_id_tables": appointment_id_tables,
        "reference_pid_columns": reference_pid_columns,
    }


def _bulk_insert_mappings(
    mappings_engine,
    source_ids: list[str],
    model_class,
    source_id_attr: str,
    auto_id_attr: str,
    object_factory,
    default_start: int,
    label: str,
) -> int:
    """Generic bulk-insert helper — skips IDs that already exist.

    Args:
        source_ids: Deduplicated list of source ID strings to ensure are mapped.
        model_class: SQLAlchemy ORM model (PatientMapping, EncounterMapping, etc.).
        source_id_attr: Model attribute name for the source ID column.
        auto_id_attr: Model attribute name for the auto-incrementing ND ID column.
        object_factory: ``(source_id, next_id) -> model_instance``.
        default_start: Starting value for auto ID when the table is empty.
        label: Logging label (e.g. "bulk_insert_patient_mappings").

    Returns:
        Number of NEW mappings created.
    """
    if not source_ids:
        return 0

    total_new = 0
    source_id_col = getattr(model_class, source_id_attr)
    auto_id_col = getattr(model_class, auto_id_attr)

    with Session(mappings_engine) as session:
        existing_ids: set[str] = set()
        for i in range(0, len(source_ids), _WRITE_BATCH):
            batch = source_ids[i : i + _WRITE_BATCH]
            rows = session.query(source_id_col).filter(source_id_col.in_(batch)).all()
            existing_ids.update(r[0] for r in rows)

        new_ids = [sid for sid in source_ids if sid not in existing_ids]
        if not new_ids:
            logger.info("%s: all %d IDs already exist", label, len(source_ids))
            return 0

        max_existing = session.query(func.max(auto_id_col)).scalar()
        next_id = (max_existing if max_existing is not None else default_start) + 1

        for i in range(0, len(new_ids), _WRITE_BATCH):
            batch = new_ids[i : i + _WRITE_BATCH]
            objects = [object_factory(sid, next_id + j) for j, sid in enumerate(batch)]
            next_id += len(batch)
            session.bulk_save_objects(objects)
            session.commit()
            total_new += len(objects)
            logger.debug("%s: committed batch of %d", label, len(objects))

    logger.info("%s: created %d new mappings (%d already existed)", label, total_new, len(existing_ids))
    return total_new


def bulk_insert_patient_mappings(
    mappings_engine,
    patient_ids: list[str],
    id_prefix: int,
    max_offset: int,
    random_seed: int = 42,
) -> int:
    """Bulk-insert new patient mappings, skipping IDs that already exist."""
    rng = random.Random(random_seed)

    def factory(pid, next_id):
        return PatientMapping(
            nd_patient_id=next_id,
            patient_id=pid,
            offset=rng.randint(1, max_offset),
        )

    return _bulk_insert_mappings(
        mappings_engine, patient_ids, PatientMapping,
        "patient_id", "nd_patient_id", factory, id_prefix,
        "bulk_insert_patient_mappings",
    )


def bulk_insert_encounter_mappings(
    mappings_engine,
    pairs: list[tuple[str, str]],
) -> int:
    """Bulk-insert new encounter mappings, skipping encounter_ids that already exist."""
    if not pairs:
        return 0
    pair_lookup = dict(pairs)  # deduplicates by encounter_id
    source_ids = list(pair_lookup)

    def factory(eid, next_id):
        return EncounterMapping(
            encounter_id=eid,
            patient_id=pair_lookup[eid],
            nd_encounter_id=next_id,
        )

    return _bulk_insert_mappings(
        mappings_engine, source_ids, EncounterMapping,
        "encounter_id", "nd_encounter_id", factory, 0,
        "bulk_insert_encounter_mappings",
    )


def bulk_insert_appointment_mappings(
    mappings_engine,
    pairs: list[tuple[str, str]],
) -> int:
    """Bulk-insert new appointment mappings, skipping appointment_ids that already exist."""
    if not pairs:
        return 0
    pair_lookup = dict(pairs)  # deduplicates by appointment_id
    source_ids = list(pair_lookup)

    def factory(aid, next_id):
        return AppointmentMapping(
            appointment_id=aid,
            patient_id=pair_lookup[aid],
            nd_appointment_id=next_id,
        )

    return _bulk_insert_mappings(
        mappings_engine, source_ids, AppointmentMapping,
        "appointment_id", "nd_appointment_id", factory, 0,
        "bulk_insert_appointment_mappings",
    )


def populate_mappings(
    source,
    tables: list[TableConfig],
    mappings_engine,
    patient_id_prefix: int = 10000000,
    max_offset: int = 34,
    random_seed: int = 42,
) -> dict:
    """Top-level orchestration: scan rules, fetch IDs from source, insert mappings.

    Args:
        source: NDDBHandler (or mock) with ``fetch_distinct_values`` and
            ``fetch_distinct_pairs`` methods.
        tables: List of ``TableConfig`` objects describing every table to process.
        mappings_engine: SQLAlchemy engine for the mappings database.
        patient_id_prefix: Base value for ``nd_patient_id`` when the table is empty.
        max_offset: Upper bound (inclusive) for random date-offset per patient.
        random_seed: Seed for reproducible offset generation (default 42).

    Returns:
        Summary dict with counts: patients_found, patients_created,
        encounters_found, encounters_created, appointments_found,
        appointments_created.
    """
    id_info = scan_rules_for_id_columns(tables)

    # ── 1. Patient IDs (from PATIENT_ID + REFERENCE_PID columns) ────────
    all_patient_ids: set[str] = set()
    for table_name, columns in id_info["patient_id_columns"].items():
        for col in columns:
            try:
                for pid in source.fetch_distinct_values(table_name, col):
                    all_patient_ids.add(pid)
            except Exception:
                logger.exception("Failed to scan PATIENT_ID column %s.%s — skipping", table_name, col)

    for table_name, columns in id_info["reference_pid_columns"].items():
        for col in columns:
            try:
                for pid in source.fetch_distinct_values(table_name, col):
                    all_patient_ids.add(pid)
            except Exception:
                logger.exception("Failed to scan REFERENCE_PID column %s.%s — skipping", table_name, col)

    patients_created = bulk_insert_patient_mappings(
        mappings_engine,
        sorted(all_patient_ids),
        patient_id_prefix,
        max_offset,
        random_seed,
    )

    # ── 2. Encounters ─────────────────────────────────────────────────────
    encounter_pairs: dict[str, str] = {}
    for table_name, (enc_col, pid_col) in id_info["encounter_id_tables"].items():
        try:
            for enc_id, pat_id in source.fetch_distinct_pairs(table_name, enc_col, pid_col):
                if enc_id not in encounter_pairs:
                    encounter_pairs[enc_id] = pat_id
        except Exception:
            logger.exception("Failed to scan encounter columns %s.(%s, %s) — skipping", table_name, enc_col, pid_col)

    encounters_created = bulk_insert_encounter_mappings(
        mappings_engine,
        list(encounter_pairs.items()),
    )

    # ── 3. Appointments ───────────────────────────────────────────────────
    appointment_pairs: dict[str, str] = {}
    for table_name, (appt_col, pid_col) in id_info["appointment_id_tables"].items():
        try:
            for appt_id, pat_id in source.fetch_distinct_pairs(table_name, appt_col, pid_col):
                if appt_id not in appointment_pairs:
                    appointment_pairs[appt_id] = pat_id
        except Exception:
            logger.exception("Failed to scan appointment columns %s.(%s, %s) — skipping", table_name, appt_col, pid_col)

    appointments_created = bulk_insert_appointment_mappings(
        mappings_engine,
        list(appointment_pairs.items()),
    )

    summary = {
        "patients_found": len(all_patient_ids),
        "patients_created": patients_created,
        "encounters_found": len(encounter_pairs),
        "encounters_created": encounters_created,
        "appointments_found": len(appointment_pairs),
        "appointments_created": appointments_created,
    }

    logger.info("populate_mappings summary: %s", summary)
    return summary
