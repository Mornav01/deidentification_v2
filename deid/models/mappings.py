"""Mapping database models (mappings.db) — replaces Django PatientMappingTable, etc."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError
from sqlalchemy import JSON, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, Session, mapped_column

from deid.models.base import MappingsBase
from pydantic import validate_call


@validate_call(config=dict(arbitrary_types_allowed=True))
def _utcnow():
    return datetime.now(timezone.utc)


class PatientMapping(MappingsBase):
    __tablename__ = "patient_mapping_table"

    nd_patient_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    offset: Mapped[int] = mapped_column(Integer, default=0)
    reference_mapping: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class EncounterMapping(MappingsBase):
    __tablename__ = "encounter_mapping_table"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, index=True)
    encounter_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_encounter_id: Mapped[int] = mapped_column(Integer, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class AppointmentMapping(MappingsBase):
    __tablename__ = "appointment_mapping_table"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, index=True)
    appointment_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_appointment_id: Mapped[int] = mapped_column(Integer, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class PhiStaging(MappingsBase):
    __tablename__ = "phi_staging"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, index=True)
    phi_details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


@validate_call(config=dict(arbitrary_types_allowed=True))
def get_or_create_patient_mapping(
    session: Session, patient_id: str, id_prefix: int
) -> int:
    """Return nd_patient_id for a patient, creating mapping if it doesn't exist."""
    existing = session.query(PatientMapping).filter_by(patient_id=patient_id).first()
    if existing:
        return existing.nd_patient_id

    max_id = session.query(func.max(PatientMapping.nd_patient_id)).scalar()
    new_nd_id = (max_id or id_prefix) + 1

    try:
        mapping = PatientMapping(patient_id=patient_id, nd_patient_id=new_nd_id)
        session.add(mapping)
        session.commit()
    except IntegrityError:
        session.rollback()
        existing = session.query(PatientMapping).filter_by(patient_id=patient_id).first()
        if existing:
            return existing.nd_patient_id
        raise
    return new_nd_id


@validate_call(config=dict(arbitrary_types_allowed=True))
def get_or_create_encounter_mapping(
    session: Session, encounter_id: str, patient_id: str
) -> int:
    """Return nd_encounter_id for an encounter, creating mapping if it doesn't exist."""
    existing = session.query(EncounterMapping).filter_by(encounter_id=encounter_id).first()
    if existing:
        return existing.nd_encounter_id

    max_id = session.query(func.max(EncounterMapping.nd_encounter_id)).scalar()
    new_nd_id = (max_id or 0) + 1

    try:
        mapping = EncounterMapping(
            encounter_id=encounter_id,
            nd_encounter_id=new_nd_id,
            patient_id=patient_id,
        )
        session.add(mapping)
        session.commit()
    except IntegrityError:
        session.rollback()
        existing = session.query(EncounterMapping).filter_by(encounter_id=encounter_id).first()
        if existing:
            return existing.nd_encounter_id
        raise
    return new_nd_id
