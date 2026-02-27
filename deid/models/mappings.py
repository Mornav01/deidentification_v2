"""Mapping database models (mappings.db) — replaces Django PatientMappingTable, etc."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from deid.models.base import MappingsBase


def _utcnow():
    return datetime.now(timezone.utc)


class PatientMapping(MappingsBase):
    __tablename__ = "patient_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_patient_id: Mapped[int] = mapped_column(Integer, unique=True)
    date_offset: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class EncounterMapping(MappingsBase):
    __tablename__ = "encounter_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    encounter_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_encounter_id: Mapped[int] = mapped_column(Integer, unique=True)
    patient_mapping_id: Mapped[int] = mapped_column(ForeignKey("patient_mappings.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class AppointmentMapping(MappingsBase):
    __tablename__ = "appointment_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    appointment_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    nd_appointment_id: Mapped[int] = mapped_column(Integer, unique=True)
    patient_mapping_id: Mapped[int] = mapped_column(ForeignKey("patient_mappings.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


class PhiStaging(MappingsBase):
    __tablename__ = "phi_staging"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    patient_id: Mapped[str] = mapped_column(String, index=True)
    phi_details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)


def get_or_create_patient_mapping(
    session: Session, patient_id: str, id_prefix: int
) -> int:
    """Return nd_patient_id for a patient, creating mapping if it doesn't exist."""
    existing = session.query(PatientMapping).filter_by(patient_id=patient_id).first()
    if existing:
        return existing.nd_patient_id
    count = session.query(PatientMapping).count()
    new_nd_id = id_prefix + count + 1
    mapping = PatientMapping(patient_id=patient_id, nd_patient_id=new_nd_id)
    session.add(mapping)
    session.commit()
    return new_nd_id


def get_or_create_encounter_mapping(
    session: Session, encounter_id: str, patient_mapping_id: int
) -> int:
    """Return nd_encounter_id for an encounter, creating mapping if it doesn't exist."""
    existing = session.query(EncounterMapping).filter_by(encounter_id=encounter_id).first()
    if existing:
        return existing.nd_encounter_id
    count = session.query(EncounterMapping).count()
    new_nd_id = count + 1
    mapping = EncounterMapping(
        encounter_id=encounter_id,
        nd_encounter_id=new_nd_id,
        patient_mapping_id=patient_mapping_id,
    )
    session.add(mapping)
    session.commit()
    return new_nd_id
