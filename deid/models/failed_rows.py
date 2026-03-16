"""Failed-rows audit database model (failed_rows.db)."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow():
    return datetime.now(timezone.utc)


class FailedRowsBase(DeclarativeBase):
    """Separate base so failed_rows.db stays independent of state.db / mappings.db."""
    pass


class FailedRow(FailedRowsBase):
    """One record per source row that could not be de-identified.

    Only written for absolute faults — rows that are structurally
    unprocessable (e.g. no resolved patient ID), not for soft warnings.
    """
    __tablename__ = "failed_rows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_db: Mapped[str] = mapped_column(String, index=True)
    table_name: Mapped[str] = mapped_column(String, index=True)
    reason: Mapped[str] = mapped_column(String)   # e.g. "no_resolved_patient_id"
    row_data: Mapped[str] = mapped_column(Text)   # JSON-serialised source row
    failed_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
