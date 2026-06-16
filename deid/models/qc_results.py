"""QC results database model (qc_results.db)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow():
    return datetime.now(timezone.utc)


class QCResultsBase(DeclarativeBase):
    """Separate base so qc_results.db stays independent of state.db / mappings.db."""
    pass


class QCTableResult(QCResultsBase):
    """One record per table QC run — written as soon as the table's scan completes."""
    __tablename__ = "qc_table_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(255), index=True)
    is_qc_passed: Mapped[bool] = mapped_column(Boolean)
    reason: Mapped[str] = mapped_column(Text, default="")
    source_rows_count: Mapped[int] = mapped_column(Integer)
    dest_rows_count: Mapped[int] = mapped_column(Integer)
    sample_size: Mapped[int] = mapped_column(Integer, default=0)
    columns_result: Mapped[str] = mapped_column(Text, default="{}")  # JSON
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
