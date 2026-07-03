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


class QCDeltaIdentityResult(QCResultsBase):
    """One record per table for the delta-identity QC (cross-environment row-level diff).

    Compares a source (e.g. prod) against a dest (e.g. CDC-merged staging / local) on a shared
    row key, optionally scoped to a delta window. See deid/qc/delta_identity.py.
    """
    __tablename__ = "qc_delta_identity_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(255), index=True)
    is_qc_passed: Mapped[bool] = mapped_column(Boolean)
    id_col: Mapped[str] = mapped_column(String(255), default="")
    biz_key_col: Mapped[str] = mapped_column(String(255), default="")
    delta_after: Mapped[str] = mapped_column(String(64), default="")
    source_rows_count: Mapped[int] = mapped_column(Integer, default=0)
    dest_rows_count: Mapped[int] = mapped_column(Integer, default=0)
    matched_count: Mapped[int] = mapped_column(Integer, default=0)
    missing_in_dest: Mapped[int] = mapped_column(Integer, default=0)
    extra_in_dest: Mapped[int] = mapped_column(Integer, default=0)
    value_mismatch: Mapped[int] = mapped_column(Integer, default=0)
    sample_detail: Mapped[str] = mapped_column(Text, default="[]")  # JSON: capped sample diff rows
    reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class QCPart2Result(QCResultsBase):
    """One record per Part-2 (mapping & count) check. See deid/qc/mapping_count.py.

    Part 2 runs before the pipeline; a blocking failure halts the run (QC Framework Part 2).
    """
    __tablename__ = "qc_part2_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    check_name: Mapped[str] = mapped_column(String(128), index=True)
    entity: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(16))  # pass | fail | skipped | error
    blocking: Mapped[bool] = mapped_column(Boolean, default=True)
    expected: Mapped[str] = mapped_column(String(64), default="")
    actual: Mapped[str] = mapped_column(String(64), default="")
    delta: Mapped[str] = mapped_column(String(64), default="")
    details: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class QCUnstructuredAuditResult(QCResultsBase):
    """One record per table for the Part-3 unstructured PHI audit. See deid/qc/master_phi.py.

    Failed records (``failure_detail``) are the quarantine list — they must be remediated before
    the de-identified dataset is approved for release (QC Framework Part 3 failure action).
    """
    __tablename__ = "qc_unstructured_audit_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(255), index=True)
    total_records_audited: Mapped[int] = mapped_column(Integer, default=0)
    phi_entities_checked: Mapped[int] = mapped_column(Integer, default=0)
    pass_count: Mapped[int] = mapped_column(Integer, default=0)
    fail_count: Mapped[int] = mapped_column(Integer, default=0)
    coverage_gaps: Mapped[int] = mapped_column(Integer, default=0)
    failure_detail: Mapped[str] = mapped_column(Text, default="[]")  # JSON: quarantine list
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
