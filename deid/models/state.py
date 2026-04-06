"""State database models (state.db) — replaces Django DbDetailsModel, TableDetailsModel."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from deid.models.base import StateBase


def _utcnow():
    return datetime.now(timezone.utc)


class DbConfig(StateBase):
    __tablename__ = "db_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    source_conn_str: Mapped[str] = mapped_column(String)
    dest_conn_str: Mapped[str] = mapped_column(String)
    run_config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)

    table_states: Mapped[list["TableState"]] = relationship(back_populates="db_config")


class TableState(StateBase):
    __tablename__ = "table_states"
    __table_args__ = (UniqueConstraint("table_name", "db_config_id", "config_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    db_config_id: Mapped[int] = mapped_column(ForeignKey("db_configs.id"))
    table_name: Mapped[str] = mapped_column(String)
    config_key: Mapped[str] = mapped_column(String, default="default", index=True)
    status: Mapped[str] = mapped_column(String, default="pending")
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rules_config: Mapped[dict] = mapped_column(JSON, default=dict)
    failure_remarks: Mapped[str | None] = mapped_column(String, nullable=True)
    qc_status: Mapped[str | None] = mapped_column(String, nullable=True)
    qc_result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)

    db_config: Mapped["DbConfig"] = relationship(back_populates="table_states")


class RunLog(StateBase):
    __tablename__ = "run_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    config_hash: Mapped[str] = mapped_column(String)
    phases: Mapped[list] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stats: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class BatchState(StateBase):
    __tablename__ = "batch_states"
    __table_args__ = (
        UniqueConstraint("table_name", "start_id", "end_id", "config_key"),
        Index("ix_batchstate_table_config", "table_name", "config_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String)
    config_key: Mapped[str] = mapped_column(String, default="default")
    start_id: Mapped[int] = mapped_column(Integer)
    end_id: Mapped[int] = mapped_column(Integer)
    actual_end_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String, default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)
