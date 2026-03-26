"""Failed-rows audit database model (failed_rows.db).

Each source schema gets its own table: ``failed_rows_{schema_name}``.
This allows selective cleanup on --rerun without destroying audit data
from other schemas.
"""
from __future__ import annotations

try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    import re  # type: ignore[no-redef]
from datetime import datetime, timezone
from functools import lru_cache

from sqlalchemy import DateTime, Integer, String, Text, Table, Column, MetaData
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow():
    return datetime.now(timezone.utc)


class FailedRowsBase(DeclarativeBase):
    """Separate base so failed_rows.db stays independent of state.db / mappings.db."""
    pass


# Legacy table — kept so existing failed_rows.db files remain readable.
class FailedRow(FailedRowsBase):
    """One record per source row that could not be de-identified."""
    __tablename__ = "failed_rows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_db: Mapped[str] = mapped_column(String, index=True)
    table_name: Mapped[str] = mapped_column(String, index=True)
    reason: Mapped[str] = mapped_column(String)
    row_data: Mapped[str] = mapped_column(Text)
    failed_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


def _sanitize_table_name(name: str) -> str:
    """Ensure the name is safe for use as a SQLite table name."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower() or "unknown"


_metadata = MetaData()


@lru_cache(maxsize=128)
def _get_schema_table(schema_name: str) -> Table:
    """Return (and cache) a SQLAlchemy Table for a specific source schema."""
    safe_name = f"failed_rows_{_sanitize_table_name(schema_name)}"
    return Table(
        safe_name,
        _metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("source_db", String, index=True),
        Column("table_name", String, index=True),
        Column("reason", String),
        Column("row_data", Text),
        Column("failed_at", DateTime, default=_utcnow),
        extend_existing=True,
    )


def ensure_schema_table(engine, schema_name: str) -> Table:
    """Create the per-schema failed_rows table if it doesn't exist and return it."""
    table = _get_schema_table(schema_name)
    table.create(engine, checkfirst=True)
    return table


def get_schema_table_name(schema_name: str) -> str:
    """Return the SQLite table name for a source schema (for DROP/cleanup)."""
    return f"failed_rows_{_sanitize_table_name(schema_name)}"
