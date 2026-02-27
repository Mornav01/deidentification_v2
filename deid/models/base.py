"""SQLAlchemy engine factories and base declarative classes."""
from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase


class StateBase(DeclarativeBase):
    """Base class for state.db models."""
    pass


class MappingsBase(DeclarativeBase):
    """Base class for mappings.db models."""
    pass


def _enable_wal(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


def create_state_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


def create_mappings_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


def create_all_state_tables(engine):
    import deid.models.state  # noqa: F401 — ensure models are registered
    StateBase.metadata.create_all(engine)


def create_all_mappings_tables(engine):
    import deid.models.mappings  # noqa: F401 — ensure models are registered
    MappingsBase.metadata.create_all(engine)
