"""SQLAlchemy engine factories and base declarative classes."""
from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase
from pydantic import validate_call


class StateBase(DeclarativeBase):
    """Base class for state.db models."""
    pass


class MappingsBase(DeclarativeBase):
    """Base class for mappings.db models."""
    pass


@validate_call(config=dict(arbitrary_types_allowed=True))
def _enable_wal(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_state_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_mappings_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_all_state_tables(engine):
    import deid.models.state  # noqa: F401 — ensure models are registered
    StateBase.metadata.create_all(engine)


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_all_mappings_tables(engine):
    import deid.models.mappings  # noqa: F401 — ensure models are registered
    MappingsBase.metadata.create_all(engine)
