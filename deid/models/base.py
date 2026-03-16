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


_WRITE_PREFIXES = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "CREATE", "TRUNCATE", "REPLACE", "MERGE", "UPSERT",
)


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_read_only_mappings_engine(db_path: str):
    """Create a read-only SQLite engine for mappings.db.

    Two layers of protection:
    1. ``PRAGMA query_only=ON`` — SQLite refuses all writes at the engine level.
    2. ``before_cursor_execute`` guard that blocks any DML/DDL statement.
    """
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    @event.listens_for(engine, "connect")
    def _set_read_only(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA query_only=ON")
        cursor.close()

    @event.listens_for(engine, "before_cursor_execute")
    def _block_writes(conn, cursor, statement, parameters, context, executemany):
        stmt_upper = statement.lstrip().upper()
        if stmt_upper.startswith(_WRITE_PREFIXES):
            raise RuntimeError(
                f"Refusing write operation on read-only mappings engine: "
                f"{statement[:120]}..."
            )

    return engine


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_all_state_tables(engine):
    import deid.models.state  # noqa: F401 — ensure models are registered
    StateBase.metadata.create_all(engine)


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_all_mappings_tables(engine):
    import deid.models.mappings  # noqa: F401 — ensure models are registered
    MappingsBase.metadata.create_all(engine)


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_failed_rows_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_all_failed_rows_tables(engine):
    from deid.models.failed_rows import FailedRowsBase
    import deid.models.failed_rows  # noqa: F401 — ensure models are registered
    FailedRowsBase.metadata.create_all(engine)
