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
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_state_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_mappings_engine(db_path: str):
    """Create a read-write SQLite engine for mapping population (``deid mapping`` only)."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


_WRITE_PREFIXES = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "CREATE", "TRUNCATE", "REPLACE", "MERGE", "UPSERT",
)


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_read_only_mappings_engine(conn_str: str):
    """Create a read-only engine for the mappings database.

    Accepts a full connection string (``sqlite:///path``, ``mysql+pymysql://...``, etc.).
    For backwards compatibility, bare file paths are treated as SQLite.

    Two layers of protection:
    1. Dialect-specific session-level READ ONLY.
    2. ``before_cursor_execute`` guard that blocks any DML/DDL statement.
    """
    if "://" not in conn_str:
        conn_str = f"sqlite:///{conn_str}"
    engine = create_engine(conn_str, echo=False)

    @event.listens_for(engine, "connect")
    def _set_read_only(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        dialect = engine.dialect.name
        if dialect == "sqlite":
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA query_only=ON")
        elif dialect == "mysql":
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
        elif dialect == "postgresql":
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        elif dialect == "mssql":
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
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


# Module-level state engine cache — one engine per state_db_path, per process.
# Prefork workers get their own copy after fork (empty at fork time, populated
# lazily), so there are no fork-safety concerns with stale connections.
_state_engine_cache: dict = {}


def get_cached_state_engine(db_path: str):
    """Return a cached state engine for ``db_path``.

    Creates the engine and initialises tables on first call per process.
    Subsequent calls return the same engine, eliminating the overhead of
    engine create/dispose cycles on every batch status update in the hot
    task path.
    """
    if db_path not in _state_engine_cache:
        engine = create_state_engine(db_path)
        create_all_state_tables(engine)
        _state_engine_cache[db_path] = engine
    return _state_engine_cache[db_path]


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


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_qc_results_engine(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _enable_wal)
    return engine


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_all_qc_results_tables(engine):
    from deid.models.qc_results import QCResultsBase
    import deid.models.qc_results  # noqa: F401 — ensure models are registered
    QCResultsBase.metadata.create_all(engine)
