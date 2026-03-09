from sqlalchemy import create_engine, event, MetaData, Table, text, func, Column
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from deid.core.logger import nd_logger
from sqlalchemy import String
import datetime
import decimal
import os
import polars as pl
from typing import Iterator, List, Dict
from pydantic import validate_call


@validate_call(config=dict(arbitrary_types_allowed=True))
def _normalize_value(v):
    """Convert Python objects that Polars can't handle uniformly to plain scalars.

    SQLAlchemy returns typed Python objects for certain DB column types:
      - DATE / DATETIME  → ``datetime.date`` / ``datetime.datetime``
      - DECIMAL / NUMERIC → ``decimal.Decimal``
      - BLOB / BINARY     → ``bytes``

    Within a single batch the same column may contain a mix of these objects
    *and* plain strings (e.g. when the DB stores dates as VARCHAR) or None.
    Polars infers the column dtype from the first non-None value it sees; if
    a later row carries a different Python type Polars raises::

        ComputeError: could not append value: 2024-09-18 of type: date …

    Normalising every cell to ``str | float | int | None`` before handing
    the batch to Polars avoids the ambiguity entirely.
    """
    if v is None:
        return v
    if isinstance(v, datetime.datetime):
        # datetime before date because datetime IS a date (subclass)
        return v.isoformat(sep=" ")
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return v


@validate_call(config=dict(arbitrary_types_allowed=True))
def _normalize_rows(rows) -> list:
    """Apply _normalize_value to every cell in every row."""
    return [[_normalize_value(cell) for cell in row] for row in rows]


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_read_only_engine(connection_string: str, **kwargs):
    """Create a SQLAlchemy engine that enforces read-only at the DB session level."""
    engine = create_engine(connection_string, **kwargs)

    @event.listens_for(engine, "connect")
    def _set_read_only(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        dialect = engine.dialect.name
        if dialect == "mysql":
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
        elif dialect == "postgresql":
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        elif dialect == "mssql":
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.close()

    return engine


def dump_table_to_ipc_cache(
    stream: Iterator[pl.DataFrame],
    cache_dir: str,
) -> str | None:
    """Write a DataFrame stream to Arrow IPC batch files in *cache_dir*.

    Each DataFrame yielded by *stream* is written as a separate
    ``batch_NNNNN.arrow`` file.  Returns *cache_dir* on success, or
    ``None`` if the stream was empty.
    """
    batch_count = 0
    total_rows = 0
    for df in stream:
        if df.is_empty():
            continue
        if batch_count == 0:
            os.makedirs(cache_dir, exist_ok=True)
        df.write_ipc(os.path.join(cache_dir, f"batch_{batch_count:05d}.arrow"))
        batch_count += 1
        total_rows += df.height
        nd_logger.info(
            f"[IPC Cache] {cache_dir}: wrote batch {batch_count} ({df.height} rows, {total_rows} total)"
        )

    return cache_dir if batch_count > 0 else None


def stream_from_ipc_cache(
    cache_dir: str,
    start_id: int,
    end_id: int,
    id_column: str = "nd_auto_increment_id",
) -> Iterator[pl.DataFrame]:
    """Iterate Arrow IPC batch files in *cache_dir*, filtering each to the given ID range.

    The cache directory contains files named ``batch_NNNNN.arrow``, one per
    batch written during the cache phase.  Each file is read independently
    so memory stays at O(batch_size).

    Yields only non-empty DataFrames after filtering.
    """
    import glob as _glob

    paths = sorted(_glob.glob(os.path.join(cache_dir, "batch_*.arrow")))
    for path in paths:
        df = pl.read_ipc(path)
        df = df.filter(pl.col(id_column).is_between(start_id, end_id))
        if not df.is_empty():
            yield df


class NDDBHandler:
    def __init__(self, connection_string: str, read_only: bool = False):
        self.read_only = read_only
        engine_kwargs = dict(pool_size=5, max_overflow=5, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True)
        if read_only:
            self.engine = create_read_only_engine(connection_string, **engine_kwargs)
        else:
            self.engine = create_engine(connection_string, **engine_kwargs)

        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _qi(self, identifier: str) -> str:
        """Quote a table or column identifier for the current dialect."""
        if self.engine.dialect.name == "mssql":
            return f"[{identifier}]"
        return f"`{identifier}`"

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def close(self):
        self.session.close()
        self.engine.dispose()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_columns(self, table_name: str) -> list[dict]:
        inspector = reflection.Inspector.from_engine(self.engine)
        return inspector.get_columns(table_name)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _assert_writable(self, operation: str):
        if self.read_only:
            raise RuntimeError(
                f"Refusing to {operation}: this NDDBHandler is read-only (source database). "
                "Write operations must target the destination database."
            )

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def insert_to_db(self, rows: list[dict], table_name: str, batch_size: int = 10000):
        self._assert_writable(f"INSERT into {table_name}")
        import pymysql
        if not rows:
            nd_logger.warning(f"No rows to insert into {table_name}.")
            return

        connection = self.engine.raw_connection()  # Get raw DB connection
        cursor = connection.cursor()

        try:
            # Dynamically generate column names
            columns = rows[0].keys()
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {self._qi(table_name)} ({', '.join(self._qi(col) for col in columns)}) VALUES ({placeholders})"

            # Convert rows to tuple format
            data = [tuple(row.values()) for row in rows]

            # Execute bulk insert
            cursor.executemany(sql, data)
            connection.commit()
            nd_logger.info(f"Inserted {len(rows)} rows into {table_name} successfully.")
        except pymysql.err.OperationalError as e:
            connection.rollback()
            nd_logger.error(f"Error inserting into {table_name}: {e}")
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def _portable_type(col_type):
        """Map dialect-specific column types to portable generic types.

        MSSQL types like MONEY, SMALLMONEY, IMAGE, NVARCHAR(MAX), etc. have
        no direct equivalent in MySQL/PostgreSQL.  This converts them to
        standard SQL types so cross-dialect CREATE TABLE works.

        Also strips MSSQL-specific collations (e.g. SQL_Latin1_General_CP1_CI_AS)
        from string columns so MySQL doesn't reject them.
        """
        from sqlalchemy import Numeric, LargeBinary, Text, UnicodeText
        type_name = type(col_type).__name__.upper()
        if type_name in ("MONEY", "SMALLMONEY"):
            return Numeric(19, 4)
        if type_name == "IMAGE":
            return LargeBinary()
        if type_name == "NTEXT":
            return UnicodeText()
        if type_name in ("SQL_VARIANT", "UNIQUEIDENTIFIER"):
            return String(255)
        # NVARCHAR/VARCHAR without a length (MSSQL MAX) → TEXT/LONGTEXT
        if type_name in ("NVARCHAR", "NCHAR") and not getattr(col_type, "length", None):
            return UnicodeText()
        if type_name in ("VARCHAR", "CHAR") and not getattr(col_type, "length", None):
            return Text()
        # Strip MSSQL collations from string-like types
        if hasattr(col_type, "collation") and col_type.collation:
            col_type = col_type.copy()
            col_type.collation = None
        return col_type

    def create_table_in_dest(
        self,
        source_table_name: str,
        dest_handler: "NDDBHandler",
        dest_table_name: str = None,
        column_type_mapping: dict = {},
    ):
        dest_handler._assert_writable(f"CREATE TABLE {dest_table_name or source_table_name}")
        dest_table_name = dest_table_name or source_table_name

        source_table = Table(
            source_table_name, self.metadata, autoload_with=self.engine
        )
        cross_dialect = self.engine.dialect.name != dest_handler.engine.dialect.name
        mapped_columns = []
        for column in source_table.columns:
            col_name = column.name
            if col_name in column_type_mapping:
                mapping = column_type_mapping[col_name]
                new_type, col_nullable = self.get_column_type(mapping)
                col_nullable = col_nullable if (col_nullable is not None) else column.nullable
                mapped_columns.append(
                    Column(col_name, new_type, nullable=col_nullable)
                )
            else:
                col_type = self._portable_type(column.type) if cross_dialect else column.type
                mapped_columns.append(
                    Column(col_name, col_type, nullable=column.nullable)
                )

        dest_table = Table(dest_table_name, dest_handler.metadata, *mapped_columns)

        dest_table.create(dest_handler.engine)
        nd_logger.info(
            f"Table {dest_table_name} created in destination database with modified schema."
        )

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_column_type(self, col_info):
        col_nullable = col_info.get("null", None)
        col_type = col_info["type"]
        if col_type == String:
            return col_type(col_info.get("length")), col_nullable
        return col_type, col_nullable

    def create_table_in_dest_if_not_exists(
        self,
        source_table_name: str,
        dest_handler: "NDDBHandler",
        dest_table_name: str = None,
        column_type_mapping: dict = {},
    ):
        dest_table_name = dest_table_name or source_table_name
        if self._table_exists(dest_handler, dest_table_name):
            nd_logger.info(
                f"Table {dest_table_name} already exists in destination database."
            )
            return
        self.create_table_in_dest(
            source_table_name, dest_handler, dest_table_name, column_type_mapping
        )

    def _table_exists(self, dest_handler: "NDDBHandler", table_name: str) -> bool:
        try:
            dest_handler.session.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
            return True
        except ProgrammingError:
            return False

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_all_tables(self) -> list[str]:
        inspector = reflection.Inspector.from_engine(self.engine)
        return inspector.get_table_names()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_rows_count(self, table_name: str) -> int:
        """Fast estimated row count using catalog metadata (no full table scan).

        MySQL  → information_schema.TABLES  (InnoDB estimate, instant)
        MSSQL  → sys.dm_db_partition_stats  (heap/clustered index, instant)
        PG     → pg_class.reltuples         (ANALYZE estimate, instant)
        Other  → COUNT(*) fallback

        Uses engine.connect() (not self.session) so it is safe to call from
        multiple threads concurrently (e.g. asyncio.gather + run_in_executor).
        """
        dialect = self.engine.dialect.name

        with self.engine.connect() as conn:
            if dialect == "mysql":
                result = conn.execute(text(
                    "SELECT TABLE_ROWS FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
                ), {"table_name": table_name})
                count = result.scalar()
                if count is not None:
                    return int(count)

            elif dialect == "mssql":
                result = conn.execute(text(
                    "SELECT SUM(p.row_count) FROM sys.dm_db_partition_stats p "
                    "JOIN sys.tables t ON p.object_id = t.object_id "
                    "WHERE t.name = :table_name AND p.index_id IN (0, 1)"
                ), {"table_name": table_name})
                count = result.scalar()
                if count is not None:
                    return int(count)

            elif dialect == "postgresql":
                result = conn.execute(text(
                    "SELECT reltuples::bigint FROM pg_class "
                    "WHERE relname = :table_name"
                ), {"table_name": table_name})
                count = result.scalar()
                if count is not None and count >= 0:
                    return int(count)

            # Fallback: exact COUNT(*)
            table = Table(table_name, MetaData(), autoload_with=self.engine)
            result = conn.execute(func.count().select().select_from(table))
            return result.scalar()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_min_max_id(self, table_name: str, id_column: str = "nd_auto_increment_id") -> tuple[int, int] | None:
        """Return (min_id, max_id) for the given table's ID column, or None."""
        qi = self._qi
        query = text(
            f"SELECT MIN({qi(id_column)}), MAX({qi(id_column)}) "
            f"FROM {qi(table_name)} WHERE {qi(id_column)} IS NOT NULL"
        )
        with self.engine.connect() as conn:
            try:
                row = conn.execute(query).fetchone()
                if not row or row[0] is None:
                    return None
                return int(row[0]), int(row[1])
            except Exception:
                return None

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_keyset_pagination_ranges(self, table_name: str, id_column: str = "nd_auto_increment_id", batch_size: int = 100000) -> List[Dict[str, int]]:
        qi = self._qi
        min_max_query = text(
            f"SELECT MIN({qi(id_column)}), MAX({qi(id_column)}) "
            f"FROM {qi(table_name)} WHERE {qi(id_column)} IS NOT NULL"
        )
        with self.engine.connect() as conn:
            try:
                result = conn.execute(min_max_query)
                row = result.fetchone()
                if not row or row[0] is None:
                    return None
                min_id, max_id = int(row[0]), int(row[1])
            except Exception:
                return None

        ranges = []
        current = min_id
        while current <= max_id:
            end = min(current + batch_size - 1, max_id)
            ranges.append({"gt": current, "lt": end})
            current = end + 1
        return ranges


    @validate_call(config=dict(arbitrary_types_allowed=True))
    def stream_table_as_dataframes_in_range(
        self,
        table_name: str,
        batch_size: int,
        start_id: int,
        end_id: int,
        id_column: str = "nd_auto_increment_id",
    ) -> Iterator[pl.DataFrame]:
        """Stream a keyset-bounded slice of a table.

        Identical to ``stream_table_as_dataframes`` but adds a WHERE clause so
        only rows with ``id_column BETWEEN start_id AND end_id`` are returned.
        This lets multiple workers process disjoint ranges of the same table in
        parallel without any coordination overhead — each worker just needs the
        two boundary values in its task payload.

        No row count is required; the server cursor stops when the range is
        exhausted, keeping memory at O(batch_size).
        """
        qi = self._qi
        query = text(
            f"SELECT * FROM {qi(table_name)} "
            f"WHERE {qi(id_column)} BETWEEN :start_id AND :end_id"
        )
        conn = self.engine.connect().execution_options(
            stream_results=True,
            max_row_buffer=batch_size,
        )
        try:
            result = conn.execute(query, {"start_id": start_id, "end_id": end_id})
            columns = list(result.keys())
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                yield pl.DataFrame(
                    _normalize_rows(rows),
                    schema=columns,
                    orient="row",
                    infer_schema_length=len(rows),
                )
        finally:
            conn.close()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def stream_table_as_dataframes(self, table_name: str, batch_size: int) -> Iterator[pl.DataFrame]:
        """Stream a table as an iterator of Polars DataFrames using a server-side cursor.

        For MySQL+PyMySQL, `stream_results=True` activates SSCursor so rows are
        fetched from the server in batches rather than loading the full result set
        into client memory.  No row count or pagination ranges are needed — the
        cursor simply stops when it has no more rows.

        Memory stays at O(batch_size) regardless of table size, making 50 M-row
        tables as cheap to start as 50-row tables.

        Yields Polars DataFrames rather than Pandas to exploit Polars' faster
        joins, column expressions, and lower memory footprint downstream.
        """
        query = text(f"SELECT * FROM {self._qi(table_name)}")
        conn = self.engine.connect().execution_options(
            stream_results=True,
            max_row_buffer=batch_size,
        )
        try:
            result = conn.execute(query)
            columns = list(result.keys())
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                yield pl.DataFrame(
                    _normalize_rows(rows),
                    schema=columns,
                    orient="row",
                    # Scan every row in the batch before fixing column dtypes.
                    # Without this, Polars locks the schema after the first
                    # `infer_schema_length` (default 100) rows. If those rows
                    # are all NULL for a column and a later row holds a string
                    # (e.g. "7/29/2019"), Polars raises ComputeError.
                    infer_schema_length=len(rows),
                )
        finally:
            conn.close()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def insert_dataframe_in_batches(self, df: pl.DataFrame, table_name: str, batch_size: int = 10000) -> None:
        """Insert a Polars DataFrame into the given MySQL table in batches.

        Polars uses typed nulls (None) rather than float NaN, so no sanitization
        step is needed — `to_dicts()` already converts null cells to Python None.
        Only columns that exist in the destination table schema are inserted.

        String values are automatically truncated to the destination column's
        declared max-length to prevent MySQL 1265 "Data truncated" errors that
        occur when a de-identification placeholder is longer than the original
        column definition (e.g. VARCHAR(10) receiving "((UNIT_OF_MEASURE))").
        """
        self._assert_writable(f"INSERT into {table_name}")
        if df.is_empty():
            nd_logger.warning(f"[DBHandler] Empty DataFrame. Nothing to insert into '{table_name}'.")
            return

        # ── 1. Keep only columns that exist in the destination table ──────────
        col_defs = self.get_columns(table_name)
        valid_columns = [c["name"] for c in col_defs]
        select_cols = [c for c in valid_columns if c in df.columns]
        df = df.select(select_cols)

        # ── 2. Build a {col_name: max_len} map for bounded-string columns ─────
        # SQLAlchemy returns String / VARCHAR types with a `.length` attribute.
        # We only truncate columns that are (a) present in the DataFrame AND
        # (b) have a non-null, positive length declared in the schema.
        max_lengths: dict[str, int] = {}
        for col_def in col_defs:
            col_name = col_def["name"]
            if col_name not in df.columns:
                continue
            col_type = col_def.get("type")
            length = getattr(col_type, "length", None)
            if length and length > 0:
                max_lengths[col_name] = int(length)

        # ── 3. Truncate string columns that exceed the declared max-length ────
        _STRING_DTYPES = {pl.Utf8, pl.String, pl.Categorical}
        if max_lengths:
            truncate_exprs = []
            for col_name, max_len in max_lengths.items():
                try:
                    col_dtype = df[col_name].dtype
                except Exception:
                    continue
                if col_dtype in _STRING_DTYPES or str(col_dtype) in ("Utf8", "String"):
                    truncate_exprs.append(
                        pl.col(col_name)
                        .cast(pl.Utf8)
                        .str.slice(0, max_len)
                        .alias(col_name)
                    )
            if truncate_exprs:
                df = df.with_columns(truncate_exprs)
                nd_logger.debug(
                    f"[DBHandler] Safety-truncated {len(truncate_exprs)} string "
                    f"column(s) to their declared max lengths for '{table_name}'."
                )

        total_rows = df.height
        nd_logger.info(f"[DBHandler] Starting insertion of {total_rows} rows into '{table_name}' in batches of {batch_size}.")

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_df = df.slice(start, batch_size)
            try:
                rows = batch_df.to_dicts()  # nulls become Python None automatically
                self.insert_to_db(rows, table_name)
                nd_logger.info(f"[DBHandler] Inserted rows {start + 1} to {end} into '{table_name}'.")
            except Exception as e:
                nd_logger.error(f"[DBHandler] Failed to insert batch {start + 1} to {end}: {e}")
                raise

        nd_logger.info(f"[DBHandler] Completed insertion into '{table_name}'.")


# ---------------------------------------------------------------------------
# Deferred @validate_call for methods with "NDDBHandler" forward references.
# Pydantic's validate_call eagerly resolves type hints at decoration time,
# but during class body execution NDDBHandler isn't yet in module scope.
# Applying the decorator here (after the class is defined) lets the forward
# reference resolve normally.
# ---------------------------------------------------------------------------
_vc = validate_call(config=dict(arbitrary_types_allowed=True))
NDDBHandler.create_table_in_dest = _vc(NDDBHandler.create_table_in_dest)
NDDBHandler.create_table_in_dest_if_not_exists = _vc(NDDBHandler.create_table_in_dest_if_not_exists)
NDDBHandler._table_exists = _vc(NDDBHandler._table_exists)
