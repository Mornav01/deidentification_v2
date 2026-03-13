from sqlalchemy import create_engine, event, inspect, MetaData, Table, text, func, Column
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


def _normalize_value(v):
    """Convert Python objects that Polars can't handle uniformly to plain scalars."""
    if v is None:
        return v
    if isinstance(v, datetime.datetime):
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


# Per-column normalizers: only convert columns that need it
_NORMALIZERS = {
    datetime.datetime: lambda v: v.isoformat(sep=" "),
    datetime.date: lambda v: v.isoformat(),
    decimal.Decimal: float,
    bytes: lambda v: v.decode("utf-8", errors="replace"),
}

# Cache: table_name -> {col_idx: normalizer_fn}  (populated on first batch)
_COLUMN_TYPE_CACHE: dict[str, dict[int, object]] = {}


def _detect_column_normalizers(rows, table_name: str = "") -> dict[int, object]:
    """Inspect first non-None value per column; return map of col_idx -> normalizer."""
    if table_name and table_name in _COLUMN_TYPE_CACHE:
        return _COLUMN_TYPE_CACHE[table_name]

    if not rows:
        return {}

    col_normalizers: dict[int, object] = {}
    n_cols = len(rows[0])
    for col_idx in range(n_cols):
        for row in rows:
            val = row[col_idx]
            if val is not None:
                norm = _NORMALIZERS.get(type(val))
                if norm is not None:
                    col_normalizers[col_idx] = norm
                break

    if table_name:
        _COLUMN_TYPE_CACHE[table_name] = col_normalizers
    return col_normalizers


def _normalize_rows(rows, table_name: str = "") -> list:
    """Normalize only columns that need it, using per-column type detection."""
    if not rows:
        return []

    col_normalizers = _detect_column_normalizers(rows, table_name)
    if not col_normalizers:
        return [list(row) for row in rows]

    result = []
    for row in rows:
        new_row = list(row)
        for col_idx, normalizer in col_normalizers.items():
            val = new_row[col_idx]
            if val is not None:
                new_row[col_idx] = normalizer(val)
        result.append(new_row)
    return result


_WRITE_PREFIXES = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "CREATE", "TRUNCATE", "REPLACE", "MERGE", "UPSERT",
    "EXEC ", "EXECUTE ",
)


@validate_call(config=dict(arbitrary_types_allowed=True))
def create_read_only_engine(connection_string: str, **kwargs):
    """Create a SQLAlchemy engine that enforces read-only at the DB session level.

    Two layers of protection:
    1. Dialect-specific session-level READ ONLY (MySQL, PostgreSQL, Snowflake).
       MSSQL lacks a true session-level READ ONLY so READ UNCOMMITTED is kept
       as a best-effort signal.
    2. Universal ``before_cursor_execute`` guard that blocks any DML/DDL
       statement regardless of dialect.
    """
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
        elif dialect == "snowflake":
            cursor.execute("ALTER SESSION SET TRANSACTION_DEFAULT_ISOLATION_LEVEL = 'READ COMMITTED'")
        cursor.close()

    @event.listens_for(engine, "before_cursor_execute")
    def _block_writes(conn, cursor, statement, parameters, context, executemany):
        stmt_upper = statement.lstrip().upper()
        if stmt_upper.startswith(_WRITE_PREFIXES):
            raise RuntimeError(
                f"Refusing write operation on read-only (source) engine: "
                f"{statement[:120]}..."
            )

    return engine


def dump_table_to_ipc_cache(
    stream: Iterator[pl.DataFrame],
    cache_dir: str,
    table_name: str = "",
    estimated_rows: int = 0,
) -> dict | None:
    """Write a DataFrame stream to Arrow IPC batch files in *cache_dir*.

    Each DataFrame yielded by *stream* is written as a separate
    ``batch_NNNNN.arrow`` file.  Returns a summary dict on success, or
    ``None`` if the stream was empty.
    """
    import time
    from tqdm import tqdm

    batch_count = 0
    total_rows = 0
    t0 = time.monotonic()

    pbar = tqdm(
        total=estimated_rows or None,
        desc=f"[IPC Cache] {table_name}",
        unit=" rows",
        unit_scale=True,
    )

    for df in stream:
        if df.is_empty():
            continue
        if batch_count == 0:
            os.makedirs(cache_dir, exist_ok=True)
        df.write_ipc(os.path.join(cache_dir, f"batch_{batch_count:05d}.arrow"))
        batch_count += 1
        total_rows += df.height
        pbar.update(df.height)

    pbar.close()

    if batch_count == 0:
        return None

    elapsed = time.monotonic() - t0
    rate = int(total_rows / elapsed) if elapsed > 0 else 0
    nd_logger.info(
        f"[IPC Cache] {table_name}: {total_rows:,} rows cached"
        f" — {batch_count} batches, {rate:,} rows/s, {elapsed:.0f}s elapsed"
    )
    return {
        "cache_dir": cache_dir,
        "batches": batch_count,
        "rows": total_rows,
        "elapsed_s": round(elapsed, 1),
    }


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


def stream_table_paginated(
    handler: "NDDBHandler",
    table_name: str,
    min_id: int,
    max_id: int,
    page_size: int,
    id_column: str = "nd_auto_increment_id",
) -> Iterator[pl.DataFrame]:
    """Paginate through a table by ID range, yielding one DataFrame per page.

    Unlike ``stream_table_as_dataframes`` which relies on server-side cursors
    (``stream_results=True``), this issues separate bounded ``SELECT`` queries.
    Each query returns at most *page_size* rows, guaranteeing O(page_size)
    memory regardless of DB driver buffering behaviour (e.g. pymssql for MSSQL
    may buffer the entire result set even with ``stream_results=True``).
    """
    qi = handler._qi
    dialect = handler.engine.dialect.name
    nolock = " WITH (NOLOCK)" if dialect == "mssql" else ""
    query = text(
        f"SELECT * FROM {qi(table_name)}{nolock} "
        f"WHERE {qi(id_column)} BETWEEN :start AND :end"
    )
    chunk_start = min_id
    while chunk_start <= max_id:
        chunk_end = min(chunk_start + page_size - 1, max_id)
        with handler.engine.connect() as conn:
            result = conn.execute(query, {"start": chunk_start, "end": chunk_end})
            columns = list(result.keys())
            rows = result.fetchall()
        if rows:
            yield pl.DataFrame(
                _normalize_rows(rows, table_name),
                schema=columns,
                orient="row",
                infer_schema_length=len(rows),
            )
        chunk_start = chunk_end + 1


def stream_table_offset(
    handler: "NDDBHandler",
    table_name: str,
    offset: int,
    limit: int,
) -> Iterator[pl.DataFrame]:
    """Fetch exactly *limit* rows starting at *offset* using OFFSET/LIMIT.

    Column-agnostic: works on any table regardless of primary key layout.
    Returns a single DataFrame (the whole slice) to keep the interface
    consistent with ``stream_table_paginated``.
    """
    qi = handler._qi
    dialect = handler.engine.dialect.name
    if dialect == "mssql":
        # MSSQL requires ORDER BY for OFFSET; (SELECT NULL) avoids picking a column.
        query = text(
            f"SELECT * FROM {qi(table_name)} WITH (NOLOCK) "
            f"ORDER BY (SELECT NULL) OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
        )
    else:
        query = text(f"SELECT * FROM {qi(table_name)} LIMIT :limit OFFSET :offset")

    with handler.engine.connect() as conn:
        result = conn.execute(query, {"offset": offset, "limit": limit})
        columns = list(result.keys())
        rows = result.fetchall()
    if rows:
        yield pl.DataFrame(
            _normalize_rows(rows, table_name),
            schema=columns,
            orient="row",
            infer_schema_length=len(rows),
        )


def stream_table_keyset(
    handler: "NDDBHandler",
    table_name: str,
    batch_size: int,
    last_id: int | None = None,
    id_column: str = "nd_auto_increment_id",
) -> Iterator[pl.DataFrame]:
    """Fetch batch_size rows using keyset pagination (O(1) per batch with index).

    Unlike OFFSET pagination, each batch costs the same regardless of position.
    Returns rows ordered by id_column, starting after last_id.
    """
    qi = handler._qi
    dialect = handler.engine.dialect.name

    if dialect == "mssql":
        if last_id is not None:
            query = text(
                f"SELECT * FROM {qi(table_name)} WITH (NOLOCK) "
                f"WHERE {qi(id_column)} > :last_id "
                f"ORDER BY {qi(id_column)} "
                f"OFFSET 0 ROWS FETCH NEXT :batch_size ROWS ONLY"
            )
            params: dict = {"last_id": last_id, "batch_size": batch_size}
        else:
            query = text(
                f"SELECT * FROM {qi(table_name)} WITH (NOLOCK) "
                f"ORDER BY {qi(id_column)} "
                f"OFFSET 0 ROWS FETCH NEXT :batch_size ROWS ONLY"
            )
            params = {"batch_size": batch_size}
    else:
        if last_id is not None:
            query = text(
                f"SELECT * FROM {qi(table_name)} "
                f"WHERE {qi(id_column)} > :last_id "
                f"ORDER BY {qi(id_column)} "
                f"LIMIT :batch_size"
            )
            params = {"last_id": last_id, "batch_size": batch_size}
        else:
            query = text(
                f"SELECT * FROM {qi(table_name)} "
                f"ORDER BY {qi(id_column)} "
                f"LIMIT :batch_size"
            )
            params = {"batch_size": batch_size}

    with handler.engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(query, params)
        columns = list(result.keys())
        rows = result.fetchall()
    if rows:
        yield pl.DataFrame(
            _normalize_rows(rows, table_name),
            schema=columns,
            orient="row",
            infer_schema_length=len(rows),
        )


class NDDBHandler:
    def __init__(self, connection_string: str, read_only: bool = False):
        self.read_only = read_only
        if read_only:
            # Read-only workers need a single connection; keeping the pool
            # small avoids flooding the source DB when many workers run.
            engine_kwargs = dict(pool_size=1, max_overflow=2, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True)
            self.engine = create_read_only_engine(connection_string, **engine_kwargs)
        else:
            engine_kwargs = dict(pool_size=5, max_overflow=5, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True)
            self.engine = create_engine(connection_string, **engine_kwargs)

        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self._columns_cache: dict[str, list[dict]] = {}


    def _qi(self, identifier: str) -> str:
        """Quote a table or column identifier for the current dialect."""
        if self.engine.dialect.name == "mssql":
            return f"[{identifier}]"
        return f"`{identifier}`"


    def close(self):
        self.session.close()
        self.engine.dispose()


    def get_columns(self, table_name: str) -> list[dict]:
        if table_name in self._columns_cache:
            return self._columns_cache[table_name]
        with self.engine.connect() as conn:
            columns = list(inspect(conn).get_columns(table_name))
        self._columns_cache[table_name] = columns
        return columns


    def _assert_writable(self, operation: str):
        if self.read_only:
            raise RuntimeError(
                f"Refusing to {operation}: this NDDBHandler is read-only (source database). "
                "Write operations must target the destination database."
            )


    def insert_to_db(self, rows: list[dict], table_name: str, batch_size: int = 10000):
        self._assert_writable(f"INSERT into {table_name}")
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
        except Exception as e:
            connection.rollback()
            nd_logger.error(f"Error inserting into {table_name}: {e}")
            raise
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
        inspector = reflection.Inspector.from_engine(dest_handler.engine)
        return inspector.has_table(table_name)


    def get_all_tables(self) -> list[str]:
        inspector = reflection.Inspector.from_engine(self.engine)
        return inspector.get_table_names()


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


    def get_exact_row_count(self, table_name: str) -> int:
        """Return exact row count via COUNT(*).

        Slower than ``get_rows_count`` (which uses catalog estimates) but
        accurate — required for OFFSET-based batch splitting.
        """
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {qi(table_name)}{nolock}"))
            return int(result.scalar())


    def get_min_max_id(self, table_name: str, id_column: str = "nd_auto_increment_id") -> tuple[int, int] | None:
        """Return (min_id, max_id) for the given table's ID column, or None."""
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(
            f"SELECT MIN({qi(id_column)}), MAX({qi(id_column)}) "
            f"FROM {qi(table_name)}{nolock} WHERE {qi(id_column)} IS NOT NULL"
        )
        with self.engine.connect() as conn:
            try:
                row = conn.execute(query).fetchone()
                if not row or row[0] is None:
                    return None
                return int(row[0]), int(row[1])
            except Exception:
                return None


    def get_keyset_pagination_ranges(self, table_name: str, id_column: str = "nd_auto_increment_id", batch_size: int = 100000) -> List[Dict[str, int]]:
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        min_max_query = text(
            f"SELECT MIN({qi(id_column)}), MAX({qi(id_column)}) "
            f"FROM {qi(table_name)}{nolock} WHERE {qi(id_column)} IS NOT NULL"
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
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(
            f"SELECT * FROM {qi(table_name)}{nolock} "
            f"WHERE {qi(id_column)} BETWEEN :start_id AND :end_id"
        )
        with self.engine.connect() as conn:
            conn = conn.execution_options(
                stream_results=True,
                max_row_buffer=batch_size,
            )
            result = conn.execute(query, {"start_id": start_id, "end_id": end_id})
            columns = list(result.keys())
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                yield pl.DataFrame(
                    _normalize_rows(rows, table_name),
                    schema=columns,
                    orient="row",
                    infer_schema_length=len(rows),
                )


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
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(f"SELECT * FROM {self._qi(table_name)}{nolock}")
        with self.engine.connect() as conn:
            conn = conn.execution_options(
                stream_results=True,
                max_row_buffer=batch_size,
            )
            result = conn.execute(query)
            columns = list(result.keys())
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                yield pl.DataFrame(
                    _normalize_rows(rows, table_name),
                    schema=columns,
                    orient="row",
                    # Scan every row in the batch before fixing column dtypes.
                    # Without this, Polars locks the schema after the first
                    # `infer_schema_length` (default 100) rows. If those rows
                    # are all NULL for a column and a later row holds a string
                    # (e.g. "7/29/2019"), Polars raises ComputeError.
                    infer_schema_length=len(rows),
                )


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

    def fetch_distinct_values(self, table_name: str, column_name: str, batch_size: int = 10000) -> Iterator[str]:
        """Yield distinct non-NULL values of a single column from *table_name*."""
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(
            f"SELECT DISTINCT {qi(column_name)} FROM {qi(table_name)}{nolock} "
            f"WHERE {qi(column_name)} IS NOT NULL"
        )
        with self.engine.connect() as conn:
            conn = conn.execution_options(
                stream_results=True,
                max_row_buffer=batch_size,
            )
            result = conn.execute(query)
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield str(row[0])

    def fetch_distinct_pairs(self, table_name: str, col_a: str, col_b: str, batch_size: int = 10000) -> Iterator[tuple[str, str]]:
        """Yield distinct non-NULL (col_a, col_b) pairs from *table_name*."""
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(
            f"SELECT DISTINCT {qi(col_a)}, {qi(col_b)} FROM {qi(table_name)}{nolock} "
            f"WHERE {qi(col_a)} IS NOT NULL AND {qi(col_b)} IS NOT NULL"
        )
        with self.engine.connect() as conn:
            conn = conn.execution_options(
                stream_results=True,
                max_row_buffer=batch_size,
            )
            result = conn.execute(query)
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield (str(row[0]), str(row[1]))

