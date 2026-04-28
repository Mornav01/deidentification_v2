from sqlalchemy import create_engine, event, inspect, MetaData, Table, text, func, Column, String
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from deid.core.logger import nd_logger
from sqlalchemy.types import Enum as SAEnum

from deid.core.dbPkg.type_mapping import mssql_type_to_mysql
import datetime
import decimal
import os
try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already imported
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
            if val is not None and not isinstance(val, str):
                new_row[col_idx] = normalizer(val)
        result.append(new_row)
    return result


def _parse_mssql_table_ref(table_name: str, default_schema: str = "dbo") -> tuple[str | None, str]:
    """Parse MSSQL table reference into (schema, name).
    'dbo.users' → ('dbo', 'users'); 'users' → ('dbo', 'users')."""
    if "." in table_name:
        parts = table_name.split(".", 1)
        return parts[0].strip("[]"), parts[1].strip("[]")
    return default_schema, table_name


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
        elif dialect == "sqlite":
            cursor.execute("PRAGMA query_only=ON")
        cursor.close()

    @event.listens_for(engine, "before_cursor_execute")
    def _block_writes(conn, cursor, statement, parameters, context, executemany):
        stmt_upper = statement.lstrip().upper()
        if stmt_upper.startswith(_WRITE_PREFIXES):
            raise RuntimeError(
                f"Refusing write operation on read-only engine: "
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
        sel = handler._mssql_select_clause(table_name)
        if last_id is not None:
            query = text(
                f"SELECT {sel} FROM {qi(table_name)} WITH (NOLOCK) "
                f"WHERE {qi(id_column)} > :last_id "
                f"ORDER BY {qi(id_column)} "
                f"OFFSET 0 ROWS FETCH NEXT :batch_size ROWS ONLY"
            )
            params: dict = {"last_id": last_id, "batch_size": batch_size}
        else:
            query = text(
                f"SELECT {sel} FROM {qi(table_name)} WITH (NOLOCK) "
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


def _sqlalchemy_type_to_mysql_ddl(col_type) -> str:
    """Convert SQLAlchemy column type to MySQL DDL type string."""
    if col_type is None:
        return "VARCHAR(255)"
    if isinstance(col_type, SAEnum):
        return "VARCHAR(255)"
    type_cls = type(col_type)
    mod = getattr(type_cls, "__module__", "") or ""
    if "sqlalchemy.dialects.mysql" in mod:
        name = type_cls.__name__.upper()
        if "VARCHAR" in name or "CHAR" in name:
            length = getattr(col_type, "length", None)
            if length and isinstance(length, int) and length <= 255:
                return f"{'VARCHAR' if 'VARCHAR' in name else 'CHAR'}({length})"
            if length and isinstance(length, int) and length > 255:
                return "LONGTEXT"
            return "VARCHAR(255)"
        if "INT" in name or "INTEGER" in name:
            if "BIG" in name:
                return "BIGINT"
            if "SMALL" in name:
                return "SMALLINT"
            if "TINY" in name:
                return "TINYINT"
            return "INT"
        if "DECIMAL" in name or "NUMERIC" in name:
            p = getattr(col_type, "precision", 18) or 18
            s = getattr(col_type, "scale", 2) or 2
            if s >= 2 and p <= 18:
                p = max(p, 20)
            return f"DECIMAL({p},{s})"
        if "DATETIME" in name:
            fsp = getattr(col_type, "fsp", None)
            return f"DATETIME({fsp})" if fsp else "DATETIME"
        if "TEXT" in name:
            return "LONGTEXT" if "LONG" in name else "TEXT"
        if "BLOB" in name or "BINARY" in name:
            return "LONGBLOB" if "LONG" in name else "VARBINARY(255)"
        if "FLOAT" in name or "DOUBLE" in name:
            return "DOUBLE"
        if "DATE" in name and "TIME" not in name:
            return "DATE"
        if "TIME" in name:
            return "TIME"
        return name
    if "sqlalchemy.dialects.mssql" in mod:
        mapped = mssql_type_to_mysql(col_type)
        return _sqlalchemy_type_to_mysql_ddl(mapped)
    if isinstance(col_type, (String,)):
        length = getattr(col_type, "length", None)
        if length is None:
            return "VARCHAR(255)"
        if isinstance(length, int) and length > 255:
            return "LONGTEXT"
        return f"VARCHAR({length})"
    if hasattr(col_type, "length") and col_type.length:
        length = col_type.length
        if isinstance(length, int) and length > 255:
            return "LONGTEXT"
        return f"VARCHAR({length})"
    type_name = type_cls.__name__.upper()
    if "INT" in type_name:
        return "BIGINT" if "BIG" in type_name else "INT"
    if "FLOAT" in type_name or "NUMERIC" in type_name:
        return "DOUBLE"
    if "DATETIME" in type_name:
        return "DATETIME"
    if "DATE" in type_name:
        return "DATE"
    if "TIME" in type_name:
        return "TIME"
    if "BOOL" in type_name:
        return "TINYINT(1)"
    if "TEXT" in type_name or "STRING" in type_name:
        return "VARCHAR(255)"
    return "VARCHAR(255)"


_MYSQL_ROW_SIZE_LIMIT = 65535


def _estimate_mysql_inline_size(ddl_type: str) -> int:
    """Estimate bytes this column contributes to MySQL row size (utf8mb4)."""
    ddl_upper = ddl_type.upper()
    if "VARCHAR" in ddl_upper:
        m = re.search(r"VARCHAR\s*\(\s*(\d+)\s*\)", ddl_type, re.I)
        if m:
            return int(m.group(1)) * 4 + 2
        return 255 * 4 + 2
    if "CHAR" in ddl_upper and "VAR" not in ddl_upper:
        m = re.search(r"CHAR\s*\(\s*(\d+)\s*\)", ddl_type, re.I)
        if m:
            return int(m.group(1)) * 4
        return 255 * 4
    if "TINYINT" in ddl_upper:
        return 1
    if "INT" in ddl_upper:
        return 8 if "BIG" in ddl_upper else 4
    if "DATETIME" in ddl_upper or "TIMESTAMP" in ddl_upper:
        return 8
    if "DATE" in ddl_upper:
        return 4
    if "TIME" in ddl_upper:
        return 3
    if "DOUBLE" in ddl_upper or "FLOAT" in ddl_upper:
        return 8
    if "DECIMAL" in ddl_upper:
        m = re.search(r"DECIMAL\s*\(\s*(\d+)\s*", ddl_type, re.I)
        return max(8, (int(m.group(1)) + 2) // 2) if m else 8
    if "TEXT" in ddl_upper or "BLOB" in ddl_upper or "BINARY" in ddl_upper:
        return 20
    return 255 * 4 + 2


def _adjust_ddl_for_mysql_row_limit(col_specs: list[tuple[str, str, str]]) -> list[str]:
    """Convert some VARCHAR columns to TEXT if row size would exceed MySQL limit.

    col_specs: list of (col_name, ddl_type, nullable_str) e.g. ("x", "VARCHAR(255)", "")
    """
    total = 0
    var_cols: list[tuple[int, str, int]] = []
    for i, (col_name, ddl_type, _) in enumerate(col_specs):
        size = _estimate_mysql_inline_size(ddl_type)
        total += size
        if "VARCHAR" in ddl_type.upper() or ("CHAR" in ddl_type.upper() and "VAR" not in ddl_type.upper()):
            if "TEXT" not in ddl_type.upper() and "BLOB" not in ddl_type.upper():
                var_cols.append((i, ddl_type, size))

    if total <= _MYSQL_ROW_SIZE_LIMIT:
        return [f"`{n}` {t}{null}" for n, t, null in col_specs]

    var_cols.sort(key=lambda x: x[2], reverse=True)
    to_convert = set()
    current_total = total
    for i, _, size in var_cols:
        if current_total <= _MYSQL_ROW_SIZE_LIMIT:
            break
        to_convert.add(i)
        current_total -= size
        current_total += 20

    result = []
    converted = []
    for i, (col_name, ddl_type, nullable_str) in enumerate(col_specs):
        if i in to_convert and ("VARCHAR" in ddl_type.upper() or "CHAR" in ddl_type.upper()):
            result.append(f"`{col_name}` LONGTEXT{nullable_str}")
            converted.append(col_name)
        else:
            result.append(f"`{col_name}` {ddl_type}{nullable_str}")
    if converted:
        nd_logger.info(
            f"[create_table] Row size would exceed MySQL limit; converted {len(converted)} "
            f"VARCHAR/CHAR columns to LONGTEXT: {converted[:5]}{'...' if len(converted) > 5 else ''}"
        )
    return result


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
        self._is_mssql = self.engine.dialect.name == "mssql"
        # Caches (valid_columns, max_lengths, numeric_columns) per table — avoids
        # repeated DB round-trips in insert_dataframe_in_batches when writing many batches.
        self._insert_schema_cache: dict[str, tuple[list[str], dict[str, int], set[str]]] = {}


    def _qi(self, identifier: str) -> str:
        """Quote a table or column identifier for the current dialect."""
        if self._is_mssql:
            return f"[{identifier}]"
        return f"`{identifier}`"

    def _table_ref(self, table_name: str) -> tuple[str | None, str]:
        """Return (schema, name) for reflection. MSSQL: ('dbo','users'); MySQL: (None,'users')."""
        if self._is_mssql:
            return _parse_mssql_table_ref(table_name)
        return None, table_name

    def _reflect_table(self, table_name: str) -> Table:
        """Return a reflected Table with correct schema for the dialect."""
        schema, name = self._table_ref(table_name)
        if schema:
            return Table(name, self.metadata, schema=schema, autoload_with=self.engine)
        return Table(name, self.metadata, autoload_with=self.engine)

    def close(self):
        self.session.close()
        self.engine.dispose()


    def get_columns(self, table_name: str) -> list[dict]:
        if table_name in self._columns_cache:
            return self._columns_cache[table_name]
        if self._is_mssql:
            # MSSQL/pymssql: Inspector.get_columns(name, schema=) can fail on views;
            # Table reflection is more robust for schema-qualified names.
            schema, name = self._table_ref(table_name)
            try:
                inspector = reflection.Inspector.from_engine(self.engine)
                columns = list(inspector.get_columns(name, schema=schema))
            except Exception as e:
                nd_logger.debug(f"[DBHandler] get_columns({name}, schema={schema}) failed: {e}")
                table = self._reflect_table(table_name)
                columns = [
                    {"name": c.name, "type": c.type, "nullable": c.nullable}
                    for c in table.columns
                ]
        else:
            with self.engine.connect() as conn:
                columns = list(inspect(conn).get_columns(table_name))
        self._columns_cache[table_name] = columns
        return columns

    def _mssql_select_clause(self, table_name: str) -> str:
        """Build a SELECT column list for MSSQL that casts XML columns to NVARCHAR(MAX).

        FreeTDS cannot deserialize the MSSQL-native XML wire type and raises
        'xml serialization failed'.  Casting to NVARCHAR(MAX) returns the XML
        payload as a plain Unicode string that pymssql can handle.

        Falls back to '*' for non-MSSQL dialects or if column info is unavailable.
        """
        if self.engine.dialect.name != "mssql":
            return "*"
        try:
            columns = self.get_columns(table_name)
        except Exception:
            return "*"
        qi = self._qi
        parts = []
        for col in columns:
            type_name = type(col.get("type")).__name__.upper()
            if "XML" in type_name:
                parts.append(
                    f"CAST({qi(col['name'])} AS NVARCHAR(MAX)) AS {qi(col['name'])}"
                )
            else:
                parts.append(qi(col["name"]))
        return ", ".join(parts) if parts else "*"


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
        # MySQL dest: use plain table name (no schema prefix like dbo.users)
        if dest_handler.engine.dialect.name == "mysql" and "." in str(dest_table_name):
            dest_table_name = dest_table_name.split(".", 1)[-1]

        # Use get_columns (no FK reflection) to avoid MySQL dialect KeyError('TABLENAME')
        # in _correct_for_mysql_bugs_88718_96365 when Table autoload reflects FKs.
        col_defs = self.get_columns(source_table_name)
        source_is_mssql = self.engine.dialect.name == "mssql"
        dest_is_mysql = dest_handler.engine.dialect.name == "mysql"

        # Use raw CREATE TABLE for MySQL dest to avoid Table.create() which can
        # trigger MySQL dialect FK reflection and KeyError('TABLENAME').
        if dest_is_mysql:
            from sqlalchemy.dialects.mysql import VARCHAR
            col_specs = []
            for col_def in col_defs:
                col_name = col_def["name"]
                if col_name in column_type_mapping:
                    mapping = column_type_mapping[col_name]
                    new_type, col_nullable = self.get_column_type(mapping)
                    col_nullable = col_nullable if (col_nullable is not None) else col_def.get("nullable", True)
                    ddl_type = _sqlalchemy_type_to_mysql_ddl(new_type)
                else:
                    col_type = col_def.get("type")
                    col_nullable = col_def.get("nullable", True)
                    if isinstance(col_type, SAEnum):
                        ddl_type = "VARCHAR(255)"
                    elif source_is_mssql and dest_is_mysql:
                        mapped = mssql_type_to_mysql(col_type)
                        ddl_type = _sqlalchemy_type_to_mysql_ddl(mapped)
                    else:
                        ddl_type = _sqlalchemy_type_to_mysql_ddl(col_type or VARCHAR(255))
                nullable_str = "" if col_nullable else " NOT NULL"
                col_specs.append((col_name, ddl_type, nullable_str))

            col_ddl_parts = _adjust_ddl_for_mysql_row_limit(col_specs)
            create_sql = f"CREATE TABLE `{dest_table_name}` (\n  " + ",\n  ".join(col_ddl_parts) + "\n)"
            with dest_handler.engine.connect() as conn:
                conn.execute(text("SET sql_mode = ''"))
                conn.execute(text("SET innodb_strict_mode = 0"))
                conn.execute(text(create_sql))
                conn.commit()
        else:
            # Non-MySQL dest: use Table.create() (no MySQL FK reflection bug)
            mapped_columns = []
            for col_def in col_defs:
                col_name = col_def["name"]
                if col_name in column_type_mapping:
                    mapping = column_type_mapping[col_name]
                    new_type, col_nullable = self.get_column_type(mapping)
                    col_nullable = col_nullable if (col_nullable is not None) else col_def.get("nullable", True)
                    mapped_columns.append(Column(col_name, new_type, nullable=col_nullable))
                else:
                    col_type = col_def.get("type")
                    col_nullable = col_def.get("nullable", True)
                    if isinstance(col_type, SAEnum):
                        from sqlalchemy.dialects.mysql import VARCHAR as _VARCHAR
                        col_type = _VARCHAR(255)
                    elif source_is_mssql:
                        col_type = mssql_type_to_mysql(col_type)
                    else:
                        col_type = col_type or String(255)
                    mapped_columns.append(Column(col_name, col_type, nullable=col_nullable))

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
        # Schema mapping passes type classes (e.g. LONGTEXT); _sqlalchemy_type_to_mysql_ddl
        # needs instances. Instantiate so LONGTEXT → "LONGTEXT" not "VARCHAR(255)".
        if isinstance(col_type, type):
            try:
                col_type = col_type()
            except TypeError:
                pass
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
            # MySQL dest: use plain name and backticks
            if dest_handler.engine.dialect.name == "mysql" and "." in table_name:
                table_name = table_name.split(".", 1)[-1]
            quoted = f"`{table_name}`" if dest_handler.engine.dialect.name == "mysql" else table_name
            with dest_handler.engine.connect() as conn:
                conn.execute(text(f"SELECT 1 FROM {quoted} LIMIT 1"))
            return True
        except Exception:
            return False


    def get_all_tables(self) -> list[str]:
        inspector = reflection.Inspector.from_engine(self.engine)
        if self._is_mssql:
            # MSSQL: enumerate all schemas and return schema.table so callers can
            # later pass the full name to _reflect_table / _table_ref correctly.
            tables = []
            for schema in inspector.get_schema_names():
                if schema in ("sys", "INFORMATION_SCHEMA", "guest"):
                    continue
                for name in inspector.get_table_names(schema=schema):
                    tables.append(f"{schema}.{name}")
            return tables
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

        # ── 1. Schema lookup (cached per table to avoid repeated DB round-trips) ─
        cached = self._insert_schema_cache.get(table_name)
        if cached is not None:
            valid_columns, max_lengths, numeric_columns = cached
        else:
            col_defs = self.get_columns(table_name)
            valid_columns = [c["name"] for c in col_defs]

            # Detect numeric/integer columns so we can convert empty strings → NULL
            # and avoid MySQL 1366 "Incorrect integer value ''" errors.
            _NUMERIC_TYPE_NAMES = (
                "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
                "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "REAL", "BIT",
            )
            numeric_columns: set[str] = set()
            for col_def in col_defs:
                col_type = col_def.get("type")
                if col_type is not None:
                    type_name = type(col_type).__name__.upper()
                    if any(n in type_name for n in _NUMERIC_TYPE_NAMES):
                        numeric_columns.add(col_def["name"])

            # Build {col_name: max_len} for VARCHAR/CHAR columns so we can truncate
            # before insert and avoid MySQL 1265 "Data truncated" errors.
            max_lengths: dict[str, int] = {}
            for col_def in col_defs:
                col_name = col_def["name"]
                col_type = col_def.get("type")
                length = None
                if col_type is not None:
                    length = getattr(col_type, "length", None)
                    # Some SQLAlchemy dialect wrappers nest the real type one level down.
                    if length is None and hasattr(col_type, "type"):
                        length = getattr(col_type.type, "length", None)
                if length and length > 0:
                    max_lengths[col_name] = int(length)

            # MySQL fallback: information_schema.COLUMNS (reflection sometimes misses lengths)
            if self.engine.dialect.name == "mysql":
                try:
                    db_name = self.engine.url.database
                    if db_name:
                        r = self.session.execute(
                            text(
                                "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH FROM "
                                "information_schema.COLUMNS "
                                "WHERE TABLE_SCHEMA = :dbname AND TABLE_NAME = :tname "
                                "AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL"
                            ),
                            {"dbname": db_name, "tname": table_name},
                        )
                    else:
                        r = self.session.execute(
                            text(
                                "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH FROM "
                                "information_schema.COLUMNS "
                                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tname "
                                "AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL"
                            ),
                            {"tname": table_name},
                        )
                    for row in r:
                        cname, max_len = row[0], row[1]
                        if cname not in max_lengths and max_len and max_len > 0:
                            max_lengths[cname] = int(max_len)
                except Exception as e:
                    nd_logger.debug(f"[DBHandler] information_schema fallback: {e}")

                # SHOW COLUMNS fallback: parses varchar(n) from the Type string
                missing = [c for c in valid_columns if c not in max_lengths]
                if missing:
                    try:
                        r = self.session.execute(text(f"SHOW COLUMNS FROM `{table_name}`"))
                        for row in r:
                            cname, col_type_str = row[0], str(row[1] or "")
                            if cname not in max_lengths:
                                m = re.search(r"char\s*\(\s*(\d+)\s*\)", col_type_str, re.I)
                                if m:
                                    max_lengths[cname] = int(m.group(1))
                    except Exception as e:
                        nd_logger.debug(f"[DBHandler] SHOW COLUMNS fallback: {e}")

            self._insert_schema_cache[table_name] = (valid_columns, max_lengths, numeric_columns)

        select_cols = [c for c in valid_columns if c in df.columns]
        df = df.select(select_cols)

        # ── 2. Truncate columns that exceed their declared max-length ─────────
        # Only columns in max_lengths have a declared varchar length. We cast+slice
        # unconditionally — Polars handles non-string types via cast(Utf8) safely,
        # and this avoids a per-column dtype lookup.
        if max_lengths:
            truncate_exprs = []
            for col_name, max_len in max_lengths.items():
                if col_name not in df.columns:
                    continue
                try:
                    truncate_exprs.append(
                        pl.col(col_name)
                        .cast(pl.Utf8)
                        .str.slice(0, max_len)
                        .alias(col_name)
                    )
                except Exception as e:
                    nd_logger.warning(f"[DBHandler] Could not add truncation for '{col_name}': {e}")
            if truncate_exprs:
                df = df.with_columns(truncate_exprs)
                nd_logger.info(
                    f"[DBHandler] Truncated {len(truncate_exprs)} column(s) to schema "
                    f"max lengths for '{table_name}'."
                )

        # ── 3. Convert empty strings → NULL for numeric columns ─────────────
        # MySQL strict mode rejects '' into INT/DECIMAL columns (error 1366).
        if numeric_columns:
            nullify_exprs = []
            for col_name in numeric_columns:
                if col_name not in df.columns:
                    continue
                if df[col_name].dtype == pl.Utf8:
                    nullify_exprs.append(
                        pl.when(pl.col(col_name).str.strip_chars() == "")
                        .then(None)
                        .otherwise(pl.col(col_name))
                        .alias(col_name)
                    )
            if nullify_exprs:
                df = df.with_columns(nullify_exprs)

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
        """Yield distinct non-NULL, non-empty values of a single column from *table_name*."""
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(
            f"SELECT DISTINCT {qi(column_name)} FROM {qi(table_name)}{nolock} "
            f"WHERE {qi(column_name)} IS NOT NULL AND {qi(column_name)} != ''"
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
                    val = str(row[0]).strip()
                    if val:
                        yield val

    def fetch_distinct_pairs(self, table_name: str, col_a: str, col_b: str, batch_size: int = 10000) -> Iterator[tuple[str, str]]:
        """Yield distinct non-NULL, non-empty (col_a, col_b) pairs from *table_name*."""
        qi = self._qi
        nolock = " WITH (NOLOCK)" if self.engine.dialect.name == "mssql" else ""
        query = text(
            f"SELECT DISTINCT {qi(col_a)}, {qi(col_b)} FROM {qi(table_name)}{nolock} "
            f"WHERE {qi(col_a)} IS NOT NULL AND {qi(col_b)} IS NOT NULL "
            f"AND {qi(col_a)} != '' AND {qi(col_b)} != ''"
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
                    a, b = str(row[0]).strip(), str(row[1]).strip()
                    if a and b:
                        yield (a, b)

