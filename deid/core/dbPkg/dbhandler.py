from sqlalchemy import create_engine, event, MetaData, Table, text, func, Index
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from deid.core.logger import nd_logger
from sqlalchemy import Table, Column, text, create_engine, MetaData, VARCHAR, INTEGER, BIGINT
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import (
    BigInteger,
    Integer,
    String
)
import datetime
import decimal
import pandas as pd
import polars as pl
from typing import Iterator, List, Dict, Union


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


def _normalize_rows(rows) -> list:
    """Apply _normalize_value to every cell in every row."""
    return [[_normalize_value(cell) for cell in row] for row in rows]


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


class NDDBHandler:
    def __init__(self, connection_string: str, read_only: bool = False):
        self.read_only = read_only
        engine_kwargs = dict(pool_size=100, max_overflow=10, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True)
        if read_only:
            self.engine = create_read_only_engine(connection_string, **engine_kwargs)
        else:
            self.engine = create_engine(connection_string, **engine_kwargs)

        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
    
    def close(self):
        self.session.close()
        self.engine.dispose()

    def get_columns(self, table_name: str) -> list[dict]:
        inspector = reflection.Inspector.from_engine(self.engine)
        return inspector.get_columns(table_name)

    def get_column_names(self, table_name: str) -> list[str]:
        return [column["name"] for column in self.get_columns(table_name)]

    def fetch_all(self, table_name: str) -> list[dict]:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = table.select()
        result = self.session.execute(query)
        return [dict(row) for row in result]

    # def insert_to_db(self, rows: list[dict], table_name: str):
    #     if not rows:
    #         nd_logger.warning(f"No rows to insert into {table_name}.")
    #         return
    #     table = Table(table_name, self.metadata, autoload_with=self.engine)
    #     self.session.execute(table.insert(), rows)
    #     self.session.commit()
    def _assert_writable(self, operation: str):
        if self.read_only:
            raise RuntimeError(
                f"Refusing to {operation}: this NDDBHandler is read-only (source database). "
                "Write operations must target the destination database."
            )

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
            # sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            sql = f"INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in columns)}) VALUES ({placeholders})"

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
                mapped_columns.append(
                    Column(col_name, column.type, nullable=column.nullable)
                )

        dest_table = Table(dest_table_name, dest_handler.metadata, *mapped_columns)
        # # Fix for missing Unique Indexes
        # for idx in source_table.indexes:
        #     # Map source index columns to the new destination table columns
        #     column_names = [c.name for c in idx.columns]
        #     target_columns = [dest_table.c[name] for name in column_names]
            
        #     # Re-create the index on the destination table
        #     Index(idx.name, *target_columns, unique=idx.unique)

        dest_table.create(dest_handler.engine)
        nd_logger.info(
            f"Table {dest_table_name} created in destination database with modified schema."
        )

    # def _get_sqlalchemy_type(self, type_name: str, length: int = None):
    #     type_map = {
    #         "VARCHAR": lambda l: VARCHAR(length=l) if l else VARCHAR,
    #         "INTEGER": INTEGER,
    #         "BIGINT": BIGINT
    #     }
    #     return type_map[type_name](length) if length else type_map[type_name]

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

    def get_all_tables(self) -> list[str]:
        inspector = reflection.Inspector.from_engine(self.engine)
        return inspector.get_table_names()

    def get_table_schema(self, table_name: str) -> list[dict]:
        inspector = reflection.Inspector.from_engine(self.engine)
        return inspector.get_columns(table_name)

    def get_rows_count(self, table_name: str) -> int:
        # For MySQL: use information_schema.TABLES which is near-instant (no full scan).
        # information_schema.TABLE_ROWS is an estimate maintained by InnoDB; accurate
        # enough for stats display and batch planning. Falls back to COUNT(*) for other DBs.
        if self.engine.dialect.name == "mysql":
            query = text(
                "SELECT TABLE_ROWS FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
            )
            result = self.session.execute(query, {"table_name": table_name})
            count = result.scalar()
            if count is not None:
                return int(count)
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = func.count().select().select_from(table)
        result = self.session.execute(query)
        return result.scalar()

    def get_table_size(self, table_name: str) -> str:
        return "1 GB"
        if self.engine.dialect.name == "mysql":
            query = text(
                f"SELECT (data_length + index_length) FROM information_schema.tables WHERE table_name = '{table_name}'"
            )
        else:
            query = text(f"SELECT pg_total_relation_size('{table_name}')")
        result = self.session.execute(query)
        size_in_bytes = result.scalar() or 0

        for unit in ["Bytes", "KB", "MB", "GB", "TB"]:
            if size_in_bytes < 1024:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024

    def get_db_size(self) -> str:
        return "1 GB"
        if self.engine.dialect.name == "mysql":
            query = text(
                "SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = DATABASE()"
            )
        else:
            query = text("SELECT pg_database_size(current_database())")
        result = self.session.execute(query)
        size_in_bytes = result.scalar() or 0

        for unit in ["Bytes", "KB", "MB", "GB", "TB"]:
            if size_in_bytes < 1024:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024

    def get_rows(self, table_name: str, limit: int, offset: Union[int, Dict[str, int]]) -> List[dict]:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        
        # Use primary key for offset-based pagination
        primary_key_cols = list(table.primary_key.columns)
        order_column = primary_key_cols[0] if primary_key_cols else list(table.columns)[0]
        order_col_name = order_column.name

        # Keyset Pagination using 'nd_auto_increment_id'
        if isinstance(offset, dict) and "gt" in offset and "lt" in offset:
            if "nd_auto_increment_id" not in table.c:
                raise ValueError(f"Table '{table_name}' does not have column 'nd_auto_increment_id' required for keyset pagination.")

            query = (
                table.select()
                .where(table.c.nd_auto_increment_id >= offset["gt"])
                .where(table.c.nd_auto_increment_id <= offset["lt"])
            )

        # Offset-based Pagination (e.g., limit-offset)
        elif isinstance(offset, int):
            query = (
                table.select()
                .order_by(table.c[order_col_name])
                .offset(offset)
                .limit(limit)
            )

        else:
            raise ValueError("Offset must be either an int or a dict with 'gt' and 'lt' keys.")

        result = self.session.execute(query)
        return [dict(row._mapping) for row in result]
    
    def get_all_rows(self, table_name: str) -> list[dict]:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = table.select()
        result = self.session.execute(query)
        return [dict(row._mapping) for row in result]
    
    def get_rows_where_column_values_in(self, table_name: str, column_name: str, column_values: list[str]) -> list[dict]:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
    
        # Build the query with WHERE condition
        query = table.select().where(table.c[column_name].in_(column_values))

        # Execute query
        result = self.session.execute(query)

        # Convert result to list of dictionaries
        return [dict(row._mapping) for row in result]

    def table_with_max_rows(self) -> dict[str, str]:
        tables = self.get_all_tables()
        max_rows, max_table = -1, None
        for table in tables:
            row_count = self.get_rows_count(table)
            if row_count > max_rows:
                max_rows, max_table = row_count, table
        return {"table_name": max_table, "rows_count": max_rows}

    def table_with_min_rows(self) -> dict[str, str]:
        tables = self.get_all_tables()
        min_rows, min_table = float("inf"), None
        for table in tables:
            row_count = self.get_rows_count(table)
            if row_count < min_rows:
                min_rows, min_table = row_count, table
        return {"table_name": min_table, "rows_count": min_rows}

    def table_with_max_size(self) -> str:
        tables = self.get_all_tables()
        max_size, max_table = -1, None
        for table in tables:
            table_size_str = self.get_table_size(table)
            table_size = self._parse_size_to_bytes(table_size_str)
            if table_size > max_size:
                max_size, max_table = table_size, table
        return {"table_name": max_table, "size": max_size}

    def table_with_min_size(self) -> str:
        tables = self.get_all_tables()
        min_size, min_table = float("inf"), None
        for table in tables:
            table_size_str = self.get_table_size(table)
            table_size = self._parse_size_to_bytes(table_size_str)
            if table_size < min_size:
                min_size, min_table = table_size, table
        return {"table_name": min_table, "size": min_size}

    def _parse_size_to_bytes(self, size_str: str) -> int:
        units = {"Bytes": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        size, unit = size_str.split()
        return int(float(size) * units[unit])

    def fks_to_for_table(self, table_name: str) -> list[dict]:
        inspector = reflection.Inspector.from_engine(self.engine)
        foreign_keys = []
        for fk in inspector.get_foreign_keys(table_name):
            foreign_keys.append(
                {
                    "constrained_columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"],
                }
            )
        return foreign_keys

    def fks_from_for_table(self, table_name: str) -> list[dict]:
        # Legacy single-table method — O(N) per call (N = number of tables).
        # For bulk stats generation use get_all_fks_map() once instead.
        inspector = reflection.Inspector.from_engine(self.engine)
        foreign_keys = []
        for table in self.get_all_tables():
            for fk in inspector.get_foreign_keys(table):
                if fk["referred_table"] == table_name:
                    foreign_keys.append(
                        {
                            "table": table,
                            "constrained_columns": fk["constrained_columns"],
                            "referred_columns": fk["referred_columns"],
                        }
                    )
        return foreign_keys

    def get_all_fks_map(self) -> tuple[dict, dict]:
        """Build complete FK graph for all tables in a single pass.

        Returns:
            fks_to_map:   {table_name → list of outgoing FK dicts}
            fks_from_map: {referred_table_name → list of incoming FK dicts}

        Replaces N calls to fks_from_for_table() (O(N²) total) with a single
        O(N) loop — one inspector.get_foreign_keys() call per table.
        """
        inspector = reflection.Inspector.from_engine(self.engine)
        all_tables = self.get_all_tables()
        fks_to_map: dict[str, list] = {t: [] for t in all_tables}
        fks_from_map: dict[str, list] = {t: [] for t in all_tables}

        for table in all_tables:
            for fk in inspector.get_foreign_keys(table):
                fks_to_map[table].append(
                    {
                        "constrained_columns": fk["constrained_columns"],
                        "referred_table": fk["referred_table"],
                        "referred_columns": fk["referred_columns"],
                    }
                )
                referred = fk["referred_table"]
                if referred in fks_from_map:
                    fks_from_map[referred].append(
                        {
                            "table": table,
                            "constrained_columns": fk["constrained_columns"],
                            "referred_columns": fk["referred_columns"],
                        }
                    )
        return fks_to_map, fks_from_map

    def get_keyset_pagination_ranges(self, table_name: str, id_column: str = "nd_auto_increment_id", batch_size: int = 100000) -> List[Dict[str, int]]:
        # Fast O(1) approach: MIN and MAX are pure index lookups on an auto-increment column.
        # The old ROW_NUMBER() OVER () window function was O(N) — it materialized and sorted
        # the entire table, taking 60–300s on 20–50M row tables.
        # Since nd_auto_increment_id is an auto-increment column, ID distribution is dense
        # and uniform, so MIN/MAX splitting gives batches close to batch_size rows.
        min_max_query = text(
            f"SELECT MIN(`{id_column}`), MAX(`{id_column}`) "
            f"FROM `{table_name}` WHERE `{id_column}` IS NOT NULL"
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
        query = text(
            f"SELECT * FROM `{table_name}` "
            f"WHERE `{id_column}` BETWEEN :start_id AND :end_id"
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
        query = text(f"SELECT * FROM `{table_name}`")
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

    def get_table_as_dataframe(self, table_name: str, limit: int, offset: Union[int, dict]) -> pl.DataFrame:
        """Fetch a slice of a table as a Polars DataFrame (legacy offset/keyset API).

        Prefer `stream_table_as_dataframes()` for full-table processing.
        """
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        primary_key_cols = list(table.primary_key.columns)
        order_column = primary_key_cols[0] if primary_key_cols else list(table.columns)[0]

        # Keyset pagination
        if isinstance(offset, dict) and "gt" in offset and "lt" in offset:
            gt_value = offset["gt"]
            lt_value = offset["lt"]
            query = (table.select().where(table.c["nd_auto_increment_id"] >= gt_value).where(table.c["nd_auto_increment_id"] <= lt_value))
        else:
            # Offset-based pagination
            query = (table.select().order_by(order_column).limit(limit).offset(offset))

        result = self.session.execute(query)
        rows = result.fetchall()
        columns = list(result.keys())
        return pl.DataFrame(
            [list(r) for r in rows],
            schema=columns,
            orient="row",
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
        # This is a last-resort safety net.  Ideally the destination table was
        # already created with the correct (wider) VARCHAR length via
        # _get_columns_schema_mapping in main.py.  But if the table already
        # existed from a previous run with the original narrow source schema,
        # this prevents MySQL error 1265 "Data truncated for column …".
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