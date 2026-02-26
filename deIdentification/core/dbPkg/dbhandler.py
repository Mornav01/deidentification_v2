from sqlalchemy import create_engine, MetaData, Table, text, func, Index
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from deIdentification.nd_logger import nd_logger
from sqlalchemy import Table, Column, text, create_engine, MetaData, VARCHAR, INTEGER, BIGINT
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import (
    BigInteger,
    Integer,
    String
)
from .mssql import create_table as create_table_from_mssql
import pandas as pd
from typing import List, Dict, Union


class NDDBHandler:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string, pool_size=100, max_overflow=10, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True)
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
    def insert_to_db(self, rows: list[dict], table_name: str, batch_size: int = 10000):
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
        dest_table_name = dest_table_name or source_table_name
        
        if self.engine.dialect.name == "mssql":
            create_table_from_mssql(dest_table_name, self.engine, dest_handler.engine, column_type_mapping)
            return
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

    def drop_table(self, table_name: str):
        if self._table_exists(self, table_name):
            self.session.execute(text(f"DROP TABLE {table_name}"))
            self.session.commit()
            nd_logger.info(f"Table {table_name} dropped from the database.")
        else:
            nd_logger.warning(
                f"Table {table_name} does not exist and cannot be dropped."
            )
    
    def get_keyset_pagination_ranges(self, table_name: str, id_column: str = "nd_auto_increment_id", batch_size: int = 100000) -> List[Dict[str, int]]:
        dialect_name = self.engine.dialect.name

        if dialect_name == "mssql":
            query = text(f"""
                WITH ranked AS (SELECT {id_column}, ROW_NUMBER() OVER (ORDER BY {id_column}) AS rn
                                FROM {table_name}
                                WHERE {id_column} IS NOT NULL ),
                    ranked_batches AS ( SELECT {id_column}, ((rn - 1) / :batch_size) AS batch_num
                                        FROM ranked )
                    SELECT MIN({id_column}) AS gt, MAX({id_column}) AS lt
                    FROM ranked_batches
                    GROUP BY batch_num
                    ORDER BY MIN({id_column});
                """)
        elif dialect_name == "mysql":
            query = text(f"""
                WITH ranked AS (SELECT {id_column}, ROW_NUMBER() OVER (ORDER BY {id_column}) AS rn
                                FROM {table_name}
                                WHERE {id_column} IS NOT NULL ),
                    ranked_batches AS ( SELECT {id_column}, FLOOR((rn - 1) / :batch_size) AS batch_num
                                    FROM ranked )
                    SELECT MIN({id_column}) AS gt, MAX({id_column}) AS lt
                    FROM ranked_batches
                    GROUP BY batch_num
                    ORDER BY MIN({id_column});
                """)
        else:
            raise NotImplementedError(f"Unsupported database dialect: {dialect_name}")

        with self.engine.connect() as conn:
            try:
                result = conn.execute(query, {"batch_size": batch_size})
            except:
                return None
            return [{"gt": row[0], "lt": row[1]} for row in result]


    def get_table_as_dataframe(self, table_name: str, limit: int, offset: Union[int, dict]) -> pd.DataFrame:
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
        return pd.DataFrame(result.fetchall(), columns=result.keys())
    

    def insert_dataframe_in_batches(self, df: pd.DataFrame, table_name: str, batch_size: int = 10000, sanitize: bool = True) -> None:
        """
        Insert a large DataFrame into the given MySQL table in batches.

        Parameters:
        - df: DataFrame to insert
        - table_name: Target MySQL table
        - batch_size: Number of rows per batch
        - sanitize: Replace NaN/NaT/inf with None if True
        """
        if df.empty:
            nd_logger.warning(f"[DBHandler] Empty DataFrame. Nothing to insert into '{table_name}'.")
            return

        valid_columns = self.get_column_names(table_name)
        df = df[valid_columns]  # Keep only valid columns
        
        if sanitize:
            df = df.astype(object).where(pd.notnull(df), None)

        total_rows = len(df)
        nd_logger.info(f"[DBHandler] Starting insertion of {total_rows} rows into '{table_name}' in batches of {batch_size}.")

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_df = df.iloc[start:end]

            try:
                rows = batch_df.to_dict(orient="records")
                self.insert_to_db(rows, table_name)
                nd_logger.info(f"[DBHandler] Inserted rows {start + 1} to {end} into '{table_name}'.")
            except Exception as e:
                nd_logger.error(f"[DBHandler] Failed to insert batch {start + 1} to {end}: {e}")
                raise

        nd_logger.info(f"[DBHandler] Completed insertion into '{table_name}'.")