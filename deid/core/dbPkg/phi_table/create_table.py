from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, select, update, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Integer, Float, String, Date, DateTime, Text, BigInteger, SmallInteger, DECIMAL, Boolean
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TEXT, FLOAT, DATE, DATETIME
from sqlalchemy.inspection import inspect
from typing import TypedDict
from deid.core.logger import nd_logger
from typing import Optional
from sqlalchemy.dialects.mysql import insert as mysql_insert
from deid.core.dbPkg.dbhandler import create_read_only_engine

class OnePHITableConfig(TypedDict):
    primary_col: str
    other_required_columns: list[str]


class PIITableConfig(TypedDict):
    primary_column_name: Optional[str] = None
    upsert_instead_of_append: bool
    tables: dict[str, OnePHITableConfig]


class PIITable:
    def __init__(self, src_db_url, dest_db_url, pii_tables_config: dict[str, PIITableConfig]):
        """Initialize the PII Data Manager with source and destination database URLs."""
        self.src_db_url = src_db_url
        self.dest_db_url = dest_db_url
        self.src_engine = create_read_only_engine(src_db_url)
        self.dest_engine = create_engine(dest_db_url)
        self.metadata = MetaData()

        self.pii_tables_config = pii_tables_config
        
        
    def get_column_type(self, col_type):
        # Type mapping for column types
        type_mapping = {
            'INTEGER': Integer,
            'SMALLINT': SmallInteger,
            'TEXT': Text,
            'FLOAT': Float,
            'DATE': Date,
            'DATETIME': DateTime,
            'SMALLDATETIME': DateTime,
            'DATETIME2': DateTime,
            'BIGINT': BigInteger,
            'BIT': Boolean,
            'MONEY': DECIMAL(19, 4),
            'CHAR': String,
            'NCHAR': String,
        }
        db_dialect = self.dest_engine.dialect.name
        if db_dialect == "mssql":
            return col_type
        if isinstance(col_type, String):
            length = col_type.length
            return VARCHAR(length) if length else VARCHAR(255)
        elif isinstance(col_type, DECIMAL):
            precision = col_type.precision if col_type.precision else 10
            scale = col_type.scale if col_type.scale else 2
            return DECIMAL(precision, scale)
        elif isinstance(col_type, Integer):
            return Integer
        elif isinstance(col_type, Date):
            return Date
        elif isinstance(col_type, Float):
            return Float
        else:
            return type_mapping.get(str(col_type), String)
    
    def create_table(self, pii_table_name : str, pii_table_config: PIITableConfig):
        with self.src_engine.connect() as src_conn, self.dest_engine.connect() as dest_conn:
            inspector = inspect(src_conn)
            existing_tables = inspector.get_table_names()
            nd_logger.info(f"Existing tables in source DB: {len(existing_tables)}")

            columns = []

            if pii_table_config["primary_column_name"]:
                columns.append(Column(pii_table_config['primary_column_name'], Integer, primary_key=True, nullable=False))
            for table, conf in pii_table_config.get("tables", {}).items():
                if table not in existing_tables:
                    message = f"Table {table} does not exist in the source database..."
                    #nd_logger.error(message)
                    raise Exception(message)

                for colconf in inspector.get_columns(table):
                    if colconf['name'] in conf.get("other_required_columns", []):
                        column_name = f"{table}_{colconf['name']}"
                        other_db_type = self.get_column_type(colconf['type'])
                        columns.append(Column(column_name, other_db_type, nullable=True))
                        #nd_logger.info(f"Added column: {column_name} of type {other_db_type}")

            if len(columns) == 1:
                message = "No valid columns found. Skipping table creation."
                #nd_logger.error(message)
                raise Exception(message)

            pii_table = Table(pii_table_name, self.metadata, *columns, extend_existing=True)
            self.metadata.create_all(self.dest_engine)
            #nd_logger.info(f"Table {pii_table_name} created successfully in the destination database!")


    # def _insert_data_to_pii_table(self, pii_table_name: str, source_table: str, pii_table_config: PIITableConfig):
    #     pii_table = Table(pii_table_name, self.metadata, autoload_with=self.dest_engine)
    #     source_table_conf = pii_table_config["tables"][source_table]
    #     pii_primary_column = pii_table_config["primary_column_name"]
    #     source_primary_column = source_table_conf["primary_col"]
    #     required_columns = source_table_conf["other_required_columns"]
        
    #     upsert_instead_of_append = self.pii_tables_config[pii_table_name]["upsert_instead_of_append"]

    #     columns = [source_primary_column] + required_columns if pii_primary_column else required_columns
    #     query = f"SELECT {', '.join(columns)} FROM {source_table}"

    #     with self.src_engine.connect() as src_conn:
    #         result = src_conn.execute(text(query)).fetchall()

    #     insert_data = []
    #     for row in result:
    #         data_row = {}
    #         if pii_primary_column:
    #             data_row[pii_primary_column] = row[0]

    #         for idx, column in enumerate(required_columns):
    #             data_row[f"{source_table}_{column}"] = row[idx + (1 if pii_primary_column else 0)]

    #         insert_data.append(data_row)

    #     with self.dest_engine.begin() as dest_conn:
    #         for data_row in insert_data:
    #             try:
    #                 if pii_primary_column:
    #                     select_stmt = select(pii_table.c[pii_primary_column]).where(
    #                         pii_table.c[pii_primary_column] == data_row[pii_primary_column]
    #                     )
    #                     result = dest_conn.execute(select_stmt).fetchone()

    #                     update_data = {k: v for k, v in data_row.items() if k != pii_primary_column}

    #                     if result:
    #                         update_stmt = update(pii_table).where(
    #                             pii_table.c[pii_primary_column] == data_row[pii_primary_column]
    #                         ).values(**update_data)
    #                         dest_conn.execute(update_stmt)
    #                         nd_logger.info(f"Updated {pii_primary_column} {data_row[pii_primary_column]} in {pii_table_name}.")
    #                     else:
    #                         insert_stmt = pii_table.insert().values(**data_row)
    #                         dest_conn.execute(insert_stmt)
    #                         nd_logger.info(f"Inserted {pii_primary_column} {data_row[pii_primary_column]} into {pii_table_name}.")

    #                 else:
    #                     insert_stmt = pii_table.insert().values(**data_row)
    #                     dest_conn.execute(insert_stmt)
    #                     nd_logger.info(f"Inserted data into {pii_table_name}.")

    #             except IntegrityError as e:
    #                 nd_logger.info(f"Error occurred while inserting/updating {pii_primary_column}: {e}")

    #     nd_logger.info(f"Data from table {pii_table_name} successfully inserted/updated.")

    def _insert_data_to_pii_table(self, pii_table_name: str, source_table: str, pii_table_config: PIITableConfig):
        pii_table = Table(pii_table_name, self.metadata, autoload_with=self.dest_engine)
        source_table_conf = pii_table_config["tables"][source_table]
        pii_primary_column = pii_table_config["primary_column_name"]
        source_primary_column = source_table_conf["primary_col"]
        required_columns = source_table_conf["other_required_columns"]

        columns = [source_primary_column] + required_columns if pii_primary_column else required_columns
        query = f"SELECT {', '.join(columns)} FROM {source_table}"

        with self.src_engine.connect() as src_conn:
            result = src_conn.execute(text(query)).fetchall()

        insert_data = []
        for row in result:
            data_row = {}
            if pii_primary_column:
                data_row[pii_primary_column] = row[0]

            for idx, column in enumerate(required_columns):
                value = row[idx + (1 if pii_primary_column else 0)]
                # Convert '0000-00-00' to None for MSSQL compatibility
                if isinstance(value, str) and value == "0000-00-00":
                    value = None
                data_row[f"{source_table}_{column}"] = value

            insert_data.append(data_row)

        #print(len(insert_data),insert_data[0:5])

        batch_size = 10000  # Batch size for insertion
        total_batches = (len(insert_data) + batch_size - 1) // batch_size

        for i in range(0, len(insert_data), batch_size):
            batch_index = i // batch_size + 1
            batch = insert_data[i:i + batch_size]
            if batch:
                with self.dest_engine.begin() as dest_conn:
                    try:
                        insert_stmt = mysql_insert(pii_table).values(batch)
                        update_stmt = insert_stmt.on_duplicate_key_update({
                            key: insert_stmt.inserted[key] for key in batch[0] if key != pii_primary_column
                        })
                        dest_conn.execute(update_stmt)
                        
                        
                        nd_logger.info(f"Inserted batch {batch_index} of {total_batches} for {pii_table_name}.")

                    except IntegrityError as e:
                        nd_logger.error(f"Error occurred during batch insert/upsert: {e}")

        nd_logger.info(f"Data from table {pii_table_name} successfully inserted/updated.")
    
    def generate_pii_tables(self):
        for pii_table_name, table_config in self.pii_tables_config.items():
            self.create_table(pii_table_name, table_config)
            for source_table, conf in table_config.get("tables", {}).items():
                self._insert_data_to_pii_table(pii_table_name, source_table, table_config)



# pii_tables_config = {
#     "facility": {
#         "primary_column_name": "patient_id",
#         "upsert_instead_of_append": True, # only if primary_column is None
#         "tables": {
#             "edi_facility": {
#                 "primary_col": "uid",
#                 "other_required_columns": ["employername", "employeraddress"]
#             },
#             "hcfa": {
#                 "primary_col": "patient_id",
#                 "other_required_columns": ["employername", "employeraddress"]
#             },
#         }
#     }
# }