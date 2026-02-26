# from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, select, update, text
# from sqlalchemy.exc import IntegrityError
# from sqlalchemy import Integer, Float, String, Date, DateTime, Text, BigInteger, SmallInteger, DECIMAL, Boolean
# from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TEXT, FLOAT, DATE, DATETIME
# from sqlalchemy.inspection import inspect
# from typing import TypedDict
# from deIdentification.nd_logger import nd_logger


# class OnePHITableConfig(TypedDict):
#     patient_id_column: str
#     other_required_columns: list[str]

# class PIITableConfig(TypedDict):
#     table_details: dict[str, OnePHITableConfig]

# class PIIConst:
#     PATIENT_ID_COL = "patient_id" # do not change, otherwise you need to update the queries as well.
#     PII_TABLE_NAME = "patient_pii_data"

# class PIITable:
#     def __init__(self, src_db_url, dest_db_url, pii_table_config: PIITableConfig):
#         """Initialize the PII Data Manager with source and destination database URLs."""
#         self.src_db_url = src_db_url
#         self.dest_db_url = dest_db_url
#         self.src_engine = create_engine(src_db_url)
#         self.dest_engine = create_engine(dest_db_url)
#         self.metadata = MetaData()

#         self.pii_table_config = pii_table_config
        
#         # Type mapping for column types
#         self.type_mapping = {
#             'INTEGER': Integer,
#             'SMALLINT': SmallInteger,
#             'TEXT': Text,
#             'FLOAT': Float,
#             'DATE': Date,
#             'DATETIME': DateTime,
#             'SMALLDATETIME': DateTime,
#             'DATETIME2': DateTime,
#             'BIGINT': BigInteger,
#             'BIT': Boolean,
#             'MONEY': DECIMAL(19, 4),
#             'CHAR': String,
#             'NCHAR': String,
#         }

#     def get_column_type(self, col_type):
#         """Determine the appropriate SQLAlchemy column type."""
#         db_dialect = self.dest_engine.dialect.name
#         if db_dialect == "mssql":
#             return col_type
#         if isinstance(col_type, String):
#             length = col_type.length
#             return VARCHAR(length) if length else VARCHAR(255)
#         elif isinstance(col_type, DECIMAL):
#             precision = col_type.precision if col_type.precision else 10
#             scale = col_type.scale if col_type.scale else 2
#             return DECIMAL(precision, scale)
#         elif isinstance(col_type, Integer):
#             return Integer
#         elif isinstance(col_type, Date):
#             return Date
#         elif isinstance(col_type, Float):
#             return Float
#         else:
#             return self.type_mapping.get(str(col_type), String)

#     def create_master_table(self):
#         """Create the master PII data table in the destination database."""
#         with self.src_engine.connect() as src_conn, self.dest_engine.connect() as dest_conn:
#             inspector = inspect(src_conn)
#             existing_tables = inspector.get_table_names()
#             nd_logger.info(f"Existing tables in source DB: {existing_tables}")

#             columns = []

#             # Add patient_id as primary key
#             columns.append(Column(PIIConst.PATIENT_ID_COL, Integer, primary_key=True, nullable=False))

#             for table, conf in self.pii_table_config.get("table_details", {}).items():
#                 if table not in existing_tables:
#                     nd_logger.warning(f"Table {table} does not exist in the source database. Skipping...")
#                     continue

#                 for colconf in inspector.get_columns(table):
#                     if colconf['name'] in conf.get("other_required_columns", []):
#                         column_name = f"{table}_{colconf['name']}"
#                         mysql_type = self.get_column_type(colconf['type'])
#                         columns.append(Column(column_name, mysql_type, nullable=True))
#                         nd_logger.info(f"Added column: {column_name} of type {mysql_type}")

#             if len(columns) == 1:
#                 nd_logger.warning("No valid columns found. Skipping table creation.")
#                 return

#             patient_pii_data = Table(PIIConst.PII_TABLE_NAME, self.metadata, *columns, extend_existing=True)

#             # Ensure metadata is associated with the correct engine
#             self.metadata.create_all(self.dest_engine)  # ✅ This ensures table creation
#             nd_logger.info(f"Table {PIIConst.PII_TABLE_NAME} created successfully in the destination database!")


#     def _insert_data_to_pii_table(self, table, patient_id_col, required_columns):
#         pii_data_table = Table(PIIConst.PII_TABLE_NAME, self.metadata, autoload_with=self.dest_engine)
        
#         if not patient_id_col:
#             nd_logger.info(f"Patient ID column for table {table} not found in pid_cols mapping.")
#             return

#         columns = [patient_id_col] + required_columns
#         query = f"SELECT {', '.join(columns)} FROM {table}"

#         with self.src_engine.connect() as src_conn:
#             result = src_conn.execute(text(query)).fetchall()

#         insert_data = []
#         for row in result:
#             patient_id = row[0]
#             data_row = {PIIConst.PATIENT_ID_COL: patient_id}
            
#             for idx, column in enumerate(required_columns):
#                 data_row[f"{table}_{column}"] = row[idx + 1]
            
#             insert_data.append(data_row)

#         with self.dest_engine.begin() as dest_conn:
#             for data_row in insert_data:
#                 try:
#                     select_stmt = select(pii_data_table.c.patient_id).where(
#                         pii_data_table.c.patient_id == data_row[PIIConst.PATIENT_ID_COL]
#                     )
#                     result = dest_conn.execute(select_stmt).fetchone()

#                     update_data = {k: v for k, v in data_row.items() if k != PIIConst.PATIENT_ID_COL}

#                     if result:
#                         update_stmt = update(pii_data_table).where(
#                             pii_data_table.c.patient_id == data_row[PIIConst.PATIENT_ID_COL]
#                         ).values(**update_data)
#                         dest_conn.execute(update_stmt)
#                         nd_logger.info(f"Updated patient {data_row[PIIConst.PATIENT_ID_COL]} in {PIIConst.PII_TABLE_NAME}.")
#                     else:
#                         insert_stmt = pii_data_table.insert().values(**data_row)
#                         dest_conn.execute(insert_stmt)
#                         nd_logger.info(f"Inserted patient {data_row[PIIConst.PATIENT_ID_COL]} into {PIIConst.PII_TABLE_NAME}.")

#                     # if result:
#                     #     update_stmt = update(pii_data_table).where(
#                     #         pii_data_table.c.patient_id == data_row[PIIConst.PATIENT_ID_COL]
#                     #     ).values(**data_row)
#                     #     dest_conn.execute(update_stmt)
#                     #     nd_logger.info(f"Updated patient {data_row[PIIConst.PATIENT_ID_COL]} in {PIIConst.PII_TABLE_NAME}.")
#                     # else:
#                     #     insert_stmt = pii_data_table.insert().values(**data_row)
#                     #     dest_conn.execute(insert_stmt)
#                     #     nd_logger.info(f"Inserted patient {data_row[PIIConst.PATIENT_ID_COL]} into {PIIConst.PII_TABLE_NAME}.")

#                 except IntegrityError as e:
#                     nd_logger.info(f"Error occurred while inserting/updating patient {data_row['patient_id']}: {e}")

#         nd_logger.info(f"Data from table {table} successfully inserted/updated into 'patient_pii_data'.")


#     def generate_pii_table(self):
#         self.create_master_table()
#         for table, conf in self.pii_table_config.get('table_details', {}).items():
#             self._insert_data_to_pii_table(table, conf['patient_id_column'], conf['other_required_columns'])
