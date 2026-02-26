import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

def map_dtype_to_mysql(dtype):
    """Map pandas/SQL Server types to MySQL-compatible types."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "TEXT"

def mssql_to_mysql(mssql_conn_str, mysql_conn_str, source_table, destination_table):
    # Connect to MSSQL
    mssql_engine = sqlalchemy.create_engine(mssql_conn_str)
    df = pd.read_sql(f"SELECT * FROM {source_table}", mssql_engine)

    # Connect to MySQL
    mysql_engine = sqlalchemy.create_engine(mysql_conn_str)
    with mysql_engine.connect() as conn:
        # Drop destination table if exists
        conn.execute(text(f"DROP TABLE IF EXISTS `{destination_table}`"))

        # Create destination table with inferred column types
        column_defs = []
        for col in df.columns:
            col_type = map_dtype_to_mysql(df[col].dtype)
            column_defs.append(f"`{col}` {col_type}")
        create_stmt = f"CREATE TABLE `{destination_table}` ({', '.join(column_defs)})"
        conn.execute(text(create_stmt))

        # Insert data into MySQL
        df.to_sql(destination_table, con=mysql_engine, if_exists='append', index=False)
        print(f"✅ Successfully migrated {len(df)} rows from '{source_table}' to '{destination_table}'.")

# ----------------------
# Run the migration
# ----------------------
if __name__ == "__main__":
    mssql_conn_str = 'mssql+pymssql://sa:ndADMIN2025@localhost:1433/mobiledoc'
    mysql_conn_str = 'mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/deidentified'
    
    # Change these as needed
    source_table = "ebo_cptlevel_cascodes"
    destination_table = "ebo_cptlevel_cascodes"

    mssql_to_mysql(mssql_conn_str, mysql_conn_str, source_table, destination_table)
