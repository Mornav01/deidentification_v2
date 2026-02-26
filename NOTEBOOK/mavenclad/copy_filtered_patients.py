import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os

# === CONFIGURATION ===
BATCH_SIZE = 500   # number of patient/encounter IDs per batch
STATUS_FILE = "table_status.csv"


# SQLAlchemy Engines
engine = create_engine("mssql+pymssql://sa:ndADMIN2025@localhost:1433/db_masnin")
mpengine = create_engine("mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/mavenclad_patients")


# Setup logging
logging.basicConfig(
    filename="failed_tables.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# === STEP 1: Load mapping data from MySQL ===
try:
    mapping_query = "SELECT nd_patient_id, patient_id, encounterid FROM Patient_encounter_ids"
    mapping_df = pd.read_sql(mapping_query, mpengine)
    patient_ids_list = mapping_df["patient_id"].dropna().unique().tolist()
    encounter_ids_list = mapping_df["encounterid"].dropna().unique().tolist()
except Exception as e:
    print(f"❌ Error loading mapping data: {e}")
    exit(1)

# === STEP 2: Load tables and columns from SQL Server ===
try:
    table_col_query = """
    SELECT s.name AS schema_name, t.name AS table_name, 
           c.name AS column_name, ty.name AS data_type, 
           c.max_length, c.precision, c.scale
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    ORDER BY s.name, t.name, c.column_id;
    """
    columns_df = pd.read_sql(table_col_query, engine)
    columns_df.columns = columns_df.columns.str.lower()
except Exception as e:
    print(f"❌ Error loading table metadata: {e}")
    exit(1)

# === STEP 3: Map SQL Server types to MySQL ===
def map_sqlserver_to_mysql(sql_type, max_len, precision, scale, observed_max_length=None):
    sql_type = sql_type.lower()
    if sql_type in ["int", "smallint", "bigint", "tinyint"]:
        return "INT"
    elif sql_type in ["numeric", "decimal", "money", "smallmoney"]:
        return f"DECIMAL({precision},{scale})"
    elif sql_type in ["float", "real"]:
        return "FLOAT"
    elif sql_type == "bit":
        return "TINYINT(1)"
    elif sql_type in ["datetime", "smalldatetime", "date", "time", "datetime2"]:
        return "DATETIME"
    elif sql_type in ["char", "nchar", "varchar", "nvarchar", "text", "ntext"]:
        return "LONGTEXT"
    else:
        return "LONGTEXT"

# === STEP 4: Setup status CSV ===
if os.path.exists(STATUS_FILE):
    status_df = pd.read_csv(STATUS_FILE)
else:
    status_df = pd.DataFrame(columns=["tablename", "identifier_column", "comment"])
    status_df.to_csv(STATUS_FILE, index=False)

# === Helper: Update or insert status ===
def update_status(status_df, table, identifier_col, comment):
    if table in status_df["tablename"].values:
        status_df.loc[status_df["tablename"] == table, ["identifier_column", "comment"]] = [
            identifier_col if identifier_col else "",
            comment if comment else "skipped"
        ]
    else:
        new_row = pd.DataFrame([{
            "tablename": table,
            "identifier_column": identifier_col if identifier_col else "",
            "comment": comment if comment else "skipped"
        }])
        status_df = pd.concat([status_df, new_row], ignore_index=True)
    return status_df

# === STEP 5: Keywords for identifiers ===
patient_id_keywords = [
    "patientid", "patient_id",  "pid", "ptid", "relid", "uid", "doc_patientid", "m_frompatientid", 
    "el_patientid", "enc_patientid", "la_patientid", "ref_patientid", 
    "ecwpid",  "associateduid", "localpid",  "urefid", "uid", "userid", "contactid", "users_uid", "patientControlNo", "ControlNo"
]

encounter_id_keywords = [
    "encounterid", "attachedtoencid", "newencounterid", "enc_encounterid", 
    "telenc_encounterid", "encounter_id", "encid","ld_encounterid",  "progressnoteencid", 
     "refencid", "externalencounterid", "linkedencounterid", 
    "grpencid"
]

def fetch_in_batches(col, ids, fq_table):
    """Generator that yields DataFrames in batches of IDs."""
    for i in range(0, len(ids), BATCH_SIZE):
        chunk = ids[i : i + BATCH_SIZE]
        if not chunk:
            continue
        ids_str = ",".join(f"'{x}'" for x in chunk if x is not None)
        query = f"SELECT * FROM {fq_table} WHERE {col} IN ({ids_str})"
        yield pd.read_sql(query, engine)

# === STEP 6: Process tables ===
for (schema, table), group in columns_df.groupby(["schema_name", "table_name"]):
    fq_table = f"{schema}.{table}"
    new_table = f"{table}_emd"

    '''
    if table not in ['Voiceenc', 'ACG_COST_RANGE_LOOKUP', 'ACG_FRAILTY_CONCEPTS']:
        continue
    '''

    # Check rerun condition
    if table in status_df["tablename"].values:
        prev_comment = status_df.loc[status_df["tablename"] == table, "comment"].values[0]
        if prev_comment in ["no identifier found", "Completed successfully", "no rows found"]:
            print(f"⏩ Skipping {table}, status = {prev_comment}")
            continue
        else:
            print(f"🔄 Rerunning {table}, previous status = {prev_comment}")

    identifier_col = None
    comment = None

    try:
        with mpengine.begin() as mconn:
            mconn.execute(text(f"DROP TABLE IF EXISTS {new_table}"))
            print(f"✅ Dropped {new_table} if existed.")

            col_names = group["column_name"].tolist()
            # Exact match first
            patient_cols = [c for c in col_names if c.lower() in (k.lower() for k in patient_id_keywords)]
            encounter_cols = [c for c in col_names if c.lower() in (k.lower() for k in encounter_id_keywords)]

            # Then partial match if nothing exact
            if not patient_cols:
                patient_cols = [c for k in patient_id_keywords for c in col_names if k.lower() in c.lower()]
            if not encounter_cols:
                encounter_cols = [c for k in encounter_id_keywords for c in col_names if k.lower() in c.lower()]


            dfs = []
            if patient_cols:
                identifier_col = patient_cols[0]
                print(f"🔎 Filtering {fq_table} by patient column {identifier_col}...")
                for df_chunk in fetch_in_batches(identifier_col, patient_ids_list, fq_table):
                    if not df_chunk.empty:
                        dfs.append(df_chunk)
            elif encounter_cols:
                identifier_col = encounter_cols[0]
                print(f"🔎 Filtering {fq_table} by encounter column {identifier_col}...")
                for df_chunk in fetch_in_batches(identifier_col, encounter_ids_list, fq_table):
                    if not df_chunk.empty:
                        dfs.append(df_chunk)
            else:
                comment = "no identifier found"
                print(f"⚠ No patient/encounter column in {fq_table}. Skipping.")
                status_df = update_status(status_df, table, "", comment)
                status_df.to_csv(STATUS_FILE, index=False)
                continue

            if not dfs:
                comment = "no rows found"
                logging.error(f"{fq_table} - Found {identifier_col} but no matching rows.")
                print(f"⚠ {fq_table} has no rows after filtering. Skipping.")
                status_df = update_status(status_df, table, identifier_col, comment)
                status_df.to_csv(STATUS_FILE, index=False)
                continue

            df = pd.concat(dfs, ignore_index=True)
            if df.empty:
                comment = "no rows found"
                print(f"⚠ {fq_table} empty after concat. Skipping table creation.")
                status_df = update_status(status_df, table, identifier_col, comment)
                status_df.to_csv(STATUS_FILE, index=False)
                continue

            # Detect max observed lengths
            max_lengths = df.applymap(lambda x: len(str(x)) if pd.notnull(x) else 0).max()

            # Create MySQL table
            col_defs = []
            for _, col in group.iterrows():
                col_name = col["column_name"]
                data_type = map_sqlserver_to_mysql(
                    col["data_type"], col["max_length"], col["precision"], col["scale"],
                    observed_max_length=max_lengths.get(col_name)
                )
                col_defs.append(f"`{col_name}` {data_type}")
            
            mconn.execute(text(f"DROP TABLE IF EXISTS {new_table}"))
            print(f"🗑️ Dropped {new_table} if it existed.")

            create_sql = f"CREATE TABLE {new_table} ({', '.join(col_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            mconn.execute(text(create_sql))
            print(f"✅ Created {new_table}")

            df.to_sql(new_table, mpengine, if_exists="append", index=False)
            print(f"✅ Inserted {len(df)} rows into {new_table}.")
            comment = "Completed successfully"

    except Exception as e:
        logging.error(f"Failed table {fq_table}: {e}")
        comment = f"error: {e}"
        print(f"❌ Error processing {fq_table}. See log.")

    # Save/update status
    status_df = update_status(status_df, table, identifier_col, comment)
    status_df.to_csv(STATUS_FILE, index=False)
