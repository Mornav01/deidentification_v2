from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Integer, inspect
from sqlalchemy.dialects.mssql import VARCHAR, DATETIME
from sqlalchemy.types import UnicodeText, DECIMAL, Text
from sqlalchemy.sql import select
from datetime import datetime
import re

# --- Config ---
mssql_conn_str = "mssql+pymssql://sa:ndADMIN2025@localhost:1433/mobiledoc"
source_table_name = "electronichl7content_archive2"
new_table_name = "electronichl7content_archive2_with_patient"
hl7_column = "HL7Message"

# --- Connect ---
engine = create_engine(mssql_conn_str)
inspector = inspect(engine)

# --- Reflect source table structure ---
source_metadata = MetaData()
source_table = Table(source_table_name, source_metadata, autoload_with=engine)
columns_info = inspector.get_columns(source_table_name)

# --- Define new columns ---
new_columns = []
for col in columns_info:
    col_name = col['name']
    col_nullable = col.get('nullable', True)

    if col_name in ("HL7Message", "newHL7Message"):
        col_type = Text()
    else:
        col_type = col['type']
        if hasattr(col_type, 'length') and col_type.length and col_type.length > 8000:
            col_type = UnicodeText()

    new_columns.append(Column(col_name, col_type, nullable=col_nullable))

# Add extra patient info columns
new_columns.extend([
    Column("patient_id", Integer),
    Column("patient_name", String(100)),
    Column("patient_dob", Date),
    Column("patient_first_name", String(50)),
    Column("patient_last_name", String(50)),
])

# --- Define new table ---
target_metadata = MetaData()
new_table = Table(new_table_name, target_metadata, *new_columns)

# --- Drop and recreate target table ---
with engine.begin() as conn:
    if inspector.has_table(new_table_name):
        new_table.drop(conn)
    target_metadata.create_all(conn)

# --- HL7 Parse Helper ---
def extract_patient_info(hl7_message):
    try:
        pid_match = re.search(r"PID\|([^\n\r]*)", hl7_message)
        if not pid_match:
            return 0, None, None
        fields = pid_match.group(1).split('|')

        # Extract patient_id (field 3)
        pid_raw = fields[1] if len(fields) > 2 else None
        try:
            pid_val = int(pid_raw)
            patient_id = pid_val if -2147483648 <= pid_val <= 2147483647 else 0
        except (ValueError, TypeError):
            patient_id = 0

        # Extract patient name
        name_parts = fields[4].split('^') if len(fields) > 4 else []
        patient_name = ' '.join(name_parts[:2]) if name_parts else None

        # Extract DOB
        dob_raw = fields[6] if len(fields) > 6 else None
        try:
            dob = datetime.strptime(dob_raw, "%Y%m%d").date() if dob_raw else None
        except ValueError:
            dob = None

        return patient_id, patient_name, dob
    except:
        return 0, None, None

# --- HL7 Sanitizer ---
def sanitize_hl7(text):
    if text is None:
        return None
    return ''.join(c for c in text if c.isprintable()).replace("'", "''")

# --- Load, transform and insert data ---
insert_data = []

with engine.connect() as conn:
    result = conn.execute(select(source_table))
    for row in result:
        row_dict = dict(row._mapping)

        hl7 = row_dict.get(hl7_column, "")
        if not hl7:
            continue

        # Sanitize HL7 fields
        if "HL7Message" in row_dict:
            row_dict["HL7Message"] = sanitize_hl7(row_dict["HL7Message"])
        if "newHL7Message" in row_dict:
            row_dict["newHL7Message"] = sanitize_hl7(row_dict["newHL7Message"])

        # Extract patient data
        pid, name, dob = extract_patient_info(hl7)
        row_dict["patient_id"] = pid
        row_dict["patient_name"] = name
        row_dict["patient_dob"] = dob

        # First and last name
        if name:
            name_parts = name.split()
            row_dict["patient_first_name"] = name_parts[0] if len(name_parts) > 0 else None
            row_dict["patient_last_name"] = name_parts[1] if len(name_parts) > 1 else None
        else:
            row_dict["patient_first_name"] = None
            row_dict["patient_last_name"] = None

        insert_data.append(row_dict)

# --- Insert into new table ---
with engine.begin() as conn:
    conn.execute(new_table.insert(), insert_data)

print(f"✅ Created '{new_table_name}' and inserted {len(insert_data)} records.")
