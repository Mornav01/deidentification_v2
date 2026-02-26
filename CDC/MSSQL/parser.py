from sqlalchemy import text
from db import get_session
from decoder import decode_rowlog_contents

def read_trn_log_file(trn_path: str):
    sql = text(f"""
        SELECT
            [Current LSN],
            [Operation],
            [Transaction ID],
            [AllocUnitName],
            [RowLog Contents 0],
            [RowLog Contents 1]
        FROM fn_dump_dblog
        (
            NULL, NULL,
            N'DISK',
            1,
            N'{trn_path}',
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL
        );
    """)

    session = get_session()
    rows = session.execute(sql).fetchall()
    session.close()
    return rows

def detect_operation(op):
    if op == "LOP_INSERT_ROWS":
        return "INSERT"
    if op == "LOP_MODIFY_ROW":
        return "UPDATE"
    if op == "LOP_DELETE_ROWS":
        return "DELETE"
    return None


def parse_trn_rows(rows, schema):
    parsed = []
    for r in rows:
        op = detect_operation(r[1])
        if not op:
            continue
        
        before = decode_rowlog_contents(r[4], schema)
        after  = decode_rowlog_contents(r[5], schema)

        parsed.append({
            "operation": op,
            "before": before,
            "after": after
        })

    return parsed


# trn_path = "/var/opt/mssql/dump/20251017/PrimeRecord1697/PrimeRecord1697_LOG_20251012_200000.trn"
# rows = read_trn_log_file(trn_path)
# print(f"Total records in the TRN file: {len(rows)}")
# print(f"Sample rows: {rows[:5]}")

# parsed = parse_trn_rows(rows, 'dbo')
# print(f"Total records parsed: {len(parsed)}")
# print(f"Sample parsed data: {parsed[:5]}")