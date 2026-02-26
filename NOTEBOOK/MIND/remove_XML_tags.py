from sqlalchemy import create_engine, text
from lxml import etree
import xmltodict
import json
import re

# ✅ MSSQL connection
engine = create_engine(
    "mssql+pyodbc://sa:ndADMIN2025@localhost,1433/PrimeRecordBin_oct?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes",
    fast_executemany=True,
    pool_pre_ping=True
)


def repair_xml(xml_str: str) -> str:
    """
    Attempts to repair incomplete or malformed XML content.
    - Closes unclosed tags
    - Removes invalid control chars
    - Wraps fragment in root if completely broken
    """
    if not xml_str:
        return None

    # Remove control characters
    xml_str = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", xml_str).strip()

    # If missing root tag, wrap it in one
    if not xml_str.strip().startswith("<"):
        xml_str = f"<root>{xml_str}</root>"

    # Try parsing with lxml recovery mode (auto-fix)
    parser = etree.XMLParser(recover=True)
    try:
        root = etree.fromstring(xml_str.encode("utf-8"), parser)
        repaired = etree.tostring(root, encoding="utf-8").decode("utf-8")
        return repaired
    except Exception:
        # Fallback: wrap in root if parsing still fails
        return f"<root>{xml_str}</root>"


def xml_to_clean_dict(xml_content: str) -> str:
    """
    Converts XML to a dict (JSON string) safely,
    repairing malformed XML automatically.
    """
    if not xml_content:
        return None

    xml_fixed = repair_xml(xml_content)

    try:
        data_dict = xmltodict.parse(
            xml_fixed,
            attr_prefix='',  # no '@'
            cdata_key='text',
            dict_constructor=dict
        )
        return json.dumps(data_dict, ensure_ascii=False)
    except Exception as e:
        # Fallback: Try parsing inner text if structure too broken
        parser = etree.XMLParser(recover=True)
        try:
            root = etree.fromstring(xml_fixed.encode("utf-8"), parser)
            text_content = ''.join(root.itertext()).strip()
            return json.dumps({"text": text_content}, ensure_ascii=False)
        except Exception:
            return json.dumps({"text": xml_content[:1000]}, ensure_ascii=False)  # safe fallback


# ✅ Config
source_table = "dbo.ClinicalBin_decrypt"
target_table = "dbo.clinicalbin_clean_text"
batch_size = 100
last_id = 1514758
key_column = "nd_auto_increment_id"


with engine.connect() as conn:
    while True:
        query = text(f"""* fr
            SELECT *
            FROM {source_table}
            WHERE {key_column} > :last_id
            ORDER BY {key_column} ASC
            OFFSET 0 ROWS FETCH NEXT :batch_size ROWS ONLY;
        """)
        results = conn.execute(query, {"last_id": last_id, "batch_size": batch_size}).fetchall()
        print(f"📦 Fetched {len(results)} records (last_id={last_id})")

        if not results:
            print("✅ All records processed.")
            break

        insert_data = []
        for row in results:
            row_dict = dict(row._mapping)
            auto_id = row_dict.get("nd_auto_increment_id")
            doc_id = row_dict.get("DocumentID")
            xml_data = row_dict.get("DocContent")
            sequence_number = row_dict.get("SequenceNumber")
            bintypeid = row_dict.get("BinTypeID")
            visit_id = row_dict.get("VisitID")
            patient_id = row_dict.get("PatientID")

            if not xml_data:
                continue

            # ✅ Parse + repair XML → dict JSON
            clean_dict_json = xml_to_clean_dict(xml_data)

            insert_data.append({
                "DocumentID": doc_id,
                "SequenceNumber": sequence_number,
                "BinTypeID": bintypeid,
                "nd_auto_increment_id": auto_id,
                "DocContent_clean": clean_dict_json,
                "VisitID": visit_id,
                "PatientID": patient_id
            })

            last_id = auto_id

        if insert_data:
            insert_query = text(f"""
                INSERT INTO {target_table} 
                    (DocumentID, SequenceNumber, BinTypeID, DocContent_clean, nd_auto_increment_id, VisitID, PatientID)
                VALUES 
                    (:DocumentID, :SequenceNumber, :BinTypeID, :DocContent_clean, :nd_auto_increment_id, :VisitID, :PatientID)
            """)
            conn.execute(insert_query, insert_data)
            conn.commit()
            print(f"✅ Inserted {len(insert_data)} records up to id={last_id}")
