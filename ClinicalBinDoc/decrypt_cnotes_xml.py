#!/usr/bin/env python
"""Converted from decrypt_cnotes_xml.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
import re
import zlib
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text

# Connect to database
source_engine = create_engine(f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1433/PrimeRecordBin_Jan26", pool_pre_ping=True, pool_recycle=3600)
dest_engine = create_engine(f"mysql+pymysql://{os.environ.get('DB_USER','')}:{os.environ.get('DB_PASS','')}@localhost:3306/primerecord_bin", pool_pre_ping=True, pool_recycle=3600)

# File extension map based on BinTypeID
bintypeid_to_ext = {
    1004: 'xml',
    1000: 'xml',
    1001: 'pdf',
    1016: 'xml',
    1003: 'txt',
    1005: 'xml',
    1007: 'tif'
}

# Output folder
# output_dir = "/Volumes/NDAIVol/Clinical Documents"
# output_dir = "/Volumes/NDAIVol/XML Clinical Docs"
# os.makedirs(output_dir, exist_ok=True)

# %% [code cell 2]
docdf = pd.read_sql(f"SELECT distinct DocumentID, PatientID FROM [PrimeRecord_06012026].[dbo].[ClinicalDocuments]", source_engine.connect())
docdf['DocumentID'] = docdf['DocumentID'].astype(int)
docdf['PatientID'] = docdf['PatientID'].astype(int)
docdf.shape

# %% [code cell 3]
doc_dict = docdf.set_index('DocumentID')['PatientID'].to_dict()
len(doc_dict)

# %% [code cell 4]
def clean_xml(xml_bytes):
    # Decode to string
    xml_string = xml_bytes.decode("utf-8", errors="ignore")
    
    # Remove control characters
    xml_string = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", xml_string)
    
    # Try parsing to validate structure
    try:
        parser = etree.XMLParser(recover=True)  # recover=True fixes broken XML
        root = etree.fromstring(xml_string.encode('utf-8'), parser)
        return etree.tostring(root, encoding="utf-8").decode("utf-8")  # Return fixed XML string
    except Exception as e:
        print(f"⚠ Invalid XML skipped or stored as raw text: {e}")
        return None  # or fallback to saving as text instead of XML

# %% [code cell 5]
failed_docids = []
batch_size = 10000
offset = 0

with source_engine.connect() as conn:
    while True:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        query = text(f"""
            SELECT DocumentID, SequenceNumber, BinTypeID, DocImage
            FROM ClinicalBin WHERE BinTypeID IN (1000, 1004, 1005, 1016)
            ORDER BY DocumentID
            OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY;
        """)
        response = conn.execute(query)
        results = response.fetchall()
        print(f"Fetched {len(results)} records (offset {offset})")

        if not results:
            break
        
        insert_data = []
        for row in results:
            doc_id = row.DocumentID
            seq_no = row.SequenceNumber
            bintypeid = row.BinTypeID
            binary_data = row.DocImage  # Already in bytes
            try:
                patient_id = doc_dict[doc_id]
            except:
                failed_docids.append(doc_id)
                patient_id = None

            file_ext = bintypeid_to_ext.get(bintypeid, 'bin')

            try:
                # Attempt zlib decompression for XML-like formats
                if file_ext == "xml":
                    if binary_data.startswith(b'x\x9c') or binary_data.startswith(b'\x78'):
                        try:
                            binary_data = zlib.decompress(binary_data)
                            xml_string = clean_xml(binary_data)
                            # print(f"🔓 Decompressed: {file_name}")
                        except zlib.error:
                            # print(f"⚠️ Not zlib or already decompressed: {file_name}")
                            continue

                # Append to batch insert
                insert_data.append({
                    "patient_id": patient_id,
                    "DocumentID": doc_id,
                    "SequenceNumber": seq_no,
                    "BinTypeID": bintypeid,
                    "DocContent": xml_string 
                })

            except Exception as e:
                print(f"❌ Error processing {patient_id}, {doc_id}, {seq_no}, {bintypeid}: {e}")

        # Perform batch insert into MySQL
        if insert_data:
            with dest_engine.connect() as dest_conn:
                dest_conn.execute(text("""
                    INSERT INTO clinicalbin_xml_decrypt (patient_id, DocumentID, SequenceNumber, BinTypeID, DocContent)
                    VALUES (:patient_id, :DocumentID, :SequenceNumber, :BinTypeID, :DocContent)
                """), insert_data)
                dest_conn.commit()
        offset += batch_size
