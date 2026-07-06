#!/usr/bin/env python
"""Converted from decrypt_cnotes_delta.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
import re
import zlib
from lxml import etree
import pandas as pd
from sqlalchemy import create_engine, text

# Connect to database
engine = create_engine(f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1433/PrimeRecordBin_oct", pool_pre_ping=True, pool_recycle=3600)

# File extension map based on BinTypeID
bintypeid_to_ext = {
    1000: 'xml',
    1001: 'pdf',
    1003: 'txt',
    1004: 'xml',
    1005: 'xml',
    1007: 'tif',
    1016: 'xml'
}

# Output folder
output_dir = "/Volumes/NDAIVol/Clinical Documents"
# output_dir = "/Volumes/NDAIVol/XML Clinical Docs"
os.makedirs(output_dir, exist_ok=True)

# %% [code cell 2]
docdf = pd.read_sql(f"SELECT distinct DocumentID, PatientID FROM [Primerecord_oct].[dbo].[ClinicalDocuments]", engine.connect())
docdf['DocumentID'] = docdf['DocumentID'].astype(int)
docdf['PatientID'] = docdf['PatientID'].astype(int)
docdf.shape

# %% [code cell 3]
doc_dict = docdf.set_index('DocumentID')['PatientID'].to_dict()
len(doc_dict)

# %% [markdown cell 4]
# Total records- 4777191

# %% [code cell 5]
batch_size = 10000
offset = 0
offset = 4140000

with engine.connect() as conn:
    while True:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        query = text(f"""
            SELECT DocumentID, SequenceNumber, BinTypeID, DocImage
            FROM ClinicalBin WHERE BinTypeID IN (1001, 1007)
            ORDER BY DocumentID
            OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY;
        """)
        response = conn.execute(query)
        results = response.fetchall()
        print(f"Fetched {len(results)} records (offset {offset})")

        if not results:
            break
        
        for row in results:
            doc_id = row.DocumentID
            patient_id = doc_dict[doc_id]
            seq_no = row.SequenceNumber
            bintypeid = row.BinTypeID
            binary_data = row.DocImage  # Already in bytes

            file_ext = bintypeid_to_ext.get(bintypeid, 'bin')
            file_name = f"{patient_id}_{doc_id}_{seq_no}_{bintypeid}.{file_ext}"

            directory_path = os.path.join(output_dir, str(patient_id))
            file_path = os.path.join(directory_path, file_name)

            if os.path.exists(file_path):
                # print(f"⏩ **Skipped**: File already exists at {file_path}")
                continue

            os.makedirs(directory_path, exist_ok=True)

            try:
                # Save file
                with open(file_path, 'wb') as f:
                    f.write(binary_data)

                # print(f"✅ Saved: {file_path}")

            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")

        offset += batch_size
