#!/usr/bin/env python
"""Converted from decrypt_cnotes_custom.ipynb (Jupyter notebook)."""

# %% [code cell 1]
import os
import binascii
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
output_dir = "/Volumes/NDAIVol/LillyAD_Documents"
os.makedirs(output_dir, exist_ok=True)

# %% [code cell 2]
df = pd.read_csv("lilly_ad.csv")
patients = tuple(df['patient_id'].to_list())
len(patients)

# %% [code cell 3]
docdf = pd.read_sql(f"SELECT distinct DocumentID, PatientID FROM [Primerecord_oct].[dbo].[ClinicalDocuments] WHERE PatientID IN {patients}", engine.connect())
docdf['DocumentID'] = docdf['DocumentID'].astype(int)
docdf['PatientID'] = docdf['PatientID'].astype(int)
docids = tuple(docdf['DocumentID'].to_list())
len(docids)

# %% [code cell 4]
doc_dict = docdf.set_index('DocumentID')['PatientID'].to_dict()
len(doc_dict)

# %% [code cell 5]
batch_size = 10000
offset = 0

with engine.connect() as conn:
    while True:
        query = text(f"""
            SELECT DocumentID, SequenceNumber, BinTypeID, DocImage
            FROM ClinicalBin WHERE BinTypeID IN (1001, 1007)
            AND DocumentID IN {docids}
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
            file_path = os.path.join(output_dir, file_name)

            # if os.path.exists(file_path):
            #     # print(f"⏩ **Skipped**: File already exists at {file_path}")
            #     continue
            
            if file_ext == 'tif':
                string_data = binary_data.decode('latin-1', errors='ignore')
                temp_string = string_data.strip().replace('0x', '').replace(' ', '')
                hex_string = re.sub(r'[^0-9a-fA-F]', '', temp_string)
                if len(hex_string) % 2 != 0:
                    hex_string = '0' + hex_string
                binary_data = binascii.unhexlify(hex_string)
            try:
                # Save file
                with open(file_path, 'wb') as f:
                    f.write(binary_data)

                # print(f"✅ Saved: {file_path}")

            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")

        offset += batch_size
