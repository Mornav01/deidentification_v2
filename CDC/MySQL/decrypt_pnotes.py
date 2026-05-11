"""
Decrypt progress notes from the progressnotes table and write to progressnotes_decryptfinal.
Reads from staging_schema and writes decrypted data to the same schema.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import base64
import re
import zlib
from Crypto.Cipher import Blowfish
from sqlalchemy import create_engine, text, MetaData, Table, Column, select, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.mssql import NVARCHAR
from tqdm import tqdm

# Configure logger
logger = logging.getLogger(__name__)


def setup_logging(level=logging.INFO):
    """Configure logging for the script."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


class ProgressNoteDecryptor:
    def __init__(self, datakey="qdnmbf@##$"):
        self._datakey = datakey
        self._start_js_txt = '{ "documents": ['
        self._end_js_txt = '] }'
        self._create_text = False
        self._add_cpt_code = True
        self.TAG_RE = re.compile(r'<[^>]+>')

    def get_key(self, key_type, key_date):
        """Generate encryption/decryption key based on key_type and key_date."""
        local_date = key_date.replace(' ', '_')
        key_val = ''

        if key_type == 1:
            key_val = self._datakey[::-1]
            key_val += local_date[6:12]
        else:
            key_val = self._datakey
            key_val += local_date[2:6]

        return bytes(key_val, 'utf-8')

    def remove_html_tags(self, text):
        """Remove HTML tags using a simple regex."""
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

    def remove_tags(self, text):
        """Remove HTML tags using the compiled TAG_RE."""
        return self.TAG_RE.sub('', text)

    def validate(self, date_text):
        """Validate that date_text has the format YYYY-MM-DD."""
        try:
            if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
                raise ValueError
            return True
        except ValueError:
            return False

    def decrypt_pnote(self, d, p):
        _key = self.get_key(1, d)
        _isclear = 0
        ecw_plain = "<?xml version"

        if p[:5] == ecw_plain[:5]:
            _isclear = 1

        if _isclear == 0:
            msg = base64.b64decode(p[40:])
            pads_required = 16 - (len(msg) % 16)
            padchar = b'\x00'

            if pads_required:
                msg += padchar * pads_required

            c3 = Blowfish.new(_key, Blowfish.MODE_ECB)
            m3 = c3.decrypt(msg)

            try:
                _plaintext = zlib.decompress(m3, zlib.MAX_WBITS | 32)
                _plaintext = self.cleanup_bytes(_plaintext)
            except Exception:
                _plaintext = b""
        else:
            _plaintext = p.encode('utf-8')
            _plaintext = self.cleanup_bytes(_plaintext)

        return _plaintext

    def cleanup_bytes(self, byte_data):
        """Cleans up unwanted or problematic byte characters."""
        replace_map = {
            b'\x0a': b'',
            b'\x0d': b'',
            b'\x0b': b' ',
            b'\xf8': b' ',
            b'\xe8': b' ',
            b'\xe9': b' ',
            b'\xe3': b' ',
            b'\x84': b'a',
            b'\x85': b'a',
            b'\x96': b'-',
            b'\x97': b'u',
            b'\xc2': b'',
            b'\x92': b''
        }
        for k, v in replace_map.items():
            byte_data = byte_data.replace(k, v)
        return byte_data

    def process_text(self, dtmod, summary):
        """Decrypt the progress note and return plaintext."""
        pnclear = self.decrypt_pnote(dtmod, summary)
        return pnclear


def connect_to_db(connection_string):
    """Establish a connection to the database."""
    try:
        engine = create_engine(connection_string)
        connection = engine.connect()
        logger.info("Connection successful.")
        return engine, connection
    except Exception as e:
        logger.error("Connection failed: %s", e)
        return None, None


def read_table(connection, metadata, table_name):
    """Read data from the specified table."""
    try:
        table = Table(table_name, metadata, autoload_with=connection)
        stmt = select(table)
        result = connection.execute(stmt)
        rows = result.fetchall()
        logger.info("Data read successfully from table '%s' with %d rows.", table_name, len(rows))
        return table, rows
    except Exception as e:
        logger.error("Failed to read table '%s': %s", table_name, e)
        return None, None


def create_new_table(engine, metadata, original_table, new_table_name):
    """Create a new table with appropriate column types for decrypted summary."""
    try:
        new_columns = []
        for column in original_table.columns:
            if column.name == 'summary':
                if engine.dialect.name == 'mysql':
                    new_column_type = LONGTEXT
                elif engine.dialect.name == 'mssql':
                    new_column_type = NVARCHAR(None)
                else:
                    new_column_type = Text
                new_column = Column(
                    column.name, new_column_type,
                    **{key: getattr(column, key) for key in ['primary_key', 'nullable', 'unique', 'index', 'autoincrement']}
                )
            else:
                new_column = Column(
                    column.name, column.type,
                    **{key: getattr(column, key) for key in ['primary_key', 'nullable', 'unique', 'index', 'autoincrement']}
                )
            new_columns.append(new_column)

        new_table = Table(new_table_name, metadata, *new_columns)
        metadata.create_all(engine, tables=[new_table])
        logger.info("Table '%s' created successfully with appropriate 'summary' column type.", new_table_name)
        return new_table
    except Exception as e:
        logger.error("Failed to create table '%s': %s", new_table_name, e)
        return None


def batch(iterable, n=1):
    """Yield successive n-sized chunks from iterable."""
    length = len(iterable)
    for i in range(0, length, n):
        yield iterable[i:i + n]


def process_and_insert_data(connection, new_table, rows, max_workers=20, batch_size=10000):
    """Decrypt and insert data into the new table."""
    try:
        decryptor = ProgressNoteDecryptor()
        insert_stmt = text(f"""
            INSERT INTO {new_table.name}
            (encounterID, summary, xslId, unlocked, ModifyDate, ModifyDate2, regionName, nd_auto_increment_id, nd_extracted_date, nd_updated_at, nd_operation, nd_is_active)
            VALUES (:encounterID, :summary, :xslId, :unlocked, :ModifyDate, :ModifyDate2, :regionName, :nd_auto_increment_id, :nd_extracted_date, :nd_updated_at, :nd_operation, :nd_is_active)
        """)

        def process_row_batch(row_batch):
            processed = []
            for row in row_batch:
                if row is None:
                    continue
                try:
                    row_dict = row._asdict()
                    if row_dict.get('summary', ''):
                        summary = decryptor.process_text(
                            str(row_dict.get('ModifyDate', '')),
                            row_dict.get('summary', '')
                        ) or ""

                        if isinstance(summary, bytes):
                            summary = summary.decode("utf-8", errors="ignore")
                        elif str(summary).startswith("b'"):
                            summary = str(summary)[2:-1]
                        else:
                            summary = str(summary)
                    else:
                        summary = ""

                    row_dict['summary'] = summary
                    processed.append(row_dict)
                except Exception as e:
                    logger.warning("Error processing row: %s | Row: %s", e, row)
            return processed

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for row_chunk in tqdm(batch(rows, batch_size), desc="Processing batches", unit="batch"):
                futures.append(executor.submit(process_row_batch, row_chunk))

            for future in tqdm(futures, desc="Inserting batches", unit="batch"):
                new_rows = future.result()
                if new_rows:
                    connection.execute(insert_stmt, new_rows)
                    connection.commit()

        logger.info("Inserted %d rows successfully into '%s'.", len(rows), new_table.name)

    except Exception as e:
        logger.error("Failed to insert data into table '%s': %s", new_table.name, e)
        raise


def build_connection_string(mysql_host, mysql_user, mysql_pass, schema):
    """Build MySQL connection string."""
    from urllib.parse import quote_plus
    password_encoded = quote_plus(mysql_pass)
    return f"mysql+pymysql://{mysql_user}:{password_encoded}@{mysql_host}:3306/{schema}"


def main(
    staging_schema: str,
    mysql_host: str = os.environ.get("DB_HOST", "localhost"),
    mysql_user: str = os.environ.get("DB_USER", ""),
    mysql_pass: str = os.environ.get("DB_PASS", ""),
    table_name: str = "progressnotes",
    new_table_name: Optional[str] = None,
    max_workers: int = 10,
    batch_size: int = 1000,
):
    """Run the decrypt progress notes pipeline."""
    new_table_name = new_table_name or f"{table_name}_decryptfinal"
    connection_string = build_connection_string(mysql_host, mysql_user, mysql_pass, staging_schema)

    logger.info("Starting decrypt_pnotes for schema=%s, table=%s", staging_schema, table_name)

    engine, connection = connect_to_db(connection_string)
    if not engine or not connection:
        raise RuntimeError("Failed to connect to database")

    try:
        metadata = MetaData()
        original_table, rows = read_table(connection, metadata, table_name)

        if original_table is None or rows is None:
            raise RuntimeError("Failed to read source table")

        new_table = create_new_table(engine, metadata, original_table, new_table_name)
        if new_table is None:
            raise RuntimeError("Failed to create destination table")

        process_and_insert_data(
            connection=connection,
            new_table=new_table,
            rows=rows,
            max_workers=max_workers,
            batch_size=batch_size,
        )

        logger.info("Decrypt progress notes completed successfully.")
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Decrypt progress notes from staging schema and write to progressnotes_decryptfinal."
    )
    parser.add_argument(
        "--staging_schema",
        required=True,
        help="Staging schema (database) containing progressnotes table",
    )
    parser.add_argument("--mysql_host", default=os.environ.get("DB_HOST", "localhost"), help="MySQL host")
    parser.add_argument("--mysql_user", default=os.environ.get("DB_USER", ""), help="MySQL user")
    parser.add_argument("--mysql_pass", default=os.environ.get("DB_PASS", ""), help="MySQL password")
    parser.add_argument(
        "--table_name",
        default="progressnotes",
        help="Source table name (default: progressnotes)",
    )
    parser.add_argument(
        "--new_table_name",
        default="progressnotes_decryptfinal",
        help="Destination table name (default: {table_name}_decryptfinal)",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=10,
        help="Number of parallel workers for decryption (default: 10)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1000,
        help="Batch size for processing (default: 1000)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    args = parser.parse_args()
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    try:
        main(
            staging_schema=args.staging_schema,
            mysql_host=args.mysql_host,
            mysql_user=args.mysql_user,
            mysql_pass=args.mysql_pass,
            table_name=args.table_name,
            new_table_name=args.new_table_name,
            max_workers=args.max_workers,
            batch_size=args.batch_size,
        )
    except Exception as e:
        logger.exception("Decrypt progress notes failed: %s", e)
        sys.exit(1)
