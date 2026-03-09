"""Extract and decrypt clinical binary documents from MSSQL ClinicalBin."""
from __future__ import annotations

import re
import zlib
from typing import Iterator

import polars as pl
from lxml import etree
from sqlalchemy import text
from sqlalchemy.engine import Engine

from deid.core.logger import nd_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BINTYPEID_TO_EXT: dict[int, str] = {
    1000: "xml",
    1004: "xml",
    1005: "xml",
    1016: "xml",
    1001: "pdf",
    1003: "txt",
    1007: "tif",
}

XML_BIN_TYPE_IDS: set[int] = {k for k, v in BINTYPEID_TO_EXT.items() if v == "xml"}
BINARY_BIN_TYPE_IDS: set[int] = {1001, 1007}

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_ZLIB_PREFIXES = (b"\x78\x9c", b"\x78\x01", b"\x78\xda")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def decompress_blob(data: bytes | None) -> bytes | None:
    """Attempt zlib decompression; return raw bytes if not compressed."""
    if data is None or len(data) == 0:
        return data
    if any(data.startswith(prefix) for prefix in _ZLIB_PREFIXES):
        try:
            return zlib.decompress(data)
        except zlib.error:
            pass
    return data


def clean_xml(raw: bytes) -> str | None:
    """Decode, strip control chars, and validate XML via lxml recovery parser.

    Returns cleaned XML string or None if parsing fails entirely.
    """
    if not raw:
        return None
    xml_string = raw.decode("utf-8", errors="ignore")
    xml_string = _CONTROL_CHAR_RE.sub("", xml_string)
    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_string.encode("utf-8"), parser)
        if root is None:
            return None
        return etree.tostring(root, encoding="utf-8").decode("utf-8")
    except Exception:
        return None


def resolve_bin_type_ids(bin_type: str) -> set[int]:
    """Map CLI --bin-type flag value to a set of BinTypeIDs."""
    mapping = {
        "xml": XML_BIN_TYPE_IDS,
        "pdf": {1001},
        "tiff": {1007},
        "txt": {1003},
        "all": set(BINTYPEID_TO_EXT.keys()),
    }
    ids = mapping.get(bin_type.lower())
    if ids is None:
        raise ValueError(f"Unknown bin_type: {bin_type!r}. Valid: {list(mapping.keys())}")
    return ids


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------
class ClinicalBinExtractor:
    """Streams batches from MSSQL ClinicalBin + ClinicalDocuments."""

    def __init__(self, engine: Engine, source_table: str = "ClinicalBin",
                 metadata_table: str = "ClinicalDocuments"):
        self.engine = engine
        self.source_table = source_table
        self.metadata_table = metadata_table

    def _build_query(
        self,
        bin_type_ids: set[int],
        last_doc_id: int,
        batch_size: int,
        patient_ids: set[int] | None,
        after_date: str | None,
        metadata_db: str | None,
    ) -> str:
        """Build the JOIN query with filters."""
        meta_ref = f"[{metadata_db}].[dbo].[{self.metadata_table}]" if metadata_db else f"[{self.metadata_table}]"

        placeholders = ", ".join(str(i) for i in bin_type_ids)
        where_clauses = [
            f"c.BinTypeID IN ({placeholders})",
            f"c.DocumentID > {last_doc_id}",
        ]
        if patient_ids:
            pid_list = ", ".join(str(p) for p in patient_ids)
            where_clauses.append(f"cd.PatientID IN ({pid_list})")
        if after_date:
            where_clauses.append(f"cd.Created > '{after_date}'")

        where = " AND ".join(where_clauses)
        return (
            f"SELECT c.DocumentID, c.SequenceNumber, c.BinTypeID, c.DocImage, "
            f"cd.PatientID, cd.VisitID, cd.DocTypeID, cd.DocName, "
            f"cd.DocDescription, cd.Created, cd.LastModified "
            f"FROM [{self.source_table}] AS c "
            f"INNER JOIN {meta_ref} AS cd ON c.DocumentID = cd.DocumentID "
            f"WHERE {where} "
            f"ORDER BY c.DocumentID "
            f"OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"
        )

    def stream_batches(
        self,
        bin_type_ids: set[int],
        batch_size: int = 10000,
        patient_ids: set[int] | None = None,
        after_date: str | None = None,
        metadata_db: str | None = None,
    ) -> Iterator[pl.DataFrame]:
        """Yield Polars DataFrames of decrypted/decompressed records.

        Uses ID-based windowing (WHERE DocumentID > last_seen) instead of
        OFFSET/LIMIT for efficient pagination over large tables.
        """
        is_xml = bin_type_ids.issubset(XML_BIN_TYPE_IDS)
        last_doc_id = 0

        while True:
            query_str = self._build_query(
                bin_type_ids, last_doc_id, batch_size, patient_ids, after_date, metadata_db,
            )
            with self.engine.connect() as conn:
                result = conn.execute(text(query_str))
                rows = result.fetchall()
                columns = list(result.keys())

            if not rows:
                break

            records = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                doc_id = row_dict["DocumentID"]
                last_doc_id = max(last_doc_id, doc_id)
                binary_data = row_dict.pop("DocImage")

                if is_xml:
                    decompressed = decompress_blob(binary_data)
                    xml_content = clean_xml(decompressed)
                    if xml_content is None:
                        nd_logger.warning(
                            f"[ClinicalBinDoc] Skipping DocumentID={doc_id}: "
                            f"XML parse failed after decompression"
                        )
                        continue
                    row_dict["DocContent"] = xml_content
                else:
                    row_dict["DocContent"] = binary_data

                records.append(row_dict)

            if records:
                yield pl.DataFrame(records, infer_schema_length=len(records))

            nd_logger.info(
                f"[ClinicalBinDoc] Batch complete: {len(records)} records "
                f"(last DocumentID={last_doc_id})"
            )

            if len(rows) < batch_size:
                break
