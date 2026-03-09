import zlib
import pytest
import polars as pl

from deid.clinical_bin_doc.extractor import (
    BINTYPEID_TO_EXT,
    XML_BIN_TYPE_IDS,
    BINARY_BIN_TYPE_IDS,
    decompress_blob,
    clean_xml,
)


class TestConstants:
    def test_bintypeid_to_ext_has_all_types(self):
        assert BINTYPEID_TO_EXT[1000] == "xml"
        assert BINTYPEID_TO_EXT[1004] == "xml"
        assert BINTYPEID_TO_EXT[1005] == "xml"
        assert BINTYPEID_TO_EXT[1016] == "xml"
        assert BINTYPEID_TO_EXT[1001] == "pdf"
        assert BINTYPEID_TO_EXT[1003] == "txt"
        assert BINTYPEID_TO_EXT[1007] == "tif"

    def test_xml_bin_type_ids(self):
        assert XML_BIN_TYPE_IDS == {1000, 1004, 1005, 1016}

    def test_binary_bin_type_ids(self):
        assert BINARY_BIN_TYPE_IDS == {1001, 1007}


class TestDecompressBlob:
    def test_zlib_compressed_data(self):
        original = b"<root><name>test</name></root>"
        compressed = zlib.compress(original)
        result = decompress_blob(compressed)
        assert result == original

    def test_uncompressed_data_returned_as_is(self):
        raw = b"not compressed data"
        result = decompress_blob(raw)
        assert result == raw

    def test_none_returns_none(self):
        assert decompress_blob(None) is None

    def test_empty_bytes_returns_empty(self):
        assert decompress_blob(b"") == b""


class TestCleanXml:
    def test_valid_xml_passes_through(self):
        xml = b"<root><name>test</name></root>"
        result = clean_xml(xml)
        assert result is not None
        assert "test" in result

    def test_control_chars_removed(self):
        xml = b"<root>\x00\x01\x08<name>test</name></root>"
        result = clean_xml(xml)
        assert result is not None
        assert "\x00" not in result
        assert "test" in result

    def test_broken_xml_recovered(self):
        xml = b"<root><unclosed>text</root>"
        result = clean_xml(xml)
        # lxml recover=True should handle this
        assert result is not None

    def test_invalid_xml_returns_none(self):
        result = clean_xml(b"")
        assert result is None
