"""Tests for Arrow IPC cache read/write functions."""
import os
import tempfile

import polars as pl
import pytest


def _make_test_ipc_cache(tmp_path: str, num_batches: int = 3, rows_per_batch: int = 10):
    """Write test IPC batch files and return the cache directory."""
    cache_dir = os.path.join(tmp_path, "test_table")
    os.makedirs(cache_dir, exist_ok=True)
    all_rows = []
    for i in range(num_batches):
        start = i * rows_per_batch + 1
        ids = list(range(start, start + rows_per_batch))
        df = pl.DataFrame({
            "nd_auto_increment_id": ids,
            "name": [f"row_{x}" for x in ids],
            "value": [float(x) for x in ids],
        })
        df.write_ipc(os.path.join(cache_dir, f"batch_{i:05d}.arrow"))
        all_rows.extend(ids)
    return cache_dir, all_rows


class TestStreamFromIpcCache:
    def test_reads_all_rows_when_range_covers_everything(self, tmp_path):
        from deid.core.dbPkg.dbhandler import stream_from_ipc_cache

        cache_dir, all_ids = _make_test_ipc_cache(str(tmp_path))
        frames = list(stream_from_ipc_cache(
            cache_dir=cache_dir,
            start_id=min(all_ids),
            end_id=max(all_ids),
            id_column="nd_auto_increment_id",
        ))
        total_rows = sum(df.height for df in frames)
        assert total_rows == len(all_ids)

    def test_filters_to_range(self, tmp_path):
        from deid.core.dbPkg.dbhandler import stream_from_ipc_cache

        cache_dir, _ = _make_test_ipc_cache(str(tmp_path), num_batches=4, rows_per_batch=10)
        # IDs are 1..40.  Request range 11..20 (second batch only).
        frames = list(stream_from_ipc_cache(
            cache_dir=cache_dir,
            start_id=11,
            end_id=20,
            id_column="nd_auto_increment_id",
        ))
        total_rows = sum(df.height for df in frames)
        assert total_rows == 10
        all_ids = pl.concat(frames)["nd_auto_increment_id"].to_list()
        assert min(all_ids) == 11
        assert max(all_ids) == 20

    def test_skips_empty_batches(self, tmp_path):
        from deid.core.dbPkg.dbhandler import stream_from_ipc_cache

        cache_dir, _ = _make_test_ipc_cache(str(tmp_path), num_batches=4, rows_per_batch=10)
        # IDs are 1..40.  Request range 5..8 — only first batch has these.
        frames = list(stream_from_ipc_cache(
            cache_dir=cache_dir,
            start_id=5,
            end_id=8,
            id_column="nd_auto_increment_id",
        ))
        # Only 1 frame should be yielded (from the first batch), empty batches skipped.
        assert len(frames) == 1
        assert frames[0].height == 4

    def test_empty_range_yields_nothing(self, tmp_path):
        from deid.core.dbPkg.dbhandler import stream_from_ipc_cache

        cache_dir, _ = _make_test_ipc_cache(str(tmp_path))
        frames = list(stream_from_ipc_cache(
            cache_dir=cache_dir,
            start_id=9999,
            end_id=10000,
            id_column="nd_auto_increment_id",
        ))
        assert len(frames) == 0

    def test_preserves_all_columns(self, tmp_path):
        from deid.core.dbPkg.dbhandler import stream_from_ipc_cache

        cache_dir, all_ids = _make_test_ipc_cache(str(tmp_path))
        frames = list(stream_from_ipc_cache(
            cache_dir=cache_dir,
            start_id=1,
            end_id=5,
            id_column="nd_auto_increment_id",
        ))
        assert len(frames) == 1
        assert set(frames[0].columns) == {"nd_auto_increment_id", "name", "value"}


class TestDumpTableToIpcCache:
    def test_dump_creates_batch_files(self, tmp_path):
        from deid.core.dbPkg.dbhandler import dump_table_to_ipc_cache

        cache_dir = str(tmp_path / "cached_table")

        # Create a mock stream (simulates NDDBHandler.stream_table_as_dataframes)
        def mock_stream():
            for i in range(3):
                start = i * 5 + 1
                yield pl.DataFrame({
                    "nd_auto_increment_id": list(range(start, start + 5)),
                    "data": [f"val_{x}" for x in range(start, start + 5)],
                })

        result = dump_table_to_ipc_cache(mock_stream(), cache_dir)
        assert result is not None
        assert result["cache_dir"] == cache_dir
        assert result["batches"] == 3
        assert result["rows"] == 15

        # Should have 3 batch files
        files = sorted(os.listdir(cache_dir))
        assert files == ["batch_00000.arrow", "batch_00001.arrow", "batch_00002.arrow"]

        # Each file should have 5 rows
        for f in files:
            df = pl.read_ipc(os.path.join(cache_dir, f))
            assert df.height == 5

    def test_dump_empty_stream_returns_none(self, tmp_path):
        from deid.core.dbPkg.dbhandler import dump_table_to_ipc_cache

        cache_dir = str(tmp_path / "empty_table")

        def empty_stream():
            return
            yield  # make it a generator

        result = dump_table_to_ipc_cache(empty_stream(), cache_dir)
        assert result is None

    def test_roundtrip_dump_then_read(self, tmp_path):
        from deid.core.dbPkg.dbhandler import dump_table_to_ipc_cache, stream_from_ipc_cache

        cache_dir = str(tmp_path / "roundtrip")

        original_rows = []
        def mock_stream():
            for i in range(4):
                start = i * 10 + 1
                ids = list(range(start, start + 10))
                original_rows.extend(ids)
                yield pl.DataFrame({
                    "nd_auto_increment_id": ids,
                    "name": [f"row_{x}" for x in ids],
                })

        dump_table_to_ipc_cache(mock_stream(), cache_dir)

        # Read back range 15..25
        frames = list(stream_from_ipc_cache(cache_dir, start_id=15, end_id=25))
        result_ids = sorted(pl.concat(frames)["nd_auto_increment_id"].to_list())
        expected = [x for x in original_rows if 15 <= x <= 25]
        assert result_ids == expected
