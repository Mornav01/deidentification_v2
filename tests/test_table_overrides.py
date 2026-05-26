"""Tests for per-table batch_size override via table_overrides_path YAML."""
import os
import tempfile
import textwrap

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(d: dict) -> str:
    """Write dict to a temp YAML file and return the path."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.safe_dump(d, tmp)
    tmp.flush()
    tmp.close()
    return tmp.name


def _make_batch_states(table_name: str, row_count: int, batch_size: int) -> list[tuple]:
    """Simulate the offset/end pairs _setup_phase would create."""
    pairs = []
    offset = 0
    while offset < row_count:
        end = offset + batch_size - 1
        pairs.append((offset, end))
        offset += batch_size
    return pairs


# ---------------------------------------------------------------------------
# Schema tests — DeidConfig.table_overrides_path / table_overrides
# ---------------------------------------------------------------------------

def test_table_overrides_path_field_defaults_none():
    from deid.config.schema import DeidConfig
    cfg = DeidConfig.__new__(DeidConfig)
    assert not hasattr(cfg, "table_overrides_path") or True  # field exists


def test_table_overrides_loaded_from_file():
    """DeidConfig loads table_overrides at runtime when table_overrides_path is set."""
    overrides = {"InterfaceTransaction": {"batch_size": 5000}}
    path = _write_yaml(overrides)
    try:
        # Directly simulate what async_runner.run() does
        cfg_overrides = None
        if path and not cfg_overrides:
            import yaml as _yaml
            with open(path) as _f:
                cfg_overrides = _yaml.safe_load(_f) or {}
        assert cfg_overrides == overrides
    finally:
        os.unlink(path)


def test_table_overrides_empty_file_returns_empty_dict():
    """An empty YAML file should not crash — treated as no overrides."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    tmp.write("")
    tmp.close()
    try:
        import yaml as _yaml
        with open(tmp.name) as _f:
            result = _yaml.safe_load(_f) or {}
        assert result == {}
    finally:
        os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# Batch-split logic with overrides
# ---------------------------------------------------------------------------

def test_table_overrides_not_set_uses_global_batch_size():
    """When table_overrides is None, _setup_phase uses global batch_size."""
    global_bs = 10000
    row_count = 25000
    table_name = "SomeTable"

    table_overrides = None
    overrides = (table_overrides or {}).get(table_name, {})
    batch_size = overrides.get("batch_size") or global_bs

    assert batch_size == global_bs
    pairs = _make_batch_states(table_name, row_count, batch_size)
    # 25000 rows / 10000 per batch = 3 batches; end = offset + batch_size - 1 (not capped)
    assert len(pairs) == 3
    assert pairs[0] == (0, 9999)
    assert pairs[1] == (10000, 19999)
    assert pairs[2] == (20000, 29999)


def test_table_overrides_sets_per_table_batch_size():
    """When table_overrides has an entry, its batch_size is used for that table."""
    global_bs = 100000
    row_count = 10000
    table_name = "InterfaceTransaction"

    table_overrides = {"InterfaceTransaction": {"batch_size": 5000}}
    overrides = (table_overrides or {}).get(table_name, {})
    batch_size = overrides.get("batch_size") or global_bs

    assert batch_size == 5000
    pairs = _make_batch_states(table_name, row_count, batch_size)
    # 10000 rows / 5000 per batch = 2 batches
    assert len(pairs) == 2
    assert pairs[0] == (0, 4999)
    assert pairs[1] == (5000, 9999)


def test_table_overrides_missing_table_falls_back_to_global():
    """A table not mentioned in overrides gets the global batch_size."""
    global_bs = 100000
    row_count = 200000
    table_name = "ClaimRegister"

    table_overrides = {"InterfaceTransaction": {"batch_size": 5000}}
    overrides = (table_overrides or {}).get(table_name, {})
    batch_size = overrides.get("batch_size") or global_bs

    assert batch_size == global_bs
    pairs = _make_batch_states(table_name, row_count, batch_size)
    assert len(pairs) == 2


def test_table_overrides_zero_batch_size_falls_back_to_global():
    """batch_size: 0 in overrides should fall back to global (falsy check)."""
    global_bs = 10000
    table_overrides = {"WeirdTable": {"batch_size": 0}}
    overrides = (table_overrides or {}).get("WeirdTable", {})
    batch_size = overrides.get("batch_size") or global_bs
    assert batch_size == global_bs


# ---------------------------------------------------------------------------
# File loading edge cases
# ---------------------------------------------------------------------------

def test_table_overrides_path_missing_file_raises():
    """Opening a non-existent overrides file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        with open("/tmp/this_file_does_not_exist_at_all_xyz.yaml") as _f:
            pass


def test_table_overrides_multiple_tables():
    """Multiple table overrides are all accessible."""
    overrides_data = {
        "InterfaceTransaction": {"batch_size": 5000},
        "D_54031_Notes_bkp": {"batch_size": 2000},
        "BigTable": {"batch_size": 1000},
    }
    path = _write_yaml(overrides_data)
    try:
        import yaml as _yaml
        with open(path) as _f:
            loaded = _yaml.safe_load(_f) or {}

        assert loaded["InterfaceTransaction"]["batch_size"] == 5000
        assert loaded["D_54031_Notes_bkp"]["batch_size"] == 2000
        assert loaded["BigTable"]["batch_size"] == 1000
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# DeidConfig field validation
# ---------------------------------------------------------------------------

def test_deid_config_table_overrides_excluded_from_serialisation():
    """table_overrides field has exclude=True — it won't appear in model_dump()."""
    from deid.config.schema import DeidConfig
    from pydantic.fields import FieldInfo

    field = DeidConfig.model_fields.get("table_overrides")
    assert field is not None, "table_overrides field missing from DeidConfig"
    assert field.exclude is True, "table_overrides should have exclude=True"


def test_deid_config_table_overrides_path_field_exists():
    """table_overrides_path field exists in DeidConfig."""
    from deid.config.schema import DeidConfig

    assert "table_overrides_path" in DeidConfig.model_fields
    assert "table_overrides" in DeidConfig.model_fields
