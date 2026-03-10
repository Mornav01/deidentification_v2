import pytest
import yaml
from pathlib import Path


def _write_yaml(tmp_path: Path, content: dict) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(content))
    return p


def _minimal_config() -> dict:
    return {
        "source_db": {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_src",
            "username": "user",
            "password": "pass",
        },
        "destination_db": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_dest",
            "username": "user",
            "password": "pass",
        },
        "state_db_path": "./state.db",
        "mappings_db_path": "./mappings.db",
        "redis_url": "redis://localhost:6379/0",
        "deidentification": {
            "batch_size": 1000,
            "date_offset_days": 34,
            "patient_id_prefix": 10000000,
        },
        "tables": [
            {"name": "patients", "rules": {"patient_id": "PATIENT_ID", "name": "MASK"}}
        ],
        "mapping_tables": {
            "patient": {
                "source_column": "patient_id",
                "destination_column": "nd_patient_id",
            }
        },
        "phases": ["setup", "deidentify", "qc"],
        "workers": {"fetchers": 2, "processors": 4, "writers": 2, "max_retries": 1, "task_timeout": 3600},
        "qc": {"sample_size": 100, "scan_for_residual_pii": True},
    }


def test_load_valid_config(tmp_path):
    from deid.config.loader import load_config

    p = _write_yaml(tmp_path, _minimal_config())
    config = load_config(p)
    assert config.source_db.type == "mysql"
    assert config.destination_db.database == "test_dest"
    assert config.deidentification.batch_size == 1000
    assert len(config.tables) == 1
    assert config.tables[0].rules["patient_id"] == "PATIENT_ID"
    assert config.phases == ["setup", "deidentify", "qc"]


def test_env_var_interpolation(tmp_path, monkeypatch):
    from deid.config.loader import load_config

    monkeypatch.setenv("TEST_DB_PASS", "secret123")
    cfg = _minimal_config()
    cfg["source_db"]["password"] = "${TEST_DB_PASS}"
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.source_db.password == "secret123"


def test_missing_required_field(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    del cfg["source_db"]
    p = _write_yaml(tmp_path, cfg)
    with pytest.raises(Exception):
        load_config(p)


def test_invalid_db_type(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["source_db"]["type"] = "oracle"
    p = _write_yaml(tmp_path, cfg)
    with pytest.raises(Exception):
        load_config(p)


def test_rules_csv_alternative(tmp_path):
    from deid.config.loader import load_config

    # Write a minimal rules CSV
    csv_path = tmp_path / "rules.csv"
    csv_path.write_text(
        "table_name,column_name,data_type,rule\n"
        "patients,patient_id,INTEGER,PATIENT_ID\n"
        "patients,name,VARCHAR(100),MASK\n"
        "patients,age,INTEGER,\n"  # no rule → skipped
    )

    cfg = _minimal_config()
    del cfg["tables"]
    cfg["rules_csv"] = str(csv_path)
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.rules_csv == str(csv_path)
    assert len(config.tables) == 1
    assert config.tables[0].name == "patients"
    assert config.tables[0].rules == {"patient_id": "PATIENT_ID", "name": "MASK"}


def test_default_phases(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    del cfg["phases"]
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.phases == ["setup", "deidentify", "qc"]


def test_clinical_bin_doc_config_optional(tmp_path):
    """DeidConfig works without clinical_bin_doc section (backwards compat)."""
    from deid.config.loader import load_config

    p = _write_yaml(tmp_path, _minimal_config())
    config = load_config(p)
    assert config.clinical_bin_doc is None


def test_default_logging_settings(tmp_path):
    from deid.config.loader import load_config

    p = _write_yaml(tmp_path, _minimal_config())
    config = load_config(p)
    assert config.logging.log_dir == "./logs"
    assert config.logging.log_verbosity == "standard"


def test_custom_logging_settings(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["logging"] = {"log_dir": "/var/log/deid", "log_verbosity": "verbose"}
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.logging.log_dir == "/var/log/deid"
    assert config.logging.log_verbosity == "verbose"


def test_invalid_log_verbosity(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["logging"] = {"log_verbosity": "ultra"}
    p = _write_yaml(tmp_path, cfg)
    with pytest.raises(Exception):
        load_config(p)


def test_clinical_bin_doc_config_present(tmp_path):
    """DeidConfig parses clinical_bin_doc section when present."""
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["clinical_bin_doc"] = {
        "source_db": "mssql+pymssql://user:pass@host:1433/db",
        "dest_db": "mysql+pymysql://user:pass@host:3306/db",
    }
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.clinical_bin_doc is not None
    assert config.clinical_bin_doc.source_table == "ClinicalBin"
    assert config.clinical_bin_doc.metadata_table == "ClinicalDocuments"
    assert config.clinical_bin_doc.dest_table == "clinicalbin_xml_decrypt"
    assert config.clinical_bin_doc.processed_table == "clinicalbin_xml_processed"


def test_worker_max_tasks_per_child_default(tmp_path):
    from deid.config.loader import load_config

    p = _write_yaml(tmp_path, _minimal_config())
    config = load_config(p)
    assert config.workers.max_tasks_per_child == 1


def test_worker_max_tasks_per_child_override(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["workers"]["max_tasks_per_child"] = 5
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.workers.max_tasks_per_child == 5


def test_worker_settings_three_pools(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["workers"] = {
        "fetchers": 3,
        "processors": 6,
        "writers": 2,
        "max_retries": 1,
        "task_timeout": 3600,
    }
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.workers.fetchers == 3
    assert config.workers.processors == 6
    assert config.workers.writers == 2


def test_worker_settings_defaults(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["workers"] = {}
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.workers.fetchers == 2
    assert config.workers.processors == 4
    assert config.workers.writers == 2


def test_default_batch_size(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    del cfg["deidentification"]["batch_size"]
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.deidentification.batch_size == 1000


def test_deidentification_settings_ignores_unknown_fields(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["deidentification"]["parallel_tasks_per_table"] = 4
    cfg["deidentification"]["large_table_threshold"] = 500000
    cfg["deidentification"]["cache_concurrency"] = 4
    cfg["deidentification"]["cache_batch_size"] = 1000
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)  # should NOT raise
    assert config.deidentification.batch_size == 1000


def test_mappings_db_path_defaults_to_schema_name(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["source_db"]["database"] = "my_hospital"
    del cfg["mappings_db_path"]
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.mappings_db_path == "./my_hospital_mappings.db"


def test_mappings_db_path_explicit_overrides_default(tmp_path):
    from deid.config.loader import load_config

    cfg = _minimal_config()
    cfg["mappings_db_path"] = "./custom.db"
    p = _write_yaml(tmp_path, cfg)
    config = load_config(p)
    assert config.mappings_db_path == "./custom.db"


def test_importing_qc_builders_does_not_load_presidio():
    """Importing the QC builders package should NOT eagerly load AnalyzerEngine."""
    import sys
    mods_to_remove = [k for k in sys.modules if k.startswith("deid.qc.builders")]
    for m in mods_to_remove:
        del sys.modules[m]

    from deid.qc.builders import unstructured
    assert unstructured._analyzer is None
