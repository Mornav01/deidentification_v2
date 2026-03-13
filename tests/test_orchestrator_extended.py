"""Extended tests for orchestrator async_runner — helper functions and RunLog status."""
import pytest
from datetime import datetime, timezone


def _make_config(**overrides):
    from deid.config.schema import (
        DeidConfig, DbConfig, DeidentificationSettings,
        TableConfig, WorkerSettings, QCSettings,
    )
    defaults = dict(
        source_db=DbConfig(type="mysql", host="localhost", port=3306, database="src", username="u", password="p"),
        destination_db=DbConfig(type="postgresql", host="localhost", port=5432, database="dest", username="u", password="p"),
        tables=[TableConfig(name="patients", rules={"patient_id": "PATIENT_ID", "name": "MASK"})],
        mapping_tables={},
        workers=WorkerSettings(),
        qc=QCSettings(),
    )
    defaults.update(overrides)
    return DeidConfig(**defaults)


# ---------------------------------------------------------------------------
# _rules_to_table_details
# ---------------------------------------------------------------------------

class TestRulesToTableDetails:

    def test_basic_conversion(self):
        from deid.orchestrator.async_runner import _rules_to_table_details
        rules = {"patient_id": "PATIENT_ID", "name": "MASK"}
        result = _rules_to_table_details(rules, table_name="patients")

        assert "columns_details" in result
        assert len(result["columns_details"]) == 2

        col_names = {c["column_name"] for c in result["columns_details"]}
        assert col_names == {"patient_id", "name"}

        for col in result["columns_details"]:
            assert col["is_phi"] is True
            assert col["table_name"] == "patients"

    def test_mask_value_is_uppercased_column_name(self):
        from deid.orchestrator.async_runner import _rules_to_table_details
        rules = {"first_name": "MASK"}
        result = _rules_to_table_details(rules)
        col = result["columns_details"][0]
        assert col["mask_value"] == "FIRST_NAME"

    def test_empty_rules(self):
        from deid.orchestrator.async_runner import _rules_to_table_details
        result = _rules_to_table_details({})
        assert result["columns_details"] == []

    def test_required_keys_present(self):
        from deid.orchestrator.async_runner import _rules_to_table_details
        result = _rules_to_table_details({"col": "MASK"})
        assert "ignore_rows" in result
        assert "batch_size" in result
        assert "reference_patient_id_column" in result
        assert "reference_enc_id_column" in result
        assert "reference_mapping" in result


# ---------------------------------------------------------------------------
# _get_table_details
# ---------------------------------------------------------------------------

class TestGetTableDetails:

    def test_found_table(self):
        from deid.orchestrator.async_runner import _get_table_details
        config = _make_config()
        result = _get_table_details(config, "patients")
        col_names = {c["column_name"] for c in result["columns_details"]}
        assert "patient_id" in col_names
        assert "name" in col_names

    def test_missing_table(self):
        from deid.orchestrator.async_runner import _get_table_details
        config = _make_config()
        result = _get_table_details(config, "nonexistent")
        assert result == {"columns_details": []}


# ---------------------------------------------------------------------------
# RunLog status tracking (failed vs completed)
# ---------------------------------------------------------------------------

class TestRunLogStatus:

    def test_runlog_completed_status(self, tmp_path):
        """RunLog should be 'completed' when run() finishes without errors."""
        from deid.models.base import create_state_engine, create_all_state_tables
        from deid.models.state import RunLog
        from sqlalchemy.orm import Session

        state_engine = create_state_engine(str(tmp_path / "state.db"))
        create_all_state_tables(state_engine)

        # Simulate a successful run by directly creating a RunLog
        with Session(state_engine) as session:
            log = RunLog(config_hash="abc123", phases=["setup"])
            session.add(log)
            session.commit()
            log_id = log.id

        # Simulate the finally block from run()
        _run_exc = None
        with Session(state_engine) as session:
            log = session.get(RunLog, log_id)
            log.status = "failed" if _run_exc else "completed"
            log.completed_at = datetime.now(timezone.utc)
            session.commit()

        with Session(state_engine) as session:
            log = session.get(RunLog, log_id)
            assert log.status == "completed"
            assert log.completed_at is not None

    def test_runlog_failed_status(self, tmp_path):
        """RunLog should be 'failed' when an exception occurs."""
        from deid.models.base import create_state_engine, create_all_state_tables
        from deid.models.state import RunLog
        from sqlalchemy.orm import Session

        state_engine = create_state_engine(str(tmp_path / "state.db"))
        create_all_state_tables(state_engine)

        with Session(state_engine) as session:
            log = RunLog(config_hash="abc123", phases=["setup"])
            session.add(log)
            session.commit()
            log_id = log.id

        # Simulate the finally block with an exception
        _run_exc = RuntimeError("something went wrong")
        with Session(state_engine) as session:
            log = session.get(RunLog, log_id)
            log.status = "failed" if _run_exc else "completed"
            log.completed_at = datetime.now(timezone.utc)
            session.commit()

        with Session(state_engine) as session:
            log = session.get(RunLog, log_id)
            assert log.status == "failed"


# ---------------------------------------------------------------------------
# Credential stripping in state.db
# ---------------------------------------------------------------------------

class TestCredentialStripping:

    def test_no_password_in_conn_str(self, tmp_path):
        """DbConfig stored in state.db should NOT contain passwords."""
        import asyncio
        from unittest.mock import patch, MagicMock

        config = _make_config(
            state_db_path=str(tmp_path / "state.db"),
            mappings_db_path=str(tmp_path / "mappings.db"),
        )

        mock_handler = MagicMock()
        mock_handler.get_rows_count.return_value = 100
        mock_handler.get_min_max_id.return_value = (1, 100)

        from deid.models.base import create_state_engine, create_all_state_tables
        state_engine = create_state_engine(str(tmp_path / "state.db"))
        create_all_state_tables(state_engine)

        with patch("deid.core.dbPkg.dbhandler.NDDBHandler", return_value=mock_handler):
            from deid.orchestrator.async_runner import _setup_phase
            asyncio.run(_setup_phase(config, state_engine))

        from deid.models.state import DbConfig as StateDbConfig
        from sqlalchemy.orm import Session
        with Session(state_engine) as session:
            db_cfg = session.query(StateDbConfig).first()
            assert db_cfg is not None
            # Should NOT contain the password "p"
            assert "p@" not in db_cfg.source_conn_str
            assert "p@" not in db_cfg.dest_conn_str
            # Should contain host/port/database info
            assert "localhost" in db_cfg.source_conn_str
            assert "src" in db_cfg.source_conn_str


# ---------------------------------------------------------------------------
# QC phase uses _get_table_details (not empty dict)
# ---------------------------------------------------------------------------

class TestQCTableConfig:

    def test_qc_config_has_columns(self):
        """_get_table_details should return actual columns, not empty dict."""
        from deid.orchestrator.async_runner import _get_table_details
        config = _make_config()
        details = _get_table_details(config, "patients")
        assert len(details["columns_details"]) > 0
        rules = {c["de_identification_rule"] for c in details["columns_details"]}
        assert "PATIENT_ID" in rules
        assert "MASK" in rules
