"""Tests for Typer CLI commands."""
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

runner = CliRunner()


def test_cli_help():
    from deid.cli.app import app
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "status" in result.output
    assert "mapping" in result.output
    assert "pii-table" in result.output
    assert "generate-config" in result.output


def test_run_missing_config():
    from deid.cli.app import app
    result = runner.invoke(app, ["run", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_status_missing_state_db():
    from deid.cli.app import app
    result = runner.invoke(app, ["status", "--state-db", "/nonexistent/state.db"])
    assert result.exit_code != 0


def test_retry_command_registers():
    """The retry command should be registered in the app."""
    from deid.cli.app import app
    command_names = [cmd.name for cmd in app.registered_commands]
    assert "retry" in command_names


def test_mapping_command_registers():
    from deid.cli.app import app
    command_names = [cmd.name for cmd in app.registered_commands]
    assert "mapping" in command_names


def test_pii_table_command_registers():
    from deid.cli.app import app
    command_names = [cmd.name for cmd in app.registered_commands]
    assert "pii-table" in command_names


def test_generate_config_command_registers():
    from deid.cli.app import app
    command_names = [cmd.name for cmd in app.registered_commands]
    assert "generate-config" in command_names


def test_mapping_missing_config():
    from deid.cli.app import app
    result = runner.invoke(app, ["mapping", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_pii_table_missing_config():
    from deid.cli.app import app
    result = runner.invoke(app, ["pii-table", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_generate_config_missing_config():
    from deid.cli.app import app
    result = runner.invoke(app, ["generate-config", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_retry_reads_failures_file(tmp_path):
    """retry should parse a failures JSONL file correctly."""
    from deid.cli.retry import _load_failures

    failures_file = tmp_path / "failures.jsonl"
    failures_file.write_text(
        json.dumps({"table": "patients", "start_id": 4001, "end_id": 5000, "batch": 5, "error": "timeout", "timestamp": "2026-03-09T14:30:07Z", "task_type": "range"}) + "\n"
        + json.dumps({"table": "encounters", "start_id": None, "end_id": None, "batch": None, "error": "OOM", "timestamp": "2026-03-09T14:31:02Z", "task_type": "full"}) + "\n"
    )

    failures = _load_failures(str(failures_file))
    assert len(failures) == 2
    assert failures[0].table == "patients"
    assert failures[0].task_type == "range"
    assert failures[1].table == "encounters"
    assert failures[1].task_type == "full"
