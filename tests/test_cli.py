"""Tests for Typer CLI commands."""
import pytest
from typer.testing import CliRunner

runner = CliRunner()


def test_cli_help():
    from deid.cli.app import app
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "status" in result.output


def test_run_missing_config():
    from deid.cli.app import app
    result = runner.invoke(app, ["run", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_status_missing_state_db():
    from deid.cli.app import app
    result = runner.invoke(app, ["status", "--state-db", "/nonexistent/state.db"])
    assert result.exit_code != 0
