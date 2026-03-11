"""Typer CLI application — main entry point."""
from __future__ import annotations

import importlib.util

import typer
from pydantic import validate_call

app = typer.Typer(
    name="deid",
    help="De-identification platform — remove PII/PHI from healthcare databases.",
    add_completion=False,
)


@validate_call(config=dict(arbitrary_types_allowed=True))
def _register_commands():
    from deid.cli.run import run_command
    from deid.cli.status import status_command
    from deid.cli.retry import retry_command
    from deid.cli.generate_config import generate_config_command
    from deid.cli.mapping import mapping_command
    from deid.cli.pii_table import pii_table_command

    app.command(name="run")(run_command)
    app.command(name="status")(status_command)
    app.command(name="retry")(retry_command)
    app.command(name="generate-config")(generate_config_command)
    app.command(name="mapping")(mapping_command)
    app.command(name="pii-table")(pii_table_command)

    if importlib.util.find_spec("deid.cli.cdc") is not None:
        from deid.cli.cdc import cdc_command
        app.command(name="cdc")(cdc_command)

    if importlib.util.find_spec("deid.cli.decrypt_notes") is not None:
        from deid.cli.decrypt_notes import decrypt_notes_command
        app.command(name="decrypt-notes")(decrypt_notes_command)


_register_commands()

if __name__ == "__main__":
    app()
