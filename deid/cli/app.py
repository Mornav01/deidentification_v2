"""Typer CLI application — main entry point."""
from __future__ import annotations

import importlib.util

import typer

app = typer.Typer(
    name="deid",
    help="De-identification platform — remove PII/PHI from healthcare databases.",
    add_completion=False,
)


def _register_commands():
    from deid.cli.run import run_command
    from deid.cli.status import status_command

    app.command(name="run")(run_command)
    app.command(name="status")(status_command)

    if importlib.util.find_spec("deid.cli.cdc") is not None:
        from deid.cli.cdc import cdc_command
        app.command(name="cdc")(cdc_command)

    if importlib.util.find_spec("deid.cli.decrypt_notes") is not None:
        from deid.cli.decrypt_notes import decrypt_notes_command
        app.command(name="decrypt-notes")(decrypt_notes_command)


_register_commands()

if __name__ == "__main__":
    app()
