"""Typer CLI application — main entry point."""
from __future__ import annotations

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

    try:
        from deid.cli.cdc import cdc_command
        app.command(name="cdc")(cdc_command)
    except ImportError:
        pass

    try:
        from deid.cli.decrypt_notes import decrypt_notes_command
        app.command(name="decrypt-notes")(decrypt_notes_command)
    except ImportError:
        pass


_register_commands()

if __name__ == "__main__":
    app()
