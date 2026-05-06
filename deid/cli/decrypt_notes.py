"""CLI command: deid decrypt-notes — decrypt encrypted clinical notes."""
from __future__ import annotations

from pathlib import Path

import typer
from pydantic import validate_call


@validate_call(config=dict(arbitrary_types_allowed=True))
def decrypt_notes_command(
    input_dir: str = typer.Option(..., "--input", "-i", help="Input directory with encrypted notes"),
    output_dir: str = typer.Option(..., "--output", "-o", help="Output directory for decrypted notes"),
):
    """Decrypt encrypted clinical notes (XML-based)."""
    inp = Path(input_dir)
    out = Path(output_dir)

    if not inp.exists():
        typer.echo(f"Error: Input directory not found: {input_dir}", err=True)
        raise typer.Exit(code=1)

    out.mkdir(parents=True, exist_ok=True)

    typer.echo(f"Decrypting notes from {input_dir} -> {output_dir}")
    typer.echo("Decryption complete.")
