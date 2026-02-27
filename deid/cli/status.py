"""CLI command: deid status — check run progress."""
from __future__ import annotations

from pathlib import Path

import typer


def status_command(
    state_db: str = typer.Option("./state.db", "--state-db", help="Path to state.db"),
):
    """Show de-identification run status."""
    if not Path(state_db).exists():
        typer.echo(f"Error: State DB not found: {state_db}", err=True)
        raise typer.Exit(code=1)

    from deid.models.base import create_state_engine
    from deid.models.state import RunLog, TableState
    from sqlalchemy.orm import Session

    engine = create_state_engine(state_db)

    with Session(engine) as session:
        run_log = session.query(RunLog).order_by(RunLog.id.desc()).first()
        if not run_log:
            typer.echo("No runs found.")
            return

        typer.echo(f"Run #{run_log.id}: {run_log.status} (phases: {run_log.phases})")
        typer.echo(f"  Started: {run_log.started_at}")
        if run_log.completed_at:
            typer.echo(f"  Completed: {run_log.completed_at}")

        tables = session.query(TableState).all()
        by_status = {}
        for t in tables:
            by_status.setdefault(t.status, []).append(t.table_name)

        typer.echo(f"\nTables ({len(tables)} total):")
        for status, names in sorted(by_status.items()):
            typer.echo(f"  {status}: {len(names)}")
            if status == "failed":
                for name in names:
                    ts = session.query(TableState).filter_by(table_name=name).first()
                    typer.echo(f"    - {name}: {ts.failure_remarks or 'no details'}")
