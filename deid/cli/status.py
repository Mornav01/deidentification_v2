"""CLI command: deid status — check run progress."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from pydantic import validate_call


@validate_call(config=dict(arbitrary_types_allowed=True))
def status_command(
    state_db: str = typer.Option("./state.db", "--state-db", help="Path to state.db"),
    config_key: Optional[str] = typer.Option(None, "--config-key", "-k", help="Filter by config_key (e.g. historical, incremental)"),
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

        if config_key:
            # Show only tables for the specified config_key
            tables = session.query(TableState).filter_by(config_key=config_key).all()
            typer.echo(f"\nconfig_key: {config_key} — Tables ({len(tables)} total):")
            by_status = {}
            for t in tables:
                by_status.setdefault(t.status, []).append(t)
            for status, ts_list in sorted(by_status.items()):
                typer.echo(f"  {status}: {len(ts_list)}")
                if status == "failed":
                    for ts in ts_list:
                        typer.echo(f"    - {ts.table_name}: {ts.failure_remarks or 'no details'}")
        else:
            # Group all tables by config_key
            all_tables = session.query(TableState).all()
            by_key: dict = {}
            for t in all_tables:
                by_key.setdefault(t.config_key, []).append(t)

            typer.echo(f"\nTotal tables across all config_keys: {len(all_tables)}")
            for ck, ts_list in sorted(by_key.items()):
                typer.echo(f"\n  config_key: {ck} ({len(ts_list)} tables)")
                by_status: dict = {}
                for t in ts_list:
                    by_status.setdefault(t.status, []).append(t)
                for status, group in sorted(by_status.items()):
                    typer.echo(f"    {status}: {len(group)}")
                    if status == "failed":
                        for ts in group:
                            typer.echo(f"      - {ts.table_name}: {ts.failure_remarks or 'no details'}")
