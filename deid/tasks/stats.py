"""Celery tasks for stats generation."""
from __future__ import annotations

import logging

from celery import shared_task

logger = logging.getLogger("deid.tasks")


@shared_task(name="deid.tasks.stats.generate_table_stats")
def generate_table_stats(table_config: dict) -> dict:
    """Generate row count and size stats for a single table."""
    from deid.core.dbPkg.dbhandler import NDDBHandler

    handler = NDDBHandler(table_config["source_conn_str"])
    table_name = table_config["table_name"]
    row_count = handler.get_rows_count(table_name)
    return {"table": table_name, "row_count": row_count}
