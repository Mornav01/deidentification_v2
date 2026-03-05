"""Celery tasks for stats generation."""
from __future__ import annotations

import logging

from celery import shared_task

from deid.config.task_models import StatsTaskConfig

logger = logging.getLogger("deid.tasks")


@shared_task(name="deid.tasks.stats.generate_table_stats")
def generate_table_stats(raw_config: dict) -> dict:
    """Generate row count and size stats for a single table."""
    config = StatsTaskConfig(**raw_config)
    assert config.table_name, "table_name is required"

    from deid.core.dbPkg.dbhandler import NDDBHandler

    handler = NDDBHandler(config.source_conn_str, read_only=True)
    row_count = handler.get_rows_count(config.table_name)
    return {"table": config.table_name, "row_count": row_count}
