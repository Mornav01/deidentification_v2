"""Celery tasks for quality control."""
from __future__ import annotations

import logging

from celery import shared_task

from deid.config.task_models import QCTaskConfig

logger = logging.getLogger("deid.tasks")


@shared_task(bind=True, name="deid.tasks.qc.run_qc", max_retries=0)
def run_qc(self, raw_config: dict):
    """Run QC scanning on a de-identified table."""
    config = QCTaskConfig(**raw_config)
    assert config.table_name, "table_name is required"

    from deid.qc.scanner import DbScanner

    scanner = DbScanner(
        source_connection_string=config.source_conn_str,
        dest_connection_string=config.dest_conn_str,
        mapping_db_config=config.mapping_db_config,
        qc_config=config.qc_settings,
    )
    result = scanner.scan_table(
        table_name=config.table_name,
        table_config=config.table_config,
    )
    return {"table": config.table_name, "status": "completed", "result": result}
