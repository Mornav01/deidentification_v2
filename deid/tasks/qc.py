"""Celery tasks for quality control."""
from __future__ import annotations

import logging

from celery import shared_task

logger = logging.getLogger("deid.tasks")


@shared_task(bind=True, name="deid.tasks.qc.run_qc", max_retries=0)
def run_qc(self, qc_config: dict):
    """Run QC scanning on a de-identified table."""
    table_name = qc_config.get("table_name", "unknown")
    try:
        from deid.qc.scanner import DbScanner

        scanner = DbScanner(
            source_connection_string=qc_config["source_conn_str"],
            dest_connection_string=qc_config["dest_conn_str"],
            mapping_db_config=qc_config.get("mapping_db_config", {}),
            qc_config=qc_config.get("qc_settings", {}),
        )
        result = scanner.scan_table(
            table_name=table_name,
            table_config=qc_config["table_config"],
        )
        return {"table": table_name, "status": "completed", "result": result}
    except Exception as exc:
        logger.exception("QC failed for %s", table_name)
        return {"table": table_name, "status": "failed", "error": str(exc)}
