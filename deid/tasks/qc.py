"""Celery tasks for quality control."""
from __future__ import annotations

import json
import logging

from celery import shared_task

from deid.config.task_models import QCTaskConfig

logger = logging.getLogger("deid.tasks")


def _persist_qc_result(db_path: str, result: dict):
    """Write a single table's QC result to qc_results.db."""
    if not db_path:
        return
    from deid.models.base import create_qc_results_engine, create_all_qc_results_tables
    from deid.models.qc_results import QCTableResult
    from sqlalchemy.orm import Session

    engine = create_qc_results_engine(db_path)
    create_all_qc_results_tables(engine)

    final = result.get("final_qc_result", {})
    with Session(engine) as session:
        session.add(QCTableResult(
            table_name=result.get("table_name", ""),
            is_qc_passed=final.get("is_qc_passed", False),
            reason=final.get("reason", ""),
            source_rows_count=result.get("source_rows_count", 0),
            dest_rows_count=result.get("dest_rows_count", 0),
            sample_size=result.get("unstruct_sample_size", result.get("sample_size", 0)),
            columns_result=json.dumps(result.get("ColumnsQCResult", {}), default=str),
        ))
        session.commit()
    engine.dispose()
    logger.info("Persisted QC result for '%s' to %s", result.get("table_name"), db_path)


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

    _persist_qc_result(config.qc_results_db_url, result)

    return {"table": config.table_name, "status": "completed", "result": result}
