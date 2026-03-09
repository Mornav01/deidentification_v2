"""Celery tasks for de-identification."""
from __future__ import annotations

import json
import logging
import traceback

import redis as redis_lib
from celery import shared_task

from deid.config.task_models import DeidentifyTaskConfig, LogLevel
from deid.core.log_publisher import get_peak_memory_mb, make_log_record, publish_log
from deid.core.process_df.main import start_de_identification_for_table
from pydantic import validate_call

logger = logging.getLogger("deid.tasks")


@validate_call(config=dict(arbitrary_types_allowed=True))
def _publish_progress(redis_url: str, table_name: str, status: str, detail: str = ""):
    r = redis_lib.from_url(redis_url)
    r.publish("deid:progress", json.dumps({
        "table": table_name, "status": status, "detail": detail,
    }))


@shared_task(bind=True, name="deid.tasks.deidentify.deidentify_table", max_retries=1)
def deidentify_table(self, raw_config: dict):
    """De-identify a full table (single task, streaming batches)."""
    config = DeidentifyTaskConfig(**raw_config)
    assert config.table_name, "table_name is required"

    _publish_progress(config.redis_url, config.table_name, "started")
    publish_log(config.redis_url, make_log_record(
        LogLevel.INFO, config.table_name, "deidentify",
        "table started",
    ))

    try:
        result = start_de_identification_for_table(
            table_config=config.table_details_for_ui,
            source_conn_str=config.source_conn_str,
            dest_conn_str=config.dest_conn_str,
            mappings_db_path=config.mappings_db_path,
            batch_size=config.batch_size,
            offset_days=config.offset_days,
            pii_config=config.pii_config,
            pii_db_conn_str=config.pii_db_conn_str,
            secondary_pii_configs=config.secondary_pii_configs,
            mapping_db_config=config.mapping_db_config,
            universal_tables_config=config.universal_tables_config,
            run_config=config.run_config,
            table_name=config.table_name,
        )

        _publish_progress(config.redis_url, config.table_name, "completed")
        publish_log(config.redis_url, make_log_record(
            LogLevel.INFO, config.table_name, "deidentify",
            f"table completed — {result.get('batches_processed', 0)} batches",
            peak_memory_mb=get_peak_memory_mb(),
        ))
        return {"table": config.table_name, "status": "completed"}

    except Exception as exc:
        _publish_progress(config.redis_url, config.table_name, "failed", str(exc))
        publish_log(config.redis_url, make_log_record(
            LogLevel.ERROR, config.table_name, "deidentify",
            f"table failed: {exc}",
            error=traceback.format_exc(),
        ))
        raise


@shared_task(bind=True, name="deid.tasks.deidentify.deidentify_table_range", max_retries=1)
def deidentify_table_range(self, raw_config: dict, start_id: int, end_id: int):
    """De-identify a range of rows within a table (parallel split)."""
    config = DeidentifyTaskConfig(**raw_config)
    assert config.table_name, "table_name is required"
    assert start_id <= end_id, f"start_id ({start_id}) must be <= end_id ({end_id})"

    range_tag = f"range {start_id}-{end_id}"
    _publish_progress(config.redis_url, config.table_name, "started", range_tag)
    publish_log(config.redis_url, make_log_record(
        LogLevel.INFO, config.table_name, "deidentify",
        f"table started ({range_tag})",
        start_id=start_id, end_id=end_id,
    ))

    try:
        result = start_de_identification_for_table(
            table_config=config.table_details_for_ui,
            source_conn_str=config.source_conn_str,
            dest_conn_str=config.dest_conn_str,
            mappings_db_path=config.mappings_db_path,
            batch_size=config.batch_size,
            offset_days=config.offset_days,
            pii_config=config.pii_config,
            pii_db_conn_str=config.pii_db_conn_str,
            secondary_pii_configs=config.secondary_pii_configs,
            mapping_db_config=config.mapping_db_config,
            universal_tables_config=config.universal_tables_config,
            run_config=config.run_config,
            table_name=config.table_name,
            start_id=start_id,
            end_id=end_id,
        )

        _publish_progress(config.redis_url, config.table_name, "completed", range_tag)
        publish_log(config.redis_url, make_log_record(
            LogLevel.INFO, config.table_name, "deidentify",
            f"table completed ({range_tag}) — {result.get('batches_processed', 0)} batches",
            start_id=start_id, end_id=end_id,
            peak_memory_mb=get_peak_memory_mb(),
        ))
        return {"table": config.table_name, "range": [start_id, end_id], "status": "completed"}

    except Exception as exc:
        _publish_progress(config.redis_url, config.table_name, "failed", str(exc))
        publish_log(config.redis_url, make_log_record(
            LogLevel.ERROR, config.table_name, "deidentify",
            f"table failed ({range_tag}): {exc}",
            error=traceback.format_exc(),
            start_id=start_id, end_id=end_id,
        ))
        raise
