"""Celery tasks for de-identification."""
from __future__ import annotations

import json
import logging

import redis as redis_lib
from celery import shared_task

logger = logging.getLogger("deid.tasks")


def _publish_progress(redis_url: str, table_name: str, status: str, detail: str = ""):
    try:
        r = redis_lib.from_url(redis_url)
        r.publish("deid:progress", json.dumps({
            "table": table_name, "status": status, "detail": detail,
        }))
    except Exception:
        logger.warning("Failed to publish progress event for %s", table_name)


@shared_task(bind=True, name="deid.tasks.deidentify.deidentify_table", max_retries=1)
def deidentify_table(self, table_config: dict):
    """De-identify a full table (single task, streaming batches)."""
    redis_url = table_config.get("redis_url", "")
    table_name = table_config.get("table_name", "unknown")
    _publish_progress(redis_url, table_name, "started")

    try:
        from deid.core.process_df.main import start_de_identification_for_table

        start_de_identification_for_table(
            table_config=table_config.get("table_details_for_ui"),
            source_conn_str=table_config["source_conn_str"],
            dest_conn_str=table_config["dest_conn_str"],
            mappings_db_path=table_config.get("mappings_db_path", ""),
            batch_size=table_config.get("batch_size", 100000),
            offset_days=table_config.get("offset_days", 34),
            pii_config=table_config.get("pii_config"),
            pii_db_conn_str=table_config.get("pii_db_conn_str"),
            secondary_pii_configs=table_config.get("secondary_pii_configs"),
            mapping_db_config=table_config.get("mapping_db_config"),
            universal_tables_config=table_config.get("universal_tables_config"),
            run_config=table_config.get("run_config"),
            table_name=table_name,
        )
        _publish_progress(redis_url, table_name, "completed")
        return {"table": table_name, "status": "completed"}
    except Exception as exc:
        _publish_progress(redis_url, table_name, "failed", str(exc))
        raise self.retry(exc=exc)


@shared_task(bind=True, name="deid.tasks.deidentify.deidentify_table_range", max_retries=1)
def deidentify_table_range(self, table_config: dict, start_id: int, end_id: int):
    """De-identify a range of rows within a table (parallel split)."""
    redis_url = table_config.get("redis_url", "")
    table_name = table_config.get("table_name", "unknown")
    _publish_progress(redis_url, table_name, "started", f"range {start_id}-{end_id}")

    try:
        from deid.core.process_df.main import start_de_identification_for_table

        start_de_identification_for_table(
            table_config=table_config.get("table_details_for_ui"),
            source_conn_str=table_config["source_conn_str"],
            dest_conn_str=table_config["dest_conn_str"],
            mappings_db_path=table_config.get("mappings_db_path", ""),
            batch_size=table_config.get("batch_size", 100000),
            offset_days=table_config.get("offset_days", 34),
            pii_config=table_config.get("pii_config"),
            pii_db_conn_str=table_config.get("pii_db_conn_str"),
            secondary_pii_configs=table_config.get("secondary_pii_configs"),
            mapping_db_config=table_config.get("mapping_db_config"),
            universal_tables_config=table_config.get("universal_tables_config"),
            run_config=table_config.get("run_config"),
            table_name=table_name,
            start_id=start_id,
            end_id=end_id,
        )
        _publish_progress(redis_url, table_name, "completed", f"range {start_id}-{end_id}")
        return {"table": table_name, "range": [start_id, end_id], "status": "completed"}
    except Exception as exc:
        _publish_progress(redis_url, table_name, "failed", str(exc))
        raise self.retry(exc=exc)
