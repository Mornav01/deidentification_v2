"""Build Celery Canvas task graphs from config."""
from __future__ import annotations

import math

from celery import group

from deid.config.schema import DeidConfig


def _build_table_config_dict(config: DeidConfig, table_name: str, rules: dict) -> dict:
    """Build the config dict that gets passed to each Celery task."""
    return {
        "table_name": table_name,
        "source_conn_str": config.source_db.connection_string(),
        "dest_conn_str": config.destination_db.connection_string(),
        "mappings_db_path": config.mappings_db_path,
        "batch_size": config.deidentification.batch_size,
        "offset_days": config.deidentification.date_offset_days,
        "redis_url": config.redis_url,
        "table_details_for_ui": rules,
        "pii_config": None,
        "pii_db_conn_str": None,
        "secondary_pii_configs": None,
        "mapping_db_config": None,
        "universal_tables_config": None,
        "run_config": None,
    }


def build_task_graph(
    config: DeidConfig,
    table_row_counts: dict[str, int],
    table_id_ranges: dict[str, tuple[int, int]] | None = None,
) -> group:
    """Build a Celery group/chord graph for all tables."""
    from deid.tasks.deidentify import deidentify_table, deidentify_table_range

    tasks = []
    threshold = config.deidentification.large_table_threshold
    n_splits = config.deidentification.parallel_tasks_per_table

    for table_cfg in config.tables or []:
        tname = table_cfg.name
        row_count = table_row_counts.get(tname, 0)
        task_config = _build_table_config_dict(config, tname, table_cfg.rules)

        if row_count > threshold and table_id_ranges and tname in table_id_ranges:
            min_id, max_id = table_id_ranges[tname]
            range_size = math.ceil((max_id - min_id + 1) / n_splits)
            range_tasks = []
            for i in range(n_splits):
                start = min_id + i * range_size
                end = min(min_id + (i + 1) * range_size - 1, max_id)
                range_tasks.append(
                    deidentify_table_range.s(task_config, start, end)
                )
            tasks.append(group(range_tasks))
        else:
            tasks.append(deidentify_table.s(task_config))

    return group(tasks)
