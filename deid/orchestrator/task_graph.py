"""Build Celery Canvas task graphs from config."""
from __future__ import annotations

import math

from celery import group

from deid.config.schema import DeidConfig
from deid.config.task_models import DeidentifyTaskConfig
from pydantic import validate_call


@validate_call(config=dict(arbitrary_types_allowed=True))
def _rules_to_table_details(rules: dict[str, str]) -> dict:
    """Convert a flat {column: rule} dict to the TableDetailsForUI format.

    Columns with a rule are marked as PHI; the mask_value defaults to
    the column name uppercased (e.g. ``<<PATIENT_NAME>>``).
    """
    columns_details = []
    for col_name, rule in rules.items():
        columns_details.append({
            "column_name": col_name,
            "is_phi": True,
            "de_identification_rule": rule,
            "mask_value": col_name.upper(),
        })
    return {
        "columns_details": columns_details,
        "ignore_rows": {},
        "batch_size": 0,
        "reference_patient_id_column": None,
        "reference_enc_id_column": None,
        "reference_mapping": "",
    }


@validate_call(config=dict(arbitrary_types_allowed=True))
def _build_table_config(config: DeidConfig, table_name: str, rules: dict) -> dict:
    """Build a validated config dict that gets passed to each Celery task."""
    table_details = _rules_to_table_details(rules)
    mappings_conn_str = f"sqlite:///{config.mappings_db_path}"
    task_config = DeidentifyTaskConfig(
        table_name=table_name,
        source_conn_str=config.source_db.connection_string(),
        dest_conn_str=config.destination_db.connection_string(),
        mappings_db_path=config.mappings_db_path,
        batch_size=config.deidentification.batch_size,
        offset_days=config.deidentification.date_offset_days,
        redis_url=config.redis_url,
        table_details_for_ui=table_details,
        mapping_db_config={"connection_str": mappings_conn_str},
        run_config={
            "redis_url": config.redis_url,
            "log_verbosity": config.logging.log_verbosity.value,
            "failed_rows_db_path": config.failed_rows_db_path,
        },
    )
    return task_config.model_dump()


@validate_call(config=dict(arbitrary_types_allowed=True))
def build_task_graph(
    config: DeidConfig,
    table_row_counts: dict[str, int],
    table_id_ranges: dict[str, tuple[int, int]] | None = None,
    cache_paths: dict[str, str] | None = None,
) -> group:
    """Build a Celery group/chord graph for all tables."""
    assert config.tables, "config.tables must not be empty"

    from deid.tasks.deidentify import deidentify_table, deidentify_table_range

    tasks = []
    threshold = config.deidentification.large_table_threshold
    n_splits = config.deidentification.parallel_tasks_per_table

    for table_cfg in config.tables:
        tname = table_cfg.name
        row_count = table_row_counts.get(tname, 0)
        task_config = _build_table_config(config, tname, table_cfg.rules)

        if row_count > threshold and table_id_ranges and tname in table_id_ranges:
            # Inject cache_dir for range tasks.
            task_config["cache_dir"] = (cache_paths or {}).get(tname)

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
