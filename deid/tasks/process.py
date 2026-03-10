"""Process stage — joins mappings, de-identifies, writes processed Arrow IPC."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import polars as pl
import pyarrow.ipc as ipc
from celery import shared_task
from sqlalchemy.orm import Session

from deid.config.task_models import ProcessTaskConfig
from deid.core.dbPkg.dbhandler import NDDBHandler
from deid.core.ops_df.jointables import ReferenceMappingDataFrameJoiner
from deid.core.ops_df.utility import join_dataframes
from deid.core.process_df.base import DeIdentifier
from deid.core.process_df.main import (
    JoinMapping,
    PatientIdentifierResolver,
    _serialize_dict_values,
    get_key_phi_column_list,
)
from deid.core.process_df.rowhandler import InvalidRowHandler
from deid.models.base import create_state_engine
from deid.models.state import BatchState
from deid.staging import batch_fetched_path, batch_processed_path

logger = logging.getLogger("deid.tasks.process")


@shared_task(bind=True, name="deid.tasks.process.process_batch")
def process_batch(self, raw_config: dict):
    """Read fetched Arrow file, de-identify, write processed Arrow file."""
    config = ProcessTaskConfig(**raw_config)
    root = Path(config.staging_root)

    fetched = batch_fetched_path(root, config.table_name, config.start_id, config.end_id)
    processed = batch_processed_path(root, config.table_name, config.start_id, config.end_id)

    # 1. Read fetched Arrow file, preserving metadata
    reader = ipc.open_file(str(fetched))
    arrow_table = reader.read_all()
    file_metadata = arrow_table.schema.metadata or {}
    df = pl.from_arrow(arrow_table)

    if df.is_empty():
        fetched.unlink(missing_ok=True)
        _update_batch_status(config, "done")
        return {"table": config.table_name, "start_id": config.start_id,
                "end_id": config.end_id, "status": "done", "rows": 0}

    table_details = config.table_details
    key_phi_columns = get_key_phi_column_list(table_details.get("columns_details", []))

    # 2. Reference mapping resolution (needs source DB)
    source = NDDBHandler(config.source_conn_str, read_only=True)
    try:
        ref_joiner = ReferenceMappingDataFrameJoiner(source, df, table_details, key_phi_columns)
        df, key_phi_columns = ref_joiner.join_dataframe()
    finally:
        source.close()

    # 3. Mapping joins (needs mapping DB)
    mapping_obj = JoinMapping(df, key_phi_columns, config.mapping_db_config, config.table_name)
    try:
        distinct_eids = mapping_obj._get_distinct_encounterids()
        df_enc = mapping_obj._get_encounter_mapping(distinct_eids)
        if df_enc is not None and key_phi_columns[0]:
            df = join_dataframes(df, df_enc, left_on=key_phi_columns[0][0],
                                 right_on="encounter_id", how="left", right_suffix="",
                                 drop_right_join_column=True)

        distinct_pids = mapping_obj._get_distinct_patientids()
        df_pat = mapping_obj._get_patient_mapping(distinct_pids)
        if df_pat is not None and key_phi_columns[1]:
            df = join_dataframes(df, df_pat, left_on=key_phi_columns[1][0],
                                 right_on="patient_id", how="left",
                                 right_suffix="from_patient_mapping",
                                 drop_right_join_column=True)

        distinct_rpids = mapping_obj._get_distinct_referencepids()
        df_ref = mapping_obj._get_reference_pid_mapping(distinct_rpids)
        if df_ref is not None and key_phi_columns[2]:
            df = join_dataframes(df, df_ref, left_on=key_phi_columns[2][0],
                                 right_on="patient_id", right_suffix="from_referencepid_mapping",
                                 how="left", drop_right_join_column=True)

        distinct_aids = mapping_obj._get_distinct_appointmentids()
        df_apt = mapping_obj._get_appointment_mapping(distinct_aids)
        if df_apt is not None and key_phi_columns[3]:
            df = join_dataframes(df, df_apt, left_on=key_phi_columns[3][0],
                                 right_on="appointment_id", how="left",
                                 drop_right_join_column=True)
    finally:
        mapping_obj.close_connection()

    # 4. Resolve patient identifiers
    resolver = PatientIdentifierResolver(key_phi_columns, offset_days=config.offset_days)
    df = resolver.transform(df)

    # 5. Invalid row handling
    row_handler = InvalidRowHandler(db_name="", table_name=config.table_name)
    df = row_handler.handle(df)

    # 6. De-identification
    deidentifier = DeIdentifier(
        df=df,
        config=table_details.get("columns_details", []),
        pii_config=config.pii_config,
        pii_db_conn_str=config.pii_db_conn_str,
        secondary_pii_configs=config.secondary_pii_configs,
        key_phi_columns=key_phi_columns,
        offset_days=config.offset_days,
        run_config={**(config.run_config or {}), "table_name": config.table_name},
    )
    df = deidentifier.apply_rules()
    df = _serialize_dict_values(df)

    # 7. Write processed Arrow file with metadata carried forward
    tmp_path = processed.with_suffix(processed.suffix + ".tmp")
    processed.parent.mkdir(parents=True, exist_ok=True)
    arrow_out = df.to_arrow()
    arrow_out = arrow_out.replace_schema_metadata(file_metadata)
    with ipc.new_file(str(tmp_path), arrow_out.schema) as writer:
        writer.write_table(arrow_out)
    os.rename(tmp_path, processed)

    # 8. Delete fetched file
    fetched.unlink(missing_ok=True)

    # 9. Update BatchState: fetched -> processed
    _update_batch_status(config, "processed")

    # 10. Dispatch write_batch
    from deid.tasks.write import write_batch
    write_config = {
        "table_name": config.table_name,
        "start_id": config.start_id,
        "end_id": config.end_id,
        "staging_root": config.staging_root,
        "state_db_path": config.state_db_path,
        "redis_url": config.redis_url,
        "run_config": config.run_config,
        **{k: raw_config[k] for k in ("dest_conn_str", "id_column") if k in raw_config},
    }
    write_batch.apply_async(args=[write_config], queue="deid-write")

    logger.info("Processed %s batch %d-%d (%d rows)",
                config.table_name, config.start_id, config.end_id, df.height)

    return {"table": config.table_name, "start_id": config.start_id,
            "end_id": config.end_id, "status": "processed", "rows": df.height}


def _update_batch_status(config: ProcessTaskConfig, status: str):
    engine = create_state_engine(config.state_db_path)
    from deid.models.base import create_all_state_tables
    create_all_state_tables(engine)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
        ).first()
        if batch:
            batch.status = status
            session.commit()
    engine.dispose()
