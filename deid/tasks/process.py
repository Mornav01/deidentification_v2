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

from deid.config.task_models import LogLevel, ProcessTaskConfig
from deid.core.dbPkg.dbhandler import NDDBHandler
from deid.core.log_publisher import get_peak_memory_mb, make_log_record, publish_log
from deid.core.ops_df.jointables import ReferenceMappingDataFrameJoiner
from deid.core.ops_df.utility import join_dataframes
from deid.core.process_df.base import DeIdentifier
from deid.core.process_df.main import (
    JoinMapping,
    PatientIdentifierResolver,
    _serialize_dict_values,
    get_key_phi_column_list,
)
from deid.tasks.celery_app import get_preloaded_data
from deid.core.process_df.rowhandler import InvalidRowHandler
from deid.models.base import get_cached_state_engine
from deid.models.state import BatchState
from deid.staging import batch_fetched_path, batch_processed_path

logger = logging.getLogger("deid.tasks.process")


def _publish(config: ProcessTaskConfig, level: LogLevel, phase: str, message: str, **kwargs):
    if config.redis_url:
        publish_log(config.redis_url, make_log_record(level, config.table_name, phase, message, **kwargs))


@shared_task(bind=True, name="deid.tasks.process.process_batch")
def process_batch(self, raw_config: dict):
    """Read fetched Arrow file, de-identify, write processed Arrow file."""
    config = ProcessTaskConfig(**raw_config)
    batch_tag = f"{config.start_id}-{config.end_id}"
    import time
    t0 = time.monotonic()
    try:
        result = _process_batch_inner(config, raw_config)
        duration_ms = int((time.monotonic() - t0) * 1000)
        _publish(config, LogLevel.INFO, "process",
                 f"batch {batch_tag} processed",
                 batch=config.start_id, rows_in_batch=result.get("rows", 0),
                 rows_succeeded=result.get("rows", 0),
                 start_id=config.start_id, end_id=config.end_id,
                 duration_ms=duration_ms, peak_memory_mb=get_peak_memory_mb())
        return result
    except Exception as exc:
        # Reset batch to pending or mark permanently failed after max retries
        try:
            from deid.tasks.batch_utils import reset_or_fail_batch
            max_retries = (config.run_config or {}).get("max_batch_retries", 3)
            new_status = reset_or_fail_batch(
                get_cached_state_engine(config.state_db_url),
                config.table_name, config.start_id, config.end_id,
                config.config_key, max_retries, f"{type(exc).__name__}: {exc}",
            )
            if new_status == "failed":
                logger.error(
                    "Batch %s permanently failed after %d retries: %s",
                    batch_tag, max_retries, exc,
                )
        except Exception:
            logger.warning("Could not reset batch %s after failure", batch_tag)
        _publish(config, LogLevel.ERROR, "process",
                 f"batch {batch_tag} failed: {exc}",
                 start_id=config.start_id, end_id=config.end_id,
                 error=f"{type(exc).__name__}: {exc}")
        raise


def _process_batch_inner(config: ProcessTaskConfig, raw_config: dict):
    # Idempotency guard: skip if already processed or beyond
    engine = get_cached_state_engine(config.state_db_url)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
            config_key=config.config_key,
        ).first()
        if batch and batch.status in ("processed", "done"):
            logger.info("Skipping already-%s batch %d-%d (idempotency guard)",
                        batch.status, config.start_id, config.end_id)
            return {"table": config.table_name, "start_id": config.start_id,
                    "end_id": config.end_id, "status": batch.status, "rows": 0}

    root = Path(config.staging_root)

    fetched = batch_fetched_path(root, config.table_name, config.start_id, config.end_id, config.config_key)
    processed = batch_processed_path(root, config.table_name, config.start_id, config.end_id, config.config_key)

    # 1. Read fetched Arrow file, preserving metadata
    reader = ipc.open_file(str(fetched))
    arrow_table = reader.read_all()
    file_metadata = arrow_table.schema.metadata or {}
    df = pl.from_arrow(arrow_table)
    df = df.rename({c: c.lower() for c in df.columns})

    if df.is_empty():
        fetched.unlink(missing_ok=True)
        _update_batch_status(config, "done")
        return {"table": config.table_name, "start_id": config.start_id,
                "end_id": config.end_id, "status": "done", "rows": 0}

    rows_in = df.height

    table_details = config.table_details
    key_phi_columns = get_key_phi_column_list(table_details.get("columns_details", []))

    # 2. Reference mapping resolution (needs source DB only if reference_mapping is configured)
    if table_details.get("reference_mapping"):
        source = NDDBHandler(config.source_conn_str, read_only=True)
        join_db = NDDBHandler(config.join_db_conn_str, read_only=True) if config.join_db_conn_str else None
        try:
            ref_joiner = ReferenceMappingDataFrameJoiner(source, df, table_details, key_phi_columns, join_db=join_db)
            df, key_phi_columns = ref_joiner.join_dataframe()
        finally:
            source.close()
            if join_db is not None:
                join_db.close()

    # 3. Mapping joins — try preloaded in-memory tables first; fall back to SQL
    #    per-mapping when a specific table wasn't ready in time (partial preload).
    preloaded = get_preloaded_data()
    enc_df = preloaded.get("encounter_mapping")
    pat_df = preloaded.get("patient_mapping")
    apt_df = preloaded.get("appointment_mapping")

    # Determine which mappings need SQL fallback
    need_sql = (
        (enc_df is None and bool(key_phi_columns[0])) or
        (pat_df is None and bool(key_phi_columns[1] or key_phi_columns[2])) or
        (apt_df is None and bool(key_phi_columns[3]))
    )

    sql_obj = None
    if need_sql:
        sql_obj = JoinMapping(df, key_phi_columns, config.mapping_db_config, config.table_name)

    try:
        # --- Encounter mapping ---
        if key_phi_columns[0]:
            enc_from_sql = False
            if enc_df is None and sql_obj is not None:
                logger.warning("encounter_mapping not preloaded — using SQL fallback")
                enc_df = sql_obj._get_encounter_mapping(sql_obj._get_distinct_encounterids())
                enc_from_sql = True
            if enc_df is not None:
                # SQL fallback (_get_encounter_mapping) already runs the patient join internally
                # and renames patient_id → patient_id_from_encounter_mapping, so skip here.
                # For the raw preloaded table, do the patient enrichment only if patient_id exists.
                if enc_from_sql or "patient_id" not in enc_df.columns:
                    enc_enriched = enc_df
                else:
                    enrich_pat = pat_df if pat_df is not None else (
                        sql_obj._get_patient_mapping(sql_obj._get_distinct_patientids())
                        if sql_obj is not None else None
                    )
                    if enrich_pat is not None:
                        enc_enriched = join_dataframes(enc_df, enrich_pat,
                                                       left_on="patient_id", right_on="patient_id",
                                                       how="left", right_suffix="from_encounter_mapping",
                                                       drop_left_join_column=False)
                        if "patient_id" in enc_enriched.columns:
                            enc_enriched = enc_enriched.rename({"patient_id": "patient_id_from_encounter_mapping"})
                    else:
                        enc_enriched = enc_df
                df = join_dataframes(df, enc_enriched, left_on=key_phi_columns[0][0],
                                     right_on="encounter_id", how="left", right_suffix="",
                                     drop_right_join_column=True)

        # --- Patient mapping ---
        if key_phi_columns[1]:
            if pat_df is None and sql_obj is not None:
                logger.warning("patient_mapping not preloaded — using SQL fallback")
                pat_df = sql_obj._get_patient_mapping(sql_obj._get_distinct_patientids())
            if pat_df is not None:
                df = join_dataframes(df, pat_df, left_on=key_phi_columns[1][0],
                                     right_on="patient_id", how="left",
                                     right_suffix="from_patient_mapping",
                                     drop_right_join_column=True)

        # --- Reference PID mapping ---
        if key_phi_columns[2]:
            if pat_df is None and sql_obj is not None:
                pat_df = sql_obj._get_patient_mapping(sql_obj._get_distinct_patientids())
            if pat_df is not None:
                df = join_dataframes(df, pat_df, left_on=key_phi_columns[2][0],
                                     right_on="reference_mapping",
                                     right_suffix="from_referencepid_mapping",
                                     how="left", drop_right_join_column=True)

        # --- Appointment mapping ---
        if key_phi_columns[3]:
            apt_from_sql = False
            if apt_df is None and sql_obj is not None:
                logger.warning("appointment_mapping not preloaded — using SQL fallback")
                apt_df = sql_obj._get_appointment_mapping(sql_obj._get_distinct_appointmentids())
                apt_from_sql = True
            if apt_df is not None:
                # SQL fallback already includes the patient join; preloaded is the raw table.
                if apt_from_sql or "patient_id" not in apt_df.columns:
                    apt_enriched = apt_df
                else:
                    enrich_pat = pat_df if pat_df is not None else (
                        sql_obj._get_patient_mapping(sql_obj._get_distinct_patientids())
                        if sql_obj is not None else None
                    )
                    if enrich_pat is not None:
                        apt_enriched = join_dataframes(apt_df, enrich_pat,
                                                       left_on="patient_id", right_on="patient_id",
                                                       how="left", right_suffix="from_appointment_mapping",
                                                       drop_left_join_column=False)
                        if "patient_id" in apt_enriched.columns:
                            apt_enriched = apt_enriched.rename({"patient_id": "patient_id_from_appointment_mapping"})
                    else:
                        apt_enriched = apt_df
                df = join_dataframes(df, apt_enriched, left_on=key_phi_columns[3][0],
                                     right_on="appointment_id", how="left",
                                     drop_right_join_column=True)
    finally:
        if sql_obj is not None:
            sql_obj.close_connection()

    # 4. Resolve patient identifiers
    resolver = PatientIdentifierResolver(key_phi_columns, offset_days=config.offset_days)
    df = resolver.transform(df)

    # 5. Invalid row handling
    rows_before_filter = df.height
    if rows_before_filter != rows_in:
        logger.warning(
            "[%s] Row count changed during mapping joins: fetched=%d, after_joins=%d "
            "(possible duplicate keys in mapping tables)",
            config.table_name, rows_in, rows_before_filter,
        )
    row_handler = InvalidRowHandler(
        db_name=config.source_conn_str.split("/")[-1] if "/" in config.source_conn_str else "",
        table_name=config.table_name,
        db_path=config.failed_rows_db_url,
        config_key=config.config_key,
    )
    df = row_handler.handle(df)
    rows_failed = rows_before_filter - df.height

    # Row-count integrity check: every row entering InvalidRowHandler must
    # either survive to the output or be written to failed_rows.db.
    rows_out = df.height
    if rows_before_filter != rows_out + rows_failed:
        raise RuntimeError(
            f"[{config.table_name}] Row count mismatch: "
            f"pre_filter={rows_before_filter}, output={rows_out}, failed={rows_failed}. "
            f"{rows_before_filter - rows_out - rows_failed} rows lost silently."
        )

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
        "state_db_url": config.state_db_url,
        "config_key": config.config_key,
        "redis_url": config.redis_url,
        "run_config": config.run_config,
        **{k: raw_config[k] for k in ("dest_conn_str", "id_column", "table_details") if k in raw_config},
    }
    write_batch.apply_async(args=[write_config], queue=f"deid-write-{config.config_key}-{config.table_name}")

    logger.info("Processed %s batch %d-%d (%d rows, %d failed)",
                config.table_name, config.start_id, config.end_id, df.height, rows_failed)

    return {"table": config.table_name, "start_id": config.start_id,
            "end_id": config.end_id, "status": "processed",
            "rows": df.height, "rows_failed": rows_failed}


def _update_batch_status(config: ProcessTaskConfig, status: str):
    engine = get_cached_state_engine(config.state_db_url)
    with Session(engine) as session:
        batch = session.query(BatchState).filter_by(
            table_name=config.table_name,
            start_id=config.start_id,
            end_id=config.end_id,
            config_key=config.config_key,
        ).first()
        if batch:
            batch.status = status
            session.commit()
