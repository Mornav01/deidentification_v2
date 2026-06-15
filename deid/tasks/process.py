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
        _rows_out    = result.get("rows", 0)
        _rows_failed = result.get("rows_failed", 0)
        _publish(config, LogLevel.INFO, "process",
                 f"batch {batch_tag} processed",
                 batch=config.start_id, rows_in_batch=_rows_out + _rows_failed,
                 rows_succeeded=_rows_out,
                 rows_failed=_rows_failed,
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

    # 3. Mapping joins
    preloaded = get_preloaded_data()
    possible_patient_identifier_columns: list[str] = (
        config.mapping_db_config.get("patient_identifier_columns") or []
    )
    if preloaded:
        enc_df = preloaded.get("encounter_mapping")
        pat_df = preloaded.get("patient_mapping")
        apt_df = preloaded.get("appointment_mapping")

        if enc_df is not None and key_phi_columns[0]:
            # Enrich enc_df with patient_mapping via nd_patient_id.
            # Rename nd_patient_id → nd_patient_id_from_encounter_mapping BEFORE the join so it
            # survives as a regular (non-key) column — Polars drops the right join key when
            # left_on != right_on, so keeping it on the left side is the only reliable way.
            if pat_df is not None:
                _enc = enc_df.rename({"nd_patient_id": "nd_patient_id_from_encounter_mapping"}) \
                       if "nd_patient_id" in enc_df.columns else enc_df
                enc_enriched = join_dataframes(_enc, pat_df,
                                               left_on="nd_patient_id_from_encounter_mapping",
                                               right_on="nd_patient_id",
                                               how="left", right_suffix="from_encounter_mapping",
                                               drop_right_join_column=False)
            else:
                enc_enriched = (
                    enc_df.rename({"nd_patient_id": "nd_patient_id_from_encounter_mapping"})
                    if "nd_patient_id" in enc_df.columns else enc_df
                )
            df = join_dataframes(df, enc_enriched, left_on=key_phi_columns[0][0],
                                 right_on="encounter_id", how="left", right_suffix="",
                                 drop_right_join_column=True)

        # Direct patient mapping: one join per PATIENT_* rule, keyed by identifier column.
        if pat_df is not None and key_phi_columns[1]:
            pat_cols = pat_df.columns
            for rule, columns in key_phi_columns[1].items():
                if not columns:
                    continue
                left_col = columns[0]
                identifier_col = "patient_id" if rule == "PATIENT_ID" else rule.split("_")[-1].lower()
                if identifier_col in pat_cols and left_col in df.columns:
                    df = join_dataframes(df, pat_df, left_on=left_col,
                                         right_on=identifier_col, how="left",
                                         right_suffix=f"from_{identifier_col}_mapping",
                                         drop_right_join_column=True)

        if pat_df is not None and key_phi_columns[2]:
            df = join_dataframes(df, pat_df, left_on=key_phi_columns[2][0],
                                 right_on="reference_mapping",
                                 right_suffix="from_referencepid_mapping",
                                 how="left", drop_right_join_column=True)

        if apt_df is not None and key_phi_columns[3]:
            if pat_df is not None:
                _apt = apt_df.rename({"nd_patient_id": "nd_patient_id_from_appointment_mapping"}) \
                       if "nd_patient_id" in apt_df.columns else apt_df
                apt_enriched = join_dataframes(_apt, pat_df,
                                               left_on="nd_patient_id_from_appointment_mapping",
                                               right_on="nd_patient_id",
                                               how="left", right_suffix="from_appointment_mapping",
                                               drop_right_join_column=False)
            else:
                apt_enriched = (
                    apt_df.rename({"nd_patient_id": "nd_patient_id_from_appointment_mapping"})
                    if "nd_patient_id" in apt_df.columns else apt_df
                )
            df = join_dataframes(df, apt_enriched, left_on=key_phi_columns[3][0],
                                 right_on="appointment_id", how="left",
                                 drop_right_join_column=True)

        chart_df = preloaded.get("chart_mapping")
        if chart_df is not None and key_phi_columns[4]:
            if pat_df is not None:
                _chart = chart_df.rename({"nd_patient_id": "nd_patient_id_from_chart_mapping"}) \
                         if "nd_patient_id" in chart_df.columns else chart_df
                chart_enriched = join_dataframes(_chart, pat_df,
                                                 left_on="nd_patient_id_from_chart_mapping",
                                                 right_on="nd_patient_id",
                                                 how="left", right_suffix="from_chart_mapping",
                                                 drop_right_join_column=False)
            else:
                chart_enriched = (
                    chart_df.rename({"nd_patient_id": "nd_patient_id_from_chart_mapping"})
                    if "nd_patient_id" in chart_df.columns else chart_df
                )
            df = join_dataframes(df, chart_enriched, left_on=key_phi_columns[4][0],
                                 right_on="chart_id", how="left",
                                 drop_right_join_column=True)
    else:
        # Fallback: per-batch SQL joins via JoinMapping
        mapping_obj = JoinMapping(df, key_phi_columns, config.mapping_db_config, config.table_name)
        try:
            _, validation_err = mapping_obj.get_possible_patient_identifier_columns()
            if validation_err:
                _update_table_state_failed(config, validation_err)
                _publish(config, LogLevel.ERROR, "process",
                         f"batch {config.start_id}-{config.end_id} skipped: {validation_err}",
                         start_id=config.start_id, end_id=config.end_id, error=validation_err)
                return {"table": config.table_name, "start_id": config.start_id,
                        "end_id": config.end_id, "status": "failed", "rows": 0,
                        "error": validation_err}

            mapping_obj.apply_patient_mappings(possible_patient_identifier_columns)
            df = mapping_obj.df

            distinct_eids = mapping_obj._get_distinct_encounterids()
            df_enc = mapping_obj._get_encounter_mapping(distinct_eids, possible_patient_identifier_columns)
            if df_enc is not None and key_phi_columns[0]:
                df = join_dataframes(df, df_enc, left_on=key_phi_columns[0][0],
                                     right_on="encounter_id", how="left", right_suffix="",
                                     drop_right_join_column=True)

            distinct_rpids = mapping_obj._get_distinct_referencepids()
            df_ref = mapping_obj._get_reference_pid_mapping(distinct_rpids)
            if df_ref is not None and key_phi_columns[2]:
                df = join_dataframes(df, df_ref, left_on=key_phi_columns[2][0],
                                     right_on="reference_mapping", right_suffix="from_referencepid_mapping",
                                     how="left", drop_right_join_column=True)

            distinct_aids = mapping_obj._get_distinct_appointmentids()
            df_apt = mapping_obj._get_appointment_mapping(distinct_aids, possible_patient_identifier_columns)
            if df_apt is not None and key_phi_columns[3]:
                df = join_dataframes(df, df_apt, left_on=key_phi_columns[3][0],
                                     right_on="appointment_id", how="left",
                                     drop_right_join_column=True)

            distinct_cids = mapping_obj._get_distinct_chartids()
            df_chart = mapping_obj._get_chart_mapping(distinct_cids, possible_patient_identifier_columns)
            if df_chart is not None and key_phi_columns[4]:
                df = join_dataframes(df, df_chart, left_on=key_phi_columns[4][0],
                                     right_on="chart_id", how="left",
                                     drop_right_join_column=True)
        finally:
            mapping_obj.close_connection()

    # 4. Resolve patient identifiers
    resolver = PatientIdentifierResolver(
        key_phi_columns, possible_patient_identifier_columns, offset_days=config.offset_days
    )
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
        possible_patient_identifier_columns=possible_patient_identifier_columns,
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


def _update_table_state_failed(config: ProcessTaskConfig, message: str):
    """Mark the table as failed in state.db with a descriptive error message."""
    from deid.models.state import TableState
    engine = get_cached_state_engine(config.state_db_url)
    with Session(engine) as session:
        table_state = session.query(TableState).filter_by(
            table_name=config.table_name,
            config_key=config.config_key,
        ).first()
        if table_state:
            table_state.status = "failed"
            table_state.failure_remarks = message
            session.commit()
        else:
            logger.warning(
                "Could not find TableState for %s (config_key=%s) to record failure: %s",
                config.table_name, config.config_key, message,
            )
