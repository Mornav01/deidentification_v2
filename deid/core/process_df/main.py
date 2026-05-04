import polars as pl
import json
import queue
import threading
import time

from deid.config.table_schemas import TableDetailsForUI, ColumnDetailsForUI
from deid.config.task_models import LogLevel
from deid.core.log_publisher import make_log_record, maybe_log, get_peak_memory_mb
from deid.core.logger import nd_logger
from deid.core.dbPkg import NDDBHandler
from deid.core.dbPkg.dbhandler import stream_from_ipc_cache, stream_table_paginated, _normalize_rows
from deid.core.ops_df.jointables import ReferenceMappingDataFrameJoiner
from deid.core.ops_df.utility import DistinctValueFetcher, join_dataframes
from sqlalchemy import Table, String, MetaData, select, cast
from sqlalchemy.orm import sessionmaker
from deid.core.dbPkg.dbhandler import create_read_only_engine
from deid.core.process_df.base import DeIdentifier, Rules
from deid.core.process_df.columns_type_detector import ColumnsTypeDetector
from deid.core.process_df.rowhandler import InvalidRowHandler


# ---------------------------------------------------------------------------
# Helper: PHI column categorisation
# ---------------------------------------------------------------------------


def get_key_phi_column_list(column_details: list) -> tuple:
    """Return (encounter_ids, patient_ids, reference_pids, appointment_ids)."""
    encounter_id_columns: list = []
    patient_id_columns: list = []
    reference_pid_column: list = []
    appointment_id_columns: list = []

    if column_details is None:
        raise ValueError("Table Config Not set")

    for column in column_details:
        rule = column["de_identification_rule"]
        if column["is_phi"]:
            col_name = column["column_name"].lower()
            if rule == "PATIENT_ID":
                patient_id_columns.append(col_name)
            elif rule == "ENCOUNTER_ID":
                encounter_id_columns.append(col_name)
            elif rule == "REFERENCE_PID":
                reference_pid_column.append(col_name)
            elif rule == "APPOINTMENT_ID":
                appointment_id_columns.append(col_name)

    return encounter_id_columns, patient_id_columns, reference_pid_column, appointment_id_columns


# ---------------------------------------------------------------------------
# Patient identifier resolution (Polars coalesce — O(1) metadata + O(N) write)
# ---------------------------------------------------------------------------

class PatientIdentifierResolver:
    """Consolidate mapping-table joined columns into canonical _resolved_* columns."""

    def __init__(self, key_phi_columns: tuple, offset_days: int = 34):
        self.key_phi_columns = key_phi_columns
        self.offset_days = offset_days
        self.patient_group = {
            "nd_patient_id": "nd_patient_id_from_patient_mapping",
            "offset": "offset_from_patient_mapping",
        }
        self.encounter_group = {
            "patient_id": "patient_id_from_encounter_mapping",
            "nd_patient_id": "nd_patient_id_from_encounter_mapping",
            "offset": "offset_from_encounter_mapping",
        }
        self.referencepid_group = {
            "patient_id": "patient_id_from_referencepid_mapping",
            "nd_patient_id": "nd_patient_id_from_referencepid_mapping",
            "offset": "offset_from_referencepid_mapping",
        }
        self.appointment_group = {
            "patient_id": "patient_id_from_appointment_mapping",
            "nd_patient_id": "nd_patient_id_from_appointment_mapping",
            "offset": "offset_from_appointment_mapping",
        }

    
    def _coalesce_expr(self, df: pl.DataFrame, candidates: list[str | None]) -> pl.Expr | None:
        """Return pl.coalesce() over the candidate columns that actually exist."""
        existing = [c for c in candidates if c and c in df.columns]
        if not existing:
            return None
        return pl.coalesce([pl.col(c) for c in existing])

    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        nd_logger.info(f"[{self.__class__.__name__}] Starting patient identifier resolution...")

        # --- Step 1: _resolved_offset ---
        offset_candidates = [
            self.referencepid_group.get("offset"),
            self.encounter_group.get("offset"),
            self.patient_group.get("offset"),
            self.appointment_group.get("offset"),
        ]
        offset_expr = self._coalesce_expr(df, offset_candidates)
        if offset_expr is not None:
            df = df.with_columns(
                offset_expr.fill_null(self.offset_days).alias("_resolved_offset")
            )
        else:
            df = df.with_columns(
                pl.lit(self.offset_days).alias("_resolved_offset")
            )

        # --- Step 2: _resolved_patient_id ---
        ref_phi_col = self.key_phi_columns[1][0] if self.key_phi_columns[1] else None
        patient_id_candidates = [
            self.referencepid_group.get("patient_id"),
            self.encounter_group.get("patient_id"),
            ref_phi_col,
            self.appointment_group.get("patient_id"),
        ]
        patient_expr = self._coalesce_expr(df, patient_id_candidates)
        if patient_expr is not None:
            df = df.with_columns(patient_expr.alias("_resolved_patient_id"))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No columns found for _resolved_patient_id. Skipping."
            )

        # --- Step 3: _resolved_nd_patient_id ---
        nd_patient_id_candidates = [
            self.referencepid_group.get("nd_patient_id"),
            self.encounter_group.get("nd_patient_id"),
            self.patient_group.get("nd_patient_id"),
            self.appointment_group.get("nd_patient_id"),
        ]
        nd_patient_expr = self._coalesce_expr(df, nd_patient_id_candidates)
        if nd_patient_expr is not None:
            df = df.with_columns(nd_patient_expr.alias("_resolved_nd_patient_id"))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No columns found for _resolved_nd_patient_id. Skipping."
            )

        # --- Step 4: drop intermediate mapping columns ---
        all_used = set(
            filter(None, offset_candidates + patient_id_candidates + nd_patient_id_candidates)
        )
        if ref_phi_col:
            all_used.discard(ref_phi_col)
        to_drop = [c for c in all_used if c in df.columns]
        if to_drop:
            nd_logger.debug(
                f"[{self.__class__.__name__}] Dropping intermediate columns: {to_drop}"
            )
            df = df.drop(to_drop)

        nd_logger.info(f"[{self.__class__.__name__}] Resolution completed.")
        return df


# ---------------------------------------------------------------------------
# Mapping DB joins  (Polars DataFrames — faster joins than Pandas)
# ---------------------------------------------------------------------------


def _sql_result_to_polars(result) -> pl.DataFrame:
    """Convert a SQLAlchemy CursorResult to a Polars DataFrame."""
    rows = result.fetchall()
    columns = [c.lower() for c in result.keys()]
    if not rows:
        return pl.DataFrame(schema={c: pl.Utf8 for c in columns})
    return pl.DataFrame(
        _normalize_rows(rows),
        schema=columns,
        orient="row",
        infer_schema_length=len(rows),
    )


class JoinMapping:
    """Fetch and join patient/encounter mapping tables against the current batch."""

    def __init__(self, df: pl.DataFrame, key_phi_columns, mapping_db_config: dict, table_name: str = ""):
        self.df = df
        self.key_phi_columns = key_phi_columns
        self.mapping_db_config = mapping_db_config
        nd_logger.info(
            f"[{self.__class__.__name__}] Initialized for table: {table_name}"
        )
        self._get_mapping_table_connection()

    
    def _get_mapping_table_connection(self):
        nd_logger.info(f"[{self.__class__.__name__}] Connecting to mapping DB...")
        connection_string = self.mapping_db_config["connection_str"]
        self.engine = create_read_only_engine(connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        nd_logger.info(f"[{self.__class__.__name__}] Connection established.")

    
    def close_connection(self):
        self.session.close()
        self.engine.dispose()
        nd_logger.info(f"[{self.__class__.__name__}] Connection closed.")

    
    def _get_distinct_ids(self, index: int, label: str) -> list:
        if index >= len(self.key_phi_columns) or not self.key_phi_columns[index]:
            nd_logger.warning(f"[{self.__class__.__name__}] No {label} column configured.")
            return []
        fetcher = DistinctValueFetcher(self.df)
        return fetcher.get_distinct_values(self.key_phi_columns[index][0])

    
    def _get_distinct_encounterids(self):
        return self._get_distinct_ids(0, "encounter IDs")

    
    def _get_distinct_patientids(self):
        return self._get_distinct_ids(1, "patient IDs")

    
    def _get_distinct_referencepids(self):
        return self._get_distinct_ids(2, "reference PIDs")

    
    def _get_distinct_appointmentids(self):
        return self._get_distinct_ids(3, "appointment IDs")

    
    def _get_patient_mapping(self, patient_ids: list) -> pl.DataFrame | None:
        if not patient_ids:
            nd_logger.warning(f"[{self.__class__.__name__}] No patient IDs provided.")
            return None

        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching patient mappings for {len(patient_ids)} IDs..."
        )
        metadata = MetaData()
        patient_mapping = Table("patient_mapping_table", metadata, autoload_with=self.engine)
        stmt = (
            select(
                patient_mapping.c.patient_id,
                patient_mapping.c.nd_patient_id,
                patient_mapping.c.offset,
            )
            .where(patient_mapping.c.patient_id.in_(patient_ids))
        )
        with self.engine.connect() as conn:
            df = _sql_result_to_polars(conn.execute(stmt))
        nd_logger.info(
            f"[{self.__class__.__name__}] Retrieved {df.height} rows from patient_mapping_table."
        )
        return df

    
    def _get_mapping_with_patient_join(
        self,
        ids: list,
        table_name: str,
        id_column: str,
        nd_id_column: str,
        right_suffix: str,
    ) -> pl.DataFrame | None:
        if not ids:
            nd_logger.warning(f"[{self.__class__.__name__}] No IDs for {table_name}.")
            return None

        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching mappings from {table_name} "
            f"for {len(ids)} IDs..."
        )
        metadata = MetaData()
        mapping_table = Table(table_name, metadata, autoload_with=self.engine)
        stmt = (
            select(
                mapping_table.c[id_column],
                cast(mapping_table.c[nd_id_column], String(50)).label(nd_id_column),
                mapping_table.c.patient_id.label("patient_id"),
            )
            .where(mapping_table.c[id_column].in_(ids))
        )
        with self.engine.connect() as conn:
            df_mapping = _sql_result_to_polars(conn.execute(stmt))
            # Keep nd_id_column as Utf8 (cast already happened in SQL).

        nd_logger.info(
            f"[{self.__class__.__name__}] Retrieved {df_mapping.height} rows from {table_name}."
        )

        patient_ids = DistinctValueFetcher(df_mapping).get_distinct_values("patient_id")
        nd_logger.info(
            f"[{self.__class__.__name__}] Extracted {len(patient_ids)} unique patient IDs."
        )

        df_patient_mapping = self._get_patient_mapping(patient_ids)
        if df_patient_mapping is None:
            df_patient_mapping = pl.DataFrame(
                schema={"patient_id": pl.Int64, "nd_patient_id": pl.Int64, "offset": pl.Int64}
            )

        # Keep drop_left_join_column=False so the original patient_id from the
        # encounter/appointment row survives the join.  Polars always drops the
        # *right* key when left_on != right_on, so the renamed right-side key
        # (e.g. "patient_id_from_encounter_mapping") disappears automatically.
        # We then rename the surviving left-side "patient_id" to
        # "patient_id_{right_suffix}" so PatientIdentifierResolver can coalesce
        # it into _resolved_patient_id for use in de_identify_key_phi_columns.
        df_joined = join_dataframes(
            df_mapping,
            df_patient_mapping,
            left_on="patient_id",
            right_on="patient_id",
            how="left",
            right_suffix=right_suffix,
            drop_left_join_column=False,   # ← keep original patient_id
        )
        # Rename patient_id (left key) → patient_id_{right_suffix}
        if "patient_id" in df_joined.columns and f"patient_id_{right_suffix}" not in df_joined.columns:
            df_joined = df_joined.rename({"patient_id": f"patient_id_{right_suffix}"})
        nd_logger.info(
            f"[{self.__class__.__name__}] Joined {table_name} + patient_mapping. "
            f"Final rows: {df_joined.height}, columns: {df_joined.columns}"
        )
        return df_joined

    
    def _get_encounter_mapping(self, encounter_ids: list) -> pl.DataFrame | None:
        return self._get_mapping_with_patient_join(
            ids=encounter_ids,
            table_name="encounter_mapping_table",
            id_column="encounter_id",
            nd_id_column="nd_encounter_id",
            right_suffix="from_encounter_mapping",
        )

    
    def _get_appointment_mapping(self, appointment_ids: list) -> pl.DataFrame | None:
        return self._get_mapping_with_patient_join(
            ids=appointment_ids,
            table_name="appointment_mapping_table",
            id_column="appointment_id",
            nd_id_column="nd_appointment_id",
            right_suffix="from_appointment_mapping",
        )

    
    def _get_reference_pid_mapping(self, reference_pids: list) -> pl.DataFrame | None:
        if not reference_pids:
            nd_logger.warning(f"[{self.__class__.__name__}] No reference PIDs provided.")
            return None

        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching reference PID mappings "
            f"for {len(reference_pids)} PIDs..."
        )
        metadata = MetaData()
        patient_mapping = Table("patient_mapping_table", metadata, autoload_with=self.engine)
        stmt = (
            select(
                patient_mapping.c.reference_mapping,
                patient_mapping.c.patient_id,
                patient_mapping.c.nd_patient_id,
                patient_mapping.c.offset,
            )
            .where(patient_mapping.c.reference_mapping.in_(reference_pids))
        )
        with self.engine.connect() as conn:
            df = _sql_result_to_polars(conn.execute(stmt))
        nd_logger.info(
            f"[{self.__class__.__name__}] Retrieved {df.height} rows via reference_mapping."
        )
        return df


# ---------------------------------------------------------------------------
# Column schema mapping (unchanged — used to CREATE destination table)
# ---------------------------------------------------------------------------


def _get_columns_schema_mapping(
    table_config: dict,
    source_col_lengths: dict | None = None,
) -> dict:
    """Build the {col_name: type_dict} map used to create the destination table.

    For every PHI column the function returns a type override that matches
    what the de-identification pipeline will actually write:

    Rule            Destination type
    ─────────────── ────────────────────────────────────────────────────────
    MASK            VARCHAR(max(source_length, longest_placeholder) + 10)
                    The placeholder written by MaskRule is "<<{mask_value}>>".
                    We ensure the destination column is always wide enough to
                    hold it, even if the source column was very narrow (e.g.
                    VARCHAR(5) for a unit-of-measure column).
    PATIENT_ID /    BIGINT  ← ND integer IDs are always 64-bit
    ENCOUNTER_ID /
    REFERENCE_PID /
    APPOINTMENT_ID
    PATIENT_DOB     INTEGER  ← year-only after masking
    DATE_OFFSET /   DATETIME
    STATIC_OFFSET
    ZIP_CODE        VARCHAR(50)
    NOTES /         LONGTEXT
    GENERIC_NOTES

    ``source_col_lengths`` — optional {column_name: declared_length} dict
    fetched from the source table via ``NDDBHandler.get_columns()``.  When
    provided, MASK columns use ``max(source_length, MIN_MASK_LEN) + BUFFER``
    instead of the static 200-char fallback.
    """
    MIN_MASK_LEN = 50    # always wide enough for any "((...))" placeholder
    MASK_BUFFER  = 10    # extra headroom

    source_col_lengths = source_col_lengths or {}
    schema_mapping: dict = {}
    rule_to_schema = ColumnsTypeDetector.get_columns_definations(table_config)

    for col_conf in table_config["columns_details"]:
        if not col_conf.get("is_phi"):
            continue

        col_name = col_conf["column_name"].lower()
        rule     = Rules[col_conf["de_identification_rule"]]

        if rule == Rules.MASK:
            # Compute the placeholder width:  "(({mask_value}))"
            mask_val      = col_conf.get("mask_value", "(())")
            placeholder_w = len(f"(({mask_val}))")
            source_w      = source_col_lengths.get(col_name, 0) or 0

            dest_len = max(source_w, placeholder_w, MIN_MASK_LEN) + MASK_BUFFER
            schema_mapping[col_name] = {"type": String, "length": dest_len, "null": True}
        else:
            schema_mapping[col_name] = rule_to_schema[rule]

    return schema_mapping


# ---------------------------------------------------------------------------
# Dict-value serialisation  (safety net before DB insert)
# ---------------------------------------------------------------------------


def _serialize_dict_values(df: pl.DataFrame) -> pl.DataFrame:
    """Serialize any dict-typed cell values to JSON strings.

    MySQL cannot store Python dicts directly.  This guard converts Object-dtype
    columns (which may hold dicts from JSON columns) to Utf8 JSON strings.
    """
    
    def _serialize_batch(s: pl.Series) -> pl.Series:
        results = []
        for val in s.to_list():
            if isinstance(val, dict):
                try:
                    results.append(json.dumps(val, default=str))
                except Exception:
                    results.append(str(val))
            elif val is None:
                results.append(None)
            else:
                results.append(str(val))
        return pl.Series(results, dtype=pl.Utf8)

    for col in df.columns:
        if df[col].dtype == pl.Object:
            df = df.with_columns(
                pl.col(col)
                .map_batches(_serialize_batch, return_dtype=pl.Utf8)
                .alias(col)
            )
    return df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def start_de_identification_for_table(
    table_config: dict,
    source_conn_str: str,
    dest_conn_str: str,
    mappings_db_path: str = "",
    batch_size: int = 100000,
    offset_days: int = 34,
    pii_config: dict | None = None,
    pii_db_conn_str: str | None = None,
    secondary_pii_configs: list | None = None,
    mapping_db_config: dict | None = None,
    universal_tables_config: list | None = None,
    run_config: dict | None = None,
    table_name: str = "",
    db_name: str = "",
    start_id: int | None = None,
    end_id: int | None = None,
    id_column: str = "nd_auto_increment_id",
    cache_dir: str | None = None,
):
    """Process an entire table (or a keyset-bounded range) by streaming rows in Polars batches.

    All configuration is passed explicitly — no Django model lookups.

    Single-task mode  (start_id / end_id are None):
        A server-side cursor streams the full table batchwise.  One task
        per table, no row count needed, memory stays at O(batch_size).

    Parallel-range mode  (start_id and end_id are set):
        The cursor filters to ``id_column BETWEEN start_id AND end_id`` so multiple
        workers can process disjoint slices of the same table simultaneously.

    Pipeline optimisation:
        Destination writes run in a background thread so each batch's INSERT
        overlaps with the *next* batch's fetch + de-identification.  This hides
        most of the write I/O latency at no extra memory cost (the write queue
        holds at most one in-flight DataFrame at a time).

    Expensive objects are created once and reused across batches:
    - JoinMapping  : holds the mapping-DB connection pool.
    - DeIdentifier : lazily loads the NLP model for NOTES columns.
    """
    range_tag = f" [id {start_id}–{end_id}]" if start_id is not None else ""
    nd_logger.info(
        f"[{table_name}{range_tag}] Opening source and destination connections."
    )
    source_db_connection: NDDBHandler = NDDBHandler(source_conn_str, read_only=True)
    destination_db: NDDBHandler = NDDBHandler(dest_conn_str)

    # Fetch source column lengths so _get_columns_schema_mapping can size
    # MASK-rule destination columns correctly (avoids MySQL 1265 truncation).
    try:
        _src_col_info = source_db_connection.get_columns(table_name)
        _source_col_lengths: dict = {
            c["name"]: int(getattr(c.get("type"), "length", 0) or 0)
            for c in _src_col_info
        }
    except Exception as _e:
        nd_logger.warning(
            f"[{table_name}] Could not fetch source column lengths "
            f"(will use static defaults): {_e}"
        )
        _source_col_lengths = {}

    column_schema_mapping = _get_columns_schema_mapping(table_config, _source_col_lengths)

    mapping_obj: JoinMapping | None = None
    deidentifier: DeIdentifier | None = None
    dest_table_created = False
    batch_num = 0
    _run_config = run_config or {}

    # -----------------------------------------------------------------------
    # Async background writer
    # -----------------------------------------------------------------------
    # A single background thread runs insert_dataframe_in_batches() so that
    # each batch's INSERT to the destination DB overlaps with the *next*
    # batch's fetch + de-identification.  The queue size of 1 ensures we
    # never buffer more than one extra DataFrame in memory.
    #
    # _WRITER_SENTINEL signals the writer thread to exit cleanly.
    # -----------------------------------------------------------------------
    _WRITER_SENTINEL = object()
    write_queue: queue.Queue = queue.Queue(maxsize=1)
    write_errors: list = []

    
    def _background_writer():
        while True:
            item = write_queue.get()
            if item is _WRITER_SENTINEL:
                break
            try:
                destination_db.insert_dataframe_in_batches(
                    item, table_name=table_name
                )
            except Exception as exc:
                write_errors.append(exc)
                nd_logger.error(
                    f"[{table_name}] Background write failed: {exc}"
                )
                maybe_log(_run_config, make_log_record(
                    LogLevel.ERROR, table_name, "deidentify",
                    f"batch write failed: {exc}",
                    error=str(exc),
                    start_id=start_id,
                    end_id=end_id,
                ))
                break  # stop processing further batches on error

    writer_thread = threading.Thread(target=_background_writer, daemon=True)
    writer_thread.start()

    # Choose the appropriate stream source.
    if cache_dir and start_id is not None and end_id is not None:
        nd_logger.info(
            f"[{table_name}] Reading from IPC cache: {cache_dir}"
        )
        stream = stream_from_ipc_cache(
            cache_dir=cache_dir,
            start_id=start_id,
            end_id=end_id,
            id_column=id_column,
        )
    elif start_id is not None and end_id is not None:
        if source_db_connection.engine.dialect.name == "mssql":
            # MSSQL/pymssql doesn't support true server-side cursors: holding a
            # streaming connection open between fetchmany() calls (while mapping
            # joins and NLP run) causes the server to drop the TCP connection
            # after its query timeout → FreeTDS error 20017 "Unexpected EOF".
            # stream_table_paginated issues a fresh bounded SELECT per batch so
            # the connection is never held idle across processing work.
            stream = stream_table_paginated(
                source_db_connection, table_name,
                min_id=start_id, max_id=end_id, page_size=batch_size,
                id_column=id_column,
            )
        else:
            stream = source_db_connection.stream_table_as_dataframes_in_range(
                table_name, batch_size,
                start_id=start_id, end_id=end_id, id_column=id_column,
            )
    else:
        stream = source_db_connection.stream_table_as_dataframes(
            table_name, batch_size
        )

    try:
        for df in stream:
            if df.is_empty():
                continue

            _batch_start = time.monotonic()
            nd_logger.info(
                f"[{table_name}] Processing batch {batch_num + 1} "
                f"({df.height} rows)."
            )

            # Recompute per-batch (join_dataframe mutates lists in-place).
            key_phi_columns = get_key_phi_column_list(table_config["columns_details"])

            # Resolve indirect patient/encounter IDs via reference-table joins.
            reference_mapping_obj = ReferenceMappingDataFrameJoiner(
                source_db_connection, df, table_config, key_phi_columns
            )
            df, key_phi_columns = reference_mapping_obj.join_dataframe()

            # Reuse JoinMapping connection pool across batches.
            if mapping_obj is None:
                mapping_obj = JoinMapping(df, key_phi_columns, mapping_db_config or {}, table_name)
            else:
                mapping_obj.df = df
                mapping_obj.key_phi_columns = key_phi_columns

            distinct_encounterIds = mapping_obj._get_distinct_encounterids()
            df_encounter_mapping = mapping_obj._get_encounter_mapping(distinct_encounterIds)
            if df_encounter_mapping is not None and key_phi_columns[0]:
                df = join_dataframes(
                    df, df_encounter_mapping,
                    left_on=key_phi_columns[0][0],
                    right_on="encounter_id",
                    how="left",
                    right_suffix="",
                    drop_right_join_column=True,
                )

            distinct_patientIds = mapping_obj._get_distinct_patientids()
            df_patient_mapping = mapping_obj._get_patient_mapping(distinct_patientIds)
            if df_patient_mapping is not None and key_phi_columns[1]:
                df = join_dataframes(
                    df, df_patient_mapping,
                    left_on=key_phi_columns[1][0],
                    right_on="patient_id",
                    how="left",
                    right_suffix="from_patient_mapping",
                    drop_right_join_column=True,
                )

            distinct_referencePIds = mapping_obj._get_distinct_referencepids()
            df_referencepid_mapping = mapping_obj._get_reference_pid_mapping(distinct_referencePIds)
            if df_referencepid_mapping is not None and key_phi_columns[2]:
                df = join_dataframes(
                    df, df_referencepid_mapping,
                    left_on=key_phi_columns[2][0],
                    right_on="reference_mapping",
                    right_suffix="from_referencepid_mapping",
                    how="left",
                    drop_right_join_column=True,
                )

            distinct_appointmentIds = mapping_obj._get_distinct_appointmentids()
            df_appointment_mapping = mapping_obj._get_appointment_mapping(distinct_appointmentIds)
            if df_appointment_mapping is not None and key_phi_columns[3]:
                df = join_dataframes(
                    df, df_appointment_mapping,
                    left_on=key_phi_columns[3][0],
                    right_on="appointment_id",
                    how="left",
                    drop_right_join_column=True,
                )

            resolver = PatientIdentifierResolver(key_phi_columns, offset_days=offset_days)
            df = resolver.transform(df)
            nd_logger.info(
                f"[{table_name}] DataFrame columns: {df.columns}"
            )

            row_handler = InvalidRowHandler(
                db_name=db_name,
                table_name=table_name,
                db_path=_run_config.get("failed_rows_db_path"),
            )
            df = row_handler.handle(df)

            # Reuse DeIdentifier to avoid reloading NLP models on every batch.
            if deidentifier is None:
                deidentifier = DeIdentifier(
                    df=df,
                    config=table_config["columns_details"],
                    pii_config=pii_config,
                    pii_db_conn_str=pii_db_conn_str,
                    secondary_pii_configs=secondary_pii_configs,
                    key_phi_columns=key_phi_columns,
                    offset_days=offset_days,
                    run_config={**_run_config, "table_name": table_name},
                )
            else:
                deidentifier.df = df
                deidentifier.key_phi_columns = key_phi_columns

            df = deidentifier.apply_rules()
            df = _serialize_dict_values(df)

            # Create the destination table on the first non-empty batch.
            if not dest_table_created:
                source_db_connection.create_table_in_dest_if_not_exists(
                    table_name,
                    destination_db,
                    column_type_mapping=column_schema_mapping,
                )
                dest_table_created = True

            # Hand the processed DataFrame to the background writer.
            # If the writer is still busy with the previous batch, this
            # blocks here (queue maxsize=1) — keeping memory bounded.
            write_queue.put(df)

            # Abort early if a write error occurred in the background.
            if write_errors:
                raise write_errors[0]

            batch_num += 1
            _batch_elapsed_ms = int((time.monotonic() - _batch_start) * 1000)
            maybe_log(_run_config, make_log_record(
                LogLevel.INFO, table_name, "deidentify",
                f"batch {batch_num}: {df.height}/{df.height} rows OK in {_batch_elapsed_ms}ms",
                batch=batch_num,
                rows_in_batch=df.height,
                rows_succeeded=df.height,
                rows_failed=0,
                duration_ms=_batch_elapsed_ms,
                start_id=start_id,
                end_id=end_id,
                peak_memory_mb=get_peak_memory_mb(),
            ))

    finally:
        # Signal the writer to finish and wait for it.
        write_queue.put(_WRITER_SENTINEL)
        writer_thread.join()

        if mapping_obj is not None:
            mapping_obj.close_connection()
        source_db_connection.close()
        destination_db.close()

    # Re-raise any write error that surfaced after the loop ended.
    if write_errors:
        raise write_errors[0]

    nd_logger.info(
        f"[{table_name}{range_tag}] "
        f"Streaming complete — {batch_num} batch(es) processed."
    )
    return {"table_name": table_name, "batches_processed": batch_num}
