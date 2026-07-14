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
from deid.core.dbPkg.dbhandler import stream_from_ipc_cache, stream_table_paginated
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
    """Return (encounter_ids, patient_ids, reference_pids, appointment_ids, chart_ids).

    patient_ids is a dict mapping rule_name -> [column_names], supporting
    dynamic PATIENT_* rules (e.g. PATIENT_PATIENTID, PATIENT_CHARTID).
    Legacy PATIENT_ID is treated as PATIENT_PATIENTID-equivalent.
    """
    encounter_id_columns: list = []
    patient_id_columns: dict = {}
    reference_pid_column: list = []
    appointment_id_columns: list = []
    chart_id_columns: list = []

    if column_details is None:
        raise ValueError("Table Config Not set")

    for column in column_details:
        rule = column["de_identification_rule"]
        if column["is_phi"]:
            col_name = column["column_name"].lower()
            if rule.startswith("PATIENT_"):
                patient_id_columns.setdefault(rule, []).append(col_name)
            elif rule == "ENCOUNTER_ID":
                encounter_id_columns.append(col_name)
            elif rule == "REFERENCE_PID":
                reference_pid_column.append(col_name)
            elif rule == "APPOINTMENT_ID":
                appointment_id_columns.append(col_name)
            elif rule == "CHART_ID":
                chart_id_columns.append(col_name)

    return encounter_id_columns, patient_id_columns, reference_pid_column, appointment_id_columns, chart_id_columns


# ---------------------------------------------------------------------------
# Patient identifier resolution (Polars coalesce — O(1) metadata + O(N) write)
# ---------------------------------------------------------------------------

class PatientIdentifierResolver:
    """Consolidate mapping-table joined columns into canonical _resolved_* columns.

    Supports dynamic PATIENT_* rules (e.g. PATIENT_PATIENTID, PATIENT_CHARTID).
    key_phi_columns[1] is a dict mapping rule_name -> [column_names].

    Priority order for coalesce: referencepid > encounter > [identifier_groups...] > appointment.
    """

    def __init__(self, key_phi_columns: tuple, possible_patient_identifier_columns: list[str] | None = None, offset_days: int = 34):
        self.key_phi_columns = key_phi_columns
        self.possible_patient_identifier_columns = possible_patient_identifier_columns or []
        self.offset_days = offset_days
        self.encounter_group = {
            "nd_patient_id": "nd_patient_id_from_encounter_mapping",
            "offset": "offset_from_encounter_mapping",
        }
        self.referencepid_group = {
            "patient_id": "patient_id_from_referencepid_mapping",
            "nd_patient_id": "nd_patient_id_from_referencepid_mapping",
            "offset": "offset_from_referencepid_mapping",
        }
        self.appointment_group = {
            "nd_patient_id": "nd_patient_id_from_appointment_mapping",
            "offset": "offset_from_appointment_mapping",
        }
        self.chart_group = {
            "nd_patient_id": "nd_patient_id_from_chart_mapping",
            "offset": "offset_from_chart_mapping",
        }
        self.identifier_groups = self._create_dynamic_identifier_groups()

    def _create_dynamic_identifier_groups(self) -> list[dict]:
        """One group per PATIENT_* **source column** — keys the join suffix used during mapping.

        Most tables have a single patient-ID column per rule, but some (e.g. mergelogs, whose
        FromID and ToID reference two *different* patients) carry several.  Each source column
        is joined independently so it resolves to its own de-identified value.

        The first column of a rule keeps the identifier-keyed suffix
        (``from_{identifier_col}_mapping``) for backward compatibility; additional columns use a
        per-column suffix (``from_col_{col}_mapping``) so their joins do not collide.
        """
        groups = []
        for rule_name, columns in self.key_phi_columns[1].items():
            if rule_name == "PATIENT_ID":
                identifier_col = "patient_id"
            else:
                identifier_col = rule_name.split("_")[-1].lower()
            for idx, col in enumerate(columns or []):
                suffix = (
                    f"from_{identifier_col}_mapping" if idx == 0
                    else f"from_col_{col}_mapping"
                )
                groups.append({
                    "identifier_col": identifier_col,
                    "source_col": col,
                    "suffix": suffix,
                    "nd_patient_id": f"nd_patient_id_{suffix}",
                    "offset": f"offset_{suffix}",
                })
        return groups

    def _identifier_candidates(self, x: str) -> list[str]:
        """Candidate columns for _resolved_{x}, following all_groups priority order.

        For the group whose identifier_col == x: the right join key was dropped during the
        mapping join, so use that group's direct source column instead.
        For all other groups: use x_{suffix} (non-key column, survives the join).
        """
        candidates = [
            f"{x}_from_referencepid_mapping",
            f"{x}_from_encounter_mapping",
        ]
        for g in self.identifier_groups:
            if g["identifier_col"] == x:
                candidates.append(g["source_col"])
            else:
                candidates.append(f"{x}_{g['suffix']}")
        candidates += [
            f"{x}_from_appointment_mapping",
            f"{x}_from_chart_mapping",
        ]
        return candidates

    def _coalesce_expr(self, df: pl.DataFrame, candidates: list[str | None]) -> pl.Expr | None:
        """Return pl.coalesce() over the candidate columns that actually exist."""
        existing = [c for c in candidates if c and c in df.columns]
        if not existing:
            return None
        return pl.coalesce([pl.col(c) for c in existing])

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        nd_logger.info(f"[{self.__class__.__name__}] Starting patient identifier resolution...")

        all_groups = (
            [self.referencepid_group, self.encounter_group]
            + self.identifier_groups
            + [self.appointment_group, self.chart_group]
        )

        # --- Step 1: _resolved_offset ---
        offset_candidates = [g.get("offset") for g in all_groups]
        offset_expr = self._coalesce_expr(df, offset_candidates)
        if offset_expr is not None:
            df = df.with_columns(
                offset_expr.fill_null(self.offset_days).alias("_resolved_offset")
            )
        else:
            df = df.with_columns(
                pl.lit(self.offset_days).alias("_resolved_offset")
            )

        # --- Step 2: _resolved_nd_patient_id ---
        nd_patient_id_candidates = [g.get("nd_patient_id") for g in all_groups]
        nd_patient_expr = self._coalesce_expr(df, nd_patient_id_candidates)
        if nd_patient_expr is not None:
            df = df.with_columns(nd_patient_expr.alias("_resolved_nd_patient_id"))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No columns found for _resolved_nd_patient_id. Skipping."
            )

        # --- Step 2b: per-column _resolved_ndpid_col_{col} ---
        # Each patient-ID PHI column resolves to its OWN de-identified value (from its own
        # mapping join), independent of the coalesced row-level _resolved_nd_patient_id.
        # This keeps distinct-patient columns in the same row (e.g. mergelogs FromID/ToID)
        # from being overwritten with the same value.  Consumed by PatientIDRule.
        for g in self.identifier_groups:
            nd_col = g["nd_patient_id"]
            if nd_col in df.columns:
                df = df.with_columns(
                    pl.col(nd_col).alias(f"_resolved_ndpid_col_{g['source_col']}")
                )

        # --- Step 3: _resolved_{identifier} for every project identifier ---
        # Generated for ALL identifiers in possible_patient_identifier_columns regardless of
        # which PATIENT_* rules are present in this table's config.
        for x in self.possible_patient_identifier_columns:
            expr = self._coalesce_expr(df, self._identifier_candidates(x))
            if expr is not None:
                df = df.with_columns(expr.alias(f"_resolved_{x}"))
            else:
                nd_logger.warning(
                    "[PatientIdentifierResolver] No candidates found for _resolved_%s.", x
                )

        # --- Step 4: drop intermediate mapping columns ---
        all_join_suffixes = (
            ["from_referencepid_mapping", "from_encounter_mapping"]
            + [g["suffix"] for g in self.identifier_groups]
            + ["from_appointment_mapping", "from_chart_mapping"]
        )

        # Keep original source PHI columns and the newly generated _resolved_* columns.
        preserved: set[str] = set()
        for rule_cols in self.key_phi_columns[1].values():
            preserved.update(rule_cols)
        for x in self.possible_patient_identifier_columns:
            preserved.add(f"_resolved_{x}")
        for g in self.identifier_groups:
            preserved.add(f"_resolved_ndpid_col_{g['source_col']}")

        nd_offset_intermediates = set(filter(None, offset_candidates + nd_patient_id_candidates))
        cross_join_cols = {
            f"{x}_{suffix}"
            for x in self.possible_patient_identifier_columns
            for suffix in all_join_suffixes
        }

        to_drop = [
            c for c in (nd_offset_intermediates | cross_join_cols)
            if c in df.columns and c not in preserved
        ]
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
    columns = list(result.keys())
    if not rows:
        return pl.DataFrame(schema={c: pl.Utf8 for c in columns})
    return pl.DataFrame(
        [list(r) for r in rows],
        schema=columns,
        orient="row",
        # Scan all rows before fixing dtypes — avoids ComputeError when early
        # rows are all-null and a later row has a typed value (e.g. a string ID).
        infer_schema_length=len(rows),
    )


class JoinMapping:
    """Fetch and join patient/encounter mapping tables against the current batch."""

    def __init__(self, df: pl.DataFrame, key_phi_columns, mapping_db_config: dict, table_name: str = ""):
        self.df = df
        self.key_phi_columns = key_phi_columns
        self.mapping_db_config = mapping_db_config
        self.table_name = table_name
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

    def _get_distinct_referencepids(self):
        return self._get_distinct_ids(2, "reference PIDs")

    
    def _get_distinct_appointmentids(self):
        return self._get_distinct_ids(3, "appointment IDs")

    def _get_distinct_chartids(self):
        return self._get_distinct_ids(4, "chart IDs")

    def _get_chart_mapping(
        self, chart_ids: list, possible_patient_identifier_columns: list[str]
    ) -> pl.DataFrame | None:
        return self._get_mapping_with_patient_join(
            ids=chart_ids,
            table_name="chart_mapping_table",
            id_column="chart_id",
            nd_id_column="nd_chart_id",
            right_suffix="from_chart_mapping",
            possible_patient_identifier_columns=possible_patient_identifier_columns,
        )

    def get_possible_patient_identifier_columns(self) -> tuple[list[str], str | None]:
        """Return (patient_identifier_columns, error_message) from config.

        Reads ``patient_identifier_columns`` from mapping_db_config and validates each
        column exists in ``patient_mapping_table``.

        Returns ``(columns, None)`` on success, ``([], error_message)`` on failure.
        Never raises — callers decide how to handle the error.
        """
        configured = self.mapping_db_config.get("patient_identifier_columns") or []
        if not configured:
            msg = (
                "patient_identifier_columns is not configured. "
                "Add 'identifier_columns' under 'mapping_tables.patient' in your config YAML."
            )
            nd_logger.error("[%s] %s", self.table_name, msg)
            return [], msg

        metadata = MetaData()
        patient_mapping = Table("patient_mapping_table", metadata, autoload_with=self.engine)
        actual_cols = {c.name for c in patient_mapping.columns}
        missing = [c for c in configured if c not in actual_cols]
        if missing:
            msg = (
                f"Configured patient_identifier_columns not found in patient_mapping_table: "
                f"{missing}. Available columns: {sorted(actual_cols)}"
            )
            nd_logger.error("[%s] %s", self.table_name, msg)
            return [], msg

        nd_logger.info(
            "[%s] Patient identifier columns (from config): %s", self.table_name, configured
        )
        return configured, None

    def _get_patient_mapping_from_nd_patient_id(
        self,
        nd_patient_ids: list,
        possible_patient_identifier_columns: list[str],
    ) -> pl.DataFrame | None:
        """Fetch patient_mapping rows by nd_patient_id.

        Used by the encounter/appointment indirect lookup path: those tables
        carry nd_patient_id, so we join patient_mapping on that key to get
        all identifier columns + offset.
        """
        if not nd_patient_ids:
            nd_logger.warning(f"[{self.__class__.__name__}] No nd_patient_ids provided.")
            return None

        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching patient mapping for "
            f"{len(nd_patient_ids)} nd_patient_ids..."
        )
        metadata = MetaData()
        patient_mapping = Table("patient_mapping_table", metadata, autoload_with=self.engine)
        all_mapping_cols = [col.name for col in patient_mapping.columns]
        cols_to_select = [patient_mapping.c.nd_patient_id, patient_mapping.c.offset] + [
            patient_mapping.c[c]
            for c in possible_patient_identifier_columns
            if c in all_mapping_cols
        ]
        stmt = select(*cols_to_select).where(
            patient_mapping.c.nd_patient_id.in_(nd_patient_ids)
        )
        with self.engine.connect() as conn:
            df = _sql_result_to_polars(conn.execute(stmt))
        nd_logger.info(
            f"[{self.__class__.__name__}] Retrieved {df.height} rows from patient_mapping_table "
            f"(by nd_patient_id)."
        )
        return df

    def apply_patient_mappings(self, possible_patient_identifier_columns: list[str]) -> None:
        """Apply direct patient mapping lookups for every PATIENT_* rule.

        For each rule in key_phi_columns[1]:
        - Derives the mapping-table identifier column from the rule name
        - Fetches matching rows from patient_mapping_table
        - Joins result into self.df with suffix 'from_{identifier_col}_mapping'

        Mutates self.df in place.
        """
        if not self.key_phi_columns[1]:
            nd_logger.warning(f"[{self.__class__.__name__}] No PATIENT_* rules found.")
            return

        metadata = MetaData()
        patient_mapping_table = Table("patient_mapping_table", metadata, autoload_with=self.engine)
        all_mapping_cols = [col.name for col in patient_mapping_table.columns]

        for rule, columns in self.key_phi_columns[1].items():
            if not columns:
                continue

            if rule == "PATIENT_ID":
                identifier_col = "patient_id"
            else:
                identifier_col = rule.split("_")[-1].lower()

            if identifier_col not in all_mapping_cols:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] Identifier column '{identifier_col}' not found "
                    f"in patient_mapping_table. Skipping rule '{rule}'."
                )
                continue

            # A rule may map several source columns (e.g. mergelogs FromID/ToID, both patient
            # IDs but referencing DIFFERENT patients). Join each independently so each resolves
            # to its own de-identified value. The first column keeps the identifier-keyed suffix
            # for backward compatibility; the rest use a per-column suffix to avoid collisions.
            for idx, left_col in enumerate(columns):
                right_suffix = (
                    f"from_{identifier_col}_mapping" if idx == 0
                    else f"from_col_{left_col}_mapping"
                )
                nd_logger.info(
                    f"[{self.__class__.__name__}] Processing rule '{rule}' — "
                    f"source col '{left_col}' mapped via '{identifier_col}' "
                    f"(suffix '{right_suffix}')"
                )

                distinct_values = DistinctValueFetcher(self.df).get_distinct_values(left_col)
                if not distinct_values:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] No values in column '{left_col}'. Skipping."
                    )
                    continue

                other_cols = [
                    patient_mapping_table.c[c]
                    for c in possible_patient_identifier_columns
                    if c != identifier_col and c in all_mapping_cols
                ]
                cols_to_select = (
                    [patient_mapping_table.c[identifier_col]]
                    + other_cols
                    + [patient_mapping_table.c.nd_patient_id, patient_mapping_table.c.offset]
                )
                stmt = select(*cols_to_select).where(
                    patient_mapping_table.c[identifier_col].in_(distinct_values)
                )
                with self.engine.connect() as conn:
                    df_mapping = _sql_result_to_polars(conn.execute(stmt))

                nd_logger.info(
                    f"[{self.__class__.__name__}] Retrieved {df_mapping.height} rows for "
                    f"rule '{rule}' column '{left_col}'."
                )

                if df_mapping.height > 0:
                    self.df = join_dataframes(
                        self.df,
                        df_mapping,
                        left_on=left_col,
                        right_on=identifier_col,
                        how="left",
                        right_suffix=right_suffix,
                        drop_right_join_column=True,
                    )
                else:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] No mappings found for column '{left_col}'."
                    )

    def _get_mapping_with_patient_join(
        self,
        ids: list,
        table_name: str,
        id_column: str,
        nd_id_column: str,
        right_suffix: str,
        possible_patient_identifier_columns: list[str],
    ) -> pl.DataFrame | None:
        """Fetch encounter/appointment mapping rows and enrich with patient_mapping data.

        Uses nd_patient_id to join patient_mapping_table, matching the approach where
        encounter/appointment tables carry nd_patient_id (not patient_id).
        """
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
                mapping_table.c.nd_patient_id.label("nd_patient_id"),
            )
            .where(mapping_table.c[id_column].in_(ids))
        )
        # Only carry forward ACTIVE mappings. After a transfer, the same
        # encounter/appointment/chart id can have both an active ('Y') and a
        # soft-deleted ('N') row; without this filter the left join fans out and
        # duplicates source rows with conflicting nd_patient_id. Mirrors the
        # preload path (celery_app._fetch_mapping_table). Guarded so tables that
        # predate the column still work.
        if "nd_ActiveFlag" in mapping_table.c:
            stmt = stmt.where(mapping_table.c.nd_ActiveFlag == "Y")
        with self.engine.connect() as conn:
            df_mapping = _sql_result_to_polars(conn.execute(stmt))

        nd_logger.info(
            f"[{self.__class__.__name__}] Retrieved {df_mapping.height} rows from {table_name}."
        )

        nd_patient_ids = DistinctValueFetcher(df_mapping).get_distinct_values("nd_patient_id")
        nd_logger.info(
            f"[{self.__class__.__name__}] Extracted {len(nd_patient_ids)} unique nd_patient_ids."
        )

        df_patient_mapping = self._get_patient_mapping_from_nd_patient_id(
            nd_patient_ids, possible_patient_identifier_columns
        )
        if df_patient_mapping is None:
            df_patient_mapping = pl.DataFrame(
                schema={"nd_patient_id": pl.Int64, "offset": pl.Int64}
            )

        nd_pid_col = f"nd_patient_id_{right_suffix}"
        if "nd_patient_id" in df_mapping.columns:
            df_mapping = df_mapping.rename({"nd_patient_id": nd_pid_col})
        df_joined = join_dataframes(
            df_mapping,
            df_patient_mapping,
            left_on=nd_pid_col,
            right_on="nd_patient_id",
            how="left",
            right_suffix=right_suffix,
            drop_right_join_column=True,
        )
        nd_logger.info(
            f"[{self.__class__.__name__}] Joined {table_name} + patient_mapping. "
            f"Final rows: {df_joined.height}, columns: {df_joined.columns}"
        )
        return df_joined

    def _get_encounter_mapping(
        self, encounter_ids: list, possible_patient_identifier_columns: list[str]
    ) -> pl.DataFrame | None:
        return self._get_mapping_with_patient_join(
            ids=encounter_ids,
            table_name="encounter_mapping_table",
            id_column="encounter_id",
            nd_id_column="nd_encounter_id",
            right_suffix="from_encounter_mapping",
            possible_patient_identifier_columns=possible_patient_identifier_columns,
        )

    def _get_appointment_mapping(
        self, appointment_ids: list, possible_patient_identifier_columns: list[str]
    ) -> pl.DataFrame | None:
        return self._get_mapping_with_patient_join(
            ids=appointment_ids,
            table_name="appointment_mapping_table",
            id_column="appointment_id",
            nd_id_column="nd_appointment_id",
            right_suffix="from_appointment_mapping",
            possible_patient_identifier_columns=possible_patient_identifier_columns,
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

        col_name = col_conf["column_name"]
        rule_name = col_conf["de_identification_rule"]
        try:
            rule = Rules[rule_name]
        except KeyError:
            if rule_name.startswith("PATIENT_"):
                rule = Rules.PATIENT_ID  # dynamic rules share the BIGINT schema
            else:
                raise ValueError(f"Unknown de-identification rule: {rule_name}")

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
    # Read identifier columns from mapping_db_config (set from YAML mapping_tables.patient.identifier_columns).
    possible_patient_identifier_columns: list[str] = (
        (mapping_db_config or {}).get("patient_identifier_columns") or []
    )
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
                # Validate configured identifier columns against the actual mapping table schema.
                _, _col_err = mapping_obj.get_possible_patient_identifier_columns()
                if _col_err:
                    nd_logger.error("[%s] Aborting: %s", table_name, _col_err)
                    break
            else:
                mapping_obj.df = df
                mapping_obj.key_phi_columns = key_phi_columns

            # Direct patient mapping: one join per PATIENT_* rule (mutates mapping_obj.df).
            mapping_obj.apply_patient_mappings(possible_patient_identifier_columns)
            df = mapping_obj.df

            distinct_encounterIds = mapping_obj._get_distinct_encounterids()
            df_encounter_mapping = mapping_obj._get_encounter_mapping(
                distinct_encounterIds, possible_patient_identifier_columns
            )
            if df_encounter_mapping is not None and key_phi_columns[0]:
                df = join_dataframes(
                    df, df_encounter_mapping,
                    left_on=key_phi_columns[0][0],
                    right_on="encounter_id",
                    how="left",
                    right_suffix="",
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
            df_appointment_mapping = mapping_obj._get_appointment_mapping(
                distinct_appointmentIds, possible_patient_identifier_columns
            )
            if df_appointment_mapping is not None and key_phi_columns[3]:
                df = join_dataframes(
                    df, df_appointment_mapping,
                    left_on=key_phi_columns[3][0],
                    right_on="appointment_id",
                    how="left",
                    drop_right_join_column=True,
                )

            distinct_chartIds = mapping_obj._get_distinct_chartids()
            df_chart_mapping = mapping_obj._get_chart_mapping(
                distinct_chartIds, possible_patient_identifier_columns
            )
            if df_chart_mapping is not None and key_phi_columns[4]:
                df = join_dataframes(
                    df, df_chart_mapping,
                    left_on=key_phi_columns[4][0],
                    right_on="chart_id",
                    how="left",
                    drop_right_join_column=True,
                )

            resolver = PatientIdentifierResolver(
                key_phi_columns,
                possible_patient_identifier_columns,
                offset_days=offset_days,
            )
            df = resolver.transform(df)
            nd_logger.info(
                f"[{table_name}] DataFrame columns: {df.columns}"
            )

            row_handler = InvalidRowHandler(
                db_name=db_name,
                table_name=table_name,
                db_path=_run_config.get("failed_rows_db_url") or _run_config.get("failed_rows_db_path"),
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
                    possible_patient_identifier_columns=possible_patient_identifier_columns,
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
