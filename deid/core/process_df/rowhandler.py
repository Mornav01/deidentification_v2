import polars as pl
from deid.core.logger import nd_logger
import json
from pydantic import validate_call


class InvalidRowHandler:
    """Filter out rows with unresolved de-identified IDs and persist them for audit.

    Rows whose ``_resolved_nd_patient_id``, ``nd_encounter_id``, or
    ``nd_appointment_id`` is null (when the column exists) cannot be safely
    written to the destination.  They are removed from the processing pipeline
    and written to failed_rows.db.
    """

    # Mapping columns to check — if the column is present in the DataFrame
    # and contains nulls, those rows are considered invalid.
    _ID_COLUMNS = [
        ("_resolved_nd_patient_id", "no_resolved_patient_id"),
        ("nd_encounter_id", "no_resolved_encounter_id"),
        ("nd_appointment_id", "no_resolved_appointment_id"),
    ]

    def __init__(self, db_name: str, table_name: str, db_path: str | None = None):
        self.db_name = db_name
        self.table_name = table_name
        self.db_path = db_path
        nd_logger.info(
            f"[InvalidRowHandler] Initialized for db: '{db_name}', table: '{table_name}'"
        )

    def _write_to_failed_rows_db(self, rows: list[dict]) -> None:
        """Persist failed rows to the audit SQLite database."""
        if not self.db_path:
            raise RuntimeError(
                f"[InvalidRowHandler] {len(rows)} invalid rows in "
                f"{self.db_name}.{self.table_name} but failed_rows_db_path is not "
                f"configured — refusing to silently discard rows."
            )
        try:
            from deid.models.base import create_failed_rows_engine, create_all_failed_rows_tables
            from deid.models.failed_rows import FailedRow
            from sqlalchemy.orm import Session

            engine = create_failed_rows_engine(self.db_path)
            create_all_failed_rows_tables(engine)
            with Session(engine) as session:
                for row_dict in rows:
                    # Determine which ID columns are null for this row.
                    null_ids = [
                        col for col, _ in self._ID_COLUMNS
                        if col in row_dict and row_dict[col] is None
                    ]
                    reason = "unresolved_ids:" + ",".join(null_ids) if null_ids else "unresolved_id"
                    row_str = {k: "None" if v is None else str(v) for k, v in row_dict.items()}
                    session.add(FailedRow(
                        source_db=self.db_name,
                        table_name=self.table_name,
                        reason=reason,
                        row_data=json.dumps(row_str, default=str),
                    ))
                session.commit()
            engine.dispose()
            nd_logger.info(
                f"[InvalidRowHandler] Wrote {len(rows)} failed rows to '{self.db_path}'."
            )
        except Exception as e:
            nd_logger.error(f"[InvalidRowHandler] Failed to write to failed_rows DB: {e}")
            raise

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def handle(self, df: pl.DataFrame) -> pl.DataFrame:
        # Build a combined null mask across all ID columns present in the DataFrame.
        null_conditions = []
        matched_reasons = []
        for col_name, reason in self._ID_COLUMNS:
            if col_name in df.columns:
                null_conditions.append(pl.col(col_name).is_null())
                matched_reasons.append((col_name, reason))

        if not null_conditions:
            nd_logger.warning(
                "[InvalidRowHandler] No de-identified ID columns found in DataFrame. "
                "Returning original DataFrame."
            )
            return df

        # OR across all null conditions — a row is invalid if ANY expected ID is null.
        invalid_mask = null_conditions[0]
        for cond in null_conditions[1:]:
            invalid_mask = invalid_mask | cond

        ignored_df = df.filter(invalid_mask)

        if ignored_df.is_empty():
            nd_logger.info("[InvalidRowHandler] No invalid rows found. Nothing to ignore.")
            return df

        # Determine per-column counts for logging.
        for col_name, reason in matched_reasons:
            col_null_count = ignored_df.filter(pl.col(col_name).is_null()).height
            if col_null_count > 0:
                nd_logger.warning(
                    f"[InvalidRowHandler] {col_null_count} rows in "
                    f"{self.db_name}.{self.table_name} have null '{col_name}' — "
                    f"reason: {reason}"
                )

        rows = ignored_df.to_dicts()
        self._write_to_failed_rows_db(rows)

        nd_logger.warning(
            f"[InvalidRowHandler] Removed {ignored_df.height} rows with unresolved "
            f"de-identified IDs from {self.db_name}.{self.table_name}. "
            f"Persisted to '{self.db_path}'."
        )

        return df.filter(~invalid_mask)
