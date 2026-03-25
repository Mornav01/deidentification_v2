import polars as pl
from deid.core.logger import nd_logger
import json
from pydantic import validate_call


class InvalidRowHandler:
    """Filter out rows with unresolved de-identified IDs and persist them for audit.

    Uses a **priority-based** check: only the highest-priority ID column
    present in the DataFrame determines whether a row is kept or rejected.

    Priority order (first match wins):
      1. ``_resolved_nd_patient_id`` — if the table maps patient IDs
      2. ``nd_encounter_id``         — if the table maps encounter IDs only
      3. ``nd_appointment_id``       — if the table maps appointment IDs only

    Lower-priority columns that are null are NOT grounds for rejection.
    Their corresponding rules (``EncounterIDRule``, ``AppointmentIDRule``)
    already null out the raw PHI value, so no data leaks.
    """

    # Ordered by priority — first column found in the DataFrame is the
    # mandatory check; the rest are informational only.
    _ID_COLUMNS_PRIORITY = [
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

    def _write_to_failed_rows_db(self, rows: list[dict], check_col: str) -> None:
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
                    reason = f"unresolved_id:{check_col}"
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
        # Find the highest-priority ID column present in the DataFrame.
        # Only that column determines whether a row is kept or rejected.
        check_col = None
        check_reason = None
        for col_name, reason in self._ID_COLUMNS_PRIORITY:
            if col_name in df.columns:
                check_col = col_name
                check_reason = reason
                break

        if check_col is None:
            nd_logger.warning(
                "[InvalidRowHandler] No de-identified ID columns found in DataFrame. "
                "Returning original DataFrame."
            )
            return df

        # Log null counts for ALL ID columns (informational), not just the
        # mandatory one — helps diagnose missing encounter/appointment mappings.
        for col_name, reason in self._ID_COLUMNS_PRIORITY:
            if col_name in df.columns:
                null_count = df.filter(pl.col(col_name).is_null()).height
                if null_count > 0:
                    tag = "REJECTING" if col_name == check_col else "info-only"
                    nd_logger.warning(
                        f"[InvalidRowHandler] {null_count}/{df.height} rows in "
                        f"{self.db_name}.{self.table_name} have null '{col_name}' "
                        f"({tag})"
                    )

        invalid_mask = pl.col(check_col).is_null()
        ignored_df = df.filter(invalid_mask)

        if ignored_df.is_empty():
            nd_logger.info("[InvalidRowHandler] No invalid rows found. Nothing to ignore.")
            return df

        rows = ignored_df.to_dicts()
        self._write_to_failed_rows_db(rows, check_col)

        nd_logger.warning(
            f"[InvalidRowHandler] Removed {ignored_df.height} rows where "
            f"'{check_col}' is null from {self.db_name}.{self.table_name}. "
            f"Persisted to '{self.db_path}'."
        )

        return df.filter(~invalid_mask)
