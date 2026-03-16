import polars as pl
from deid.core.logger import nd_logger
import json
from pydantic import validate_call


class InvalidRowHandler:
    """Filter out rows with no resolved patient ID and persist them for audit.

    Rows whose ``_resolved_nd_patient_id`` is null cannot be de-identified.
    They are removed from the processing pipeline and written to failed_rows.db.
    """

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
            return
        try:
            from deid.models.base import create_failed_rows_engine, create_all_failed_rows_tables
            from deid.models.failed_rows import FailedRow
            from sqlalchemy.orm import Session

            engine = create_failed_rows_engine(self.db_path)
            create_all_failed_rows_tables(engine)
            with Session(engine) as session:
                for row_dict in rows:
                    row_str = {k: "None" if v is None else str(v) for k, v in row_dict.items()}
                    session.add(FailedRow(
                        source_db=self.db_name,
                        table_name=self.table_name,
                        reason="no_resolved_patient_id",
                        row_data=json.dumps(row_str, default=str),
                    ))
                session.commit()
            engine.dispose()
            nd_logger.info(
                f"[InvalidRowHandler] Wrote {len(rows)} failed rows to '{self.db_path}'."
            )
        except Exception as e:
            nd_logger.error(f"[InvalidRowHandler] Failed to write to failed_rows DB: {e}")

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def handle(self, df: pl.DataFrame) -> pl.DataFrame:
        if "_resolved_nd_patient_id" not in df.columns:
            nd_logger.warning(
                "[InvalidRowHandler] Column '_resolved_nd_patient_id' not found. "
                "Returning original DataFrame."
            )
            return df

        invalid_mask = pl.col("_resolved_nd_patient_id").is_null()
        ignored_df = df.filter(invalid_mask)

        if ignored_df.is_empty():
            nd_logger.info("[InvalidRowHandler] No invalid rows found. Nothing to ignore.")
            return df

        nd_logger.info(
            f"[InvalidRowHandler] Found {ignored_df.height} rows with missing "
            "_resolved_nd_patient_id. Writing to audit DB..."
        )

        rows = ignored_df.to_dicts()
        self._write_to_failed_rows_db(rows)

        for row_dict in rows:
            row_str = {k: "None" if v is None else str(v) for k, v in row_dict.items()}
            try:
                row_json = json.dumps(row_str, default=str)
                nd_logger.warning(
                    f"[InvalidRowHandler] Ignored row in {self.db_name}.{self.table_name}: {row_json}"
                )
            except Exception as e:
                nd_logger.error(f"[InvalidRowHandler] Failed to serialize row: {e}")

        return df.filter(~invalid_mask)
