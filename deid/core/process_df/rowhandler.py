import polars as pl
from deid.core.logger import nd_logger
import json


class InvalidRowHandler:
    """Filter out rows with no resolved patient ID and persist them for audit.

    Rows whose ``_resolved_nd_patient_id`` is null cannot be de-identified.
    They are removed from the processing pipeline and logged for later review.
    """

    def __init__(self, db_name: str, table_name: str):
        self.db_name = db_name
        self.table_name = table_name
        nd_logger.info(
            f"[InvalidRowHandler] Initialized for db: '{db_name}', table: '{table_name}'"
        )

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
            "_resolved_nd_patient_id. Logging for audit..."
        )

        for row_dict in ignored_df.to_dicts():
            row_str = {k: "None" if v is None else str(v) for k, v in row_dict.items()}
            try:
                row_json = json.dumps(row_str, default=str)
                nd_logger.warning(
                    f"[InvalidRowHandler] Ignored row in {self.db_name}.{self.table_name}: {row_json}"
                )
            except Exception as e:
                nd_logger.error(f"[InvalidRowHandler] Failed to serialize row: {e}")

        return df.filter(~invalid_mask)
