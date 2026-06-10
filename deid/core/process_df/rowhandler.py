import json
import random
import time

import polars as pl
from pydantic import validate_call
from sqlalchemy.exc import OperationalError

from deid.core.logger import nd_logger


class InvalidRowHandler:
    """Filter out rows where _resolved_nd_patient_id is null and persist them for audit.

    A row is invalid only when ``_resolved_nd_patient_id`` is null, meaning no
    mapping join (PATIENT_*, ENCOUNTER_ID, APPOINTMENT_ID, CHART_ID) resolved a
    de-identified patient ID for that row.  Null ``nd_encounter_id``,
    ``nd_appointment_id``, or ``nd_chart_id`` alone are NOT grounds for rejection.
    """

    _RESOLVED_ID_COL = "_resolved_nd_patient_id"

    def __init__(self, db_name: str, table_name: str, db_path: str | None = None, config_key: str = "default"):
        self.db_name = db_name
        self.table_name = table_name
        self.db_path = db_path
        self.config_key = config_key
        nd_logger.info(
            f"[InvalidRowHandler] Initialized for db: '{db_name}', table: '{table_name}'"
        )

    _MAX_WRITE_RETRIES = 5

    def _write_to_failed_rows_db(self, rows: list[dict], check_col: str) -> None:
        """Persist failed rows to a per-schema table in the audit SQLite database."""
        if not self.db_path:
            raise RuntimeError(
                f"[InvalidRowHandler] {len(rows)} invalid rows in "
                f"{self.db_name}.{self.table_name} but failed_rows_db_path is not "
                f"configured — refusing to silently discard rows."
            )
        from datetime import datetime, timezone
        from deid.models.base import get_cached_failed_rows_engine
        from deid.models.failed_rows import ensure_schema_table

        reason = f"unresolved_id:{check_col}"
        now = datetime.now(timezone.utc)
        records = [
            {
                "source_db": self.db_name,
                "table_name": self.table_name,
                "config_key": self.config_key,
                "reason": reason,
                "row_data": json.dumps(
                    {k: "None" if v is None else str(v) for k, v in row_dict.items()},
                    default=str,
                ),
                "failed_at": now,
            }
            for row_dict in rows
        ]

        engine = get_cached_failed_rows_engine(self.db_path)
        table = ensure_schema_table(engine, self.db_name)

        for attempt in range(self._MAX_WRITE_RETRIES):
            try:
                with engine.begin() as conn:
                    conn.execute(table.insert(), records)
                nd_logger.info(
                    f"[InvalidRowHandler] Wrote {len(rows)} failed rows to '{self.db_path}' "
                    f"(table: failed_rows_{self.db_name})."
                )
                return
            except OperationalError as e:
                if "database is locked" in str(e) and attempt < self._MAX_WRITE_RETRIES - 1:
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    nd_logger.warning(
                        f"[InvalidRowHandler] SQLite locked, retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{self._MAX_WRITE_RETRIES})"
                    )
                    time.sleep(delay)
                else:
                    nd_logger.error(f"[InvalidRowHandler] Failed to write to failed_rows DB: {e}")
                    raise
            except Exception as e:
                nd_logger.error(f"[InvalidRowHandler] Failed to write to failed_rows DB: {e}")
                raise

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def handle(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._RESOLVED_ID_COL not in df.columns:
            nd_logger.warning(
                "[InvalidRowHandler] '%s' not in DataFrame — returning as-is.",
                self._RESOLVED_ID_COL,
            )
            return df

        null_count = df.filter(pl.col(self._RESOLVED_ID_COL).is_null()).height
        if null_count == 0:
            nd_logger.info("[InvalidRowHandler] No invalid rows found. Nothing to ignore.")
            return df

        nd_logger.warning(
            "[InvalidRowHandler] %d/%d rows in %s.%s have null '%s' (REJECTING)",
            null_count, df.height, self.db_name, self.table_name, self._RESOLVED_ID_COL,
        )
        invalid_mask = pl.col(self._RESOLVED_ID_COL).is_null()
        ignored_df = df.filter(invalid_mask)
        self._write_to_failed_rows_db(ignored_df.to_dicts(), self._RESOLVED_ID_COL)
        nd_logger.warning(
            "[InvalidRowHandler] Removed %d rows from %s.%s — persisted to '%s'.",
            ignored_df.height, self.db_name, self.table_name, self.db_path,
        )
        return df.filter(~invalid_mask)
