import polars as pl
from datetime import timedelta, datetime
from typing import Dict
from enum import Enum
import re          # standard lib – re.Match type hint + fallback
from pydantic import validate_call
try:
    import re2
except ImportError:
    import re as re2  # type: ignore[no-redef]
from dateutil import parser as date_parser
from deid.core.process_df.constants import DATE_PATTERN_GENERAL, ZIP_CODE_PATTERNS
from deid.core.logger import nd_logger


class Rules(Enum):
    PATIENT_ID = "PATIENT_ID"
    ENCOUNTER_ID = "ENCOUNTER_ID"
    REFERENCE_PID = "REFERENCE_PID"
    APPOINTMENT_ID = "APPOINTMENT_ID"
    MASK = "MASK"
    DATE_OFFSET = "DATE_OFFSET"
    STATIC_OFFSET = "STATIC_OFFSET"
    ZIP_CODE = "ZIP_CODE"
    PATIENT_DOB = "PATIENT_DOB"
    GENERIC_NOTES = "GENERIC_NOTES"
    NOTES = "NOTES"


class RuleBase:
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# ID-replacement rules  (simple column alias — Polars expression, O(N))
# ---------------------------------------------------------------------------

class PatientIDRule(RuleBase):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying PatientIDRule for column: {column}")
        if "_resolved_nd_patient_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("_resolved_nd_patient_id").alias(column))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Missing _resolved_nd_patient_id or {column}"
            )
        return df


class EncounterIDRule(RuleBase):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying EncounterIDRule for column: {column}")
        if "nd_encounter_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("nd_encounter_id").alias(column))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Missing nd_encounter_id or {column}"
            )
        return df


class ReferencePIDRule(RuleBase):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying ReferencePIDRule for column: {column}")
        if "_resolved_nd_patient_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("_resolved_nd_patient_id").alias(column))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Missing _resolved_nd_patient_id or {column}"
            )
        return df


class AppointmentIDRule(RuleBase):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying AppointmentIDRule for column: {column}")
        if "nd_appointment_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("nd_appointment_id").alias(column))
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Missing nd_appointment_id or {column}"
            )
        return df


# ---------------------------------------------------------------------------
# Mask rule  (broadcast literal — O(1) metadata, O(N) Polars write)
# ---------------------------------------------------------------------------

class MaskRule(RuleBase):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        mask_value = column_config.get("mask_value", "<<>>")
        nd_logger.info(
            f"[{self.__class__.__name__}] Masking column '{column}' with '<<{mask_value}>>'"
        )
        if column in df.columns:
            df = df.with_columns(pl.lit(f"<<{mask_value}>>").alias(column))
        else:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{column}' not found.")
        return df


# ---------------------------------------------------------------------------
# Date-offset rules  (row-wise Python UDF — same speed as Pandas apply)
# ---------------------------------------------------------------------------

@validate_call(config=dict(arbitrary_types_allowed=True))
def _normalize_to_mysql_datetime(val) -> str | None:
    """Convert any date-like string to ``YYYY-MM-DD HH:MM:SS`` or None."""
    if val is None or str(val).strip() == "":
        return None
    try:
        parsed = date_parser.parse(str(val))
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


class BaseDateOffsetRule(RuleBase):
    COMPILED_DATE_PATTERN = re2.compile(DATE_PATTERN_GENERAL)

    def __init__(self, format_as_datetime: bool = True, is_notes: bool = False):
        self.format_as_datetime = format_as_datetime
        self.is_notes = is_notes

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _get_offset_list(self, df: pl.DataFrame) -> list:
        """Return a per-row list of offset days.  Subclasses must override."""
        raise NotImplementedError("Subclasses must implement _get_offset_list()")

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _shift_text(self, text, offset_days) -> str:
        """Apply date-offset to every date pattern found in *text*."""
        _NULL_SENTINELS = {"", "0", "null", "nan", "None", "none", None}
        if text in _NULL_SENTINELS:
            return text

        try:
            offset_days = int(offset_days)
        except (ValueError, TypeError):
            offset_days = 0

        @validate_call(config=dict(arbitrary_types_allowed=True))
        def replace_fn(match):
            date_str = match.group(0)
            try:
                parsed = date_parser.parse(date_str)
                shifted = parsed + timedelta(days=offset_days)
                if re2.search(r"\d{2}:\d{2}:\d{2}", date_str):
                    shifted_str = shifted.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    shifted_str = shifted.strftime("%Y-%m-%d")
                return f" {shifted_str} " if self.is_notes else shifted_str
            except Exception as e:
                for fmt in ("%m%d%Y", "%d%m%Y"):
                    try:
                        parsed = datetime.strptime(str(match.group(0)).strip(), fmt)
                        shifted = parsed + timedelta(days=offset_days)
                        shifted_str = shifted.strftime("%Y-%m-%d")
                        return f" {shifted_str} " if self.is_notes else shifted_str
                    except Exception:
                        continue
                nd_logger.error(
                    f"[{self.__class__.__name__}] Failed to parse '{date_str}': {e}"
                )
                return date_str

        return self.COMPILED_DATE_PATTERN.sub(replace_fn, str(text))

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying {self.__class__.__name__} for column: {col_name}"
        )
        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not in DataFrame.")
            return df

        # Build string representation and find which rows have date patterns.
        texts = df[col_name].cast(pl.Utf8)
        mask_list = texts.str.contains(self.COMPILED_DATE_PATTERN.pattern).to_list()
        matched = sum(1 for m in mask_list if m)
        nd_logger.info(f"[{self.__class__.__name__}] Found {matched} rows with date patterns.")

        if matched > 0:
            text_list = texts.to_list()
            offset_list = self._get_offset_list(df)
            result = [
                self._shift_text(t, o) if m else t
                for t, o, m in zip(text_list, offset_list, mask_list)
            ]
            df = df.with_columns(pl.Series(col_name, result, dtype=pl.Utf8))
        else:
            nd_logger.info(
                f"[{self.__class__.__name__}] No rows matched date patterns. Skipping shift."
            )

        # Optional: normalise result to MySQL DATETIME format.
        if self.format_as_datetime:
            df = df.with_columns(
                pl.col(col_name).map_elements(
                    _normalize_to_mysql_datetime, return_dtype=pl.Utf8
                )
            )

        nd_logger.info(f"[{self.__class__.__name__}] Completed.")
        return df


class StaticDateOffsetRule(BaseDateOffsetRule):
    def __init__(self, offset_days: int = 34, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)
        self.static_offset = offset_days

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _get_offset_list(self, df: pl.DataFrame) -> list:
        return [self.static_offset] * df.height


class DateOffsetRule(BaseDateOffsetRule):
    def __init__(self, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _get_offset_list(self, df: pl.DataFrame) -> list:
        if "_resolved_offset" in df.columns:
            return df["_resolved_offset"].to_list()
        return [0] * df.height


# ---------------------------------------------------------------------------
# Patient DOB rule  (extract birth year — row-wise Python UDF)
# ---------------------------------------------------------------------------

class PatientDOBRule(BaseDateOffsetRule):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _get_offset_list(self, df: pl.DataFrame) -> list:
        return [0] * df.height  # not used; apply() is overridden

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def extract_year(self, text: str):
        """Parse *text* and return the 4-digit birth year, or None on failure."""
        if not text or str(text).strip().lower() in ("none", "nan", ""):
            return None
        try:
            match = self.COMPILED_DATE_PATTERN.search(text)
            if match:
                parsed = date_parser.parse(match.group(0))
                return parsed.year
        except Exception as e:
            match_val = match.group(0) if "match" in dir() and match else text
            for fmt in ("%m%d%Y", "%d%m%Y"):
                try:
                    parsed = datetime.strptime(str(match_val).strip(), fmt)
                    return parsed.year
                except Exception:
                    continue
            nd_logger.error(
                f"[{self.__class__.__name__}] Failed to extract year from '{text}': {e}"
            )
        return None

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying PatientDOBRule for column: {col_name}")
        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not in DataFrame.")
            return df

        texts = df[col_name].cast(pl.Utf8)
        mask_list = texts.str.contains(self.COMPILED_DATE_PATTERN.pattern).to_list()

        if not any(mask_list):
            nd_logger.info(
                f"[{self.__class__.__name__}] No date patterns found. Returning original DataFrame."
            )
            return df

        text_list = texts.to_list()
        result = [self.extract_year(t) if m else None for t, m in zip(text_list, mask_list)]

        # Store as nullable Int64 (null for rows that had no date or failed to parse).
        df = df.with_columns(pl.Series(col_name, result, dtype=pl.Int64))
        nd_logger.info(f"[{self.__class__.__name__}] Completed.")
        return df


# ---------------------------------------------------------------------------
# ZIP-code masking rule  (element-wise map — Polars map_elements)
# ---------------------------------------------------------------------------

class ZIPCodeRule(RuleBase):
    DEFAULT_COUNTRY = "US"

    def __init__(self):
        self.country = self.DEFAULT_COUNTRY
        self.pattern = self._get_zip_pattern()
        nd_logger.info(
            f"[{self.__class__.__name__}] Initialized with country: {self.country}"
        )

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _get_zip_pattern(self):
        pattern = ZIP_CODE_PATTERNS.get(self.country)
        if not pattern:
            raise ValueError(
                f"[{self.__class__.__name__}] No ZIP pattern found for country '{self.country}'"
            )
        return pattern

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def mask_zip(self, zip_code) -> str | None:
        if not zip_code or str(zip_code).lower() in ("nan", "none"):
            return None
        zip_code = str(zip_code).strip()
        match = self.pattern.match(zip_code)
        if match:
            return match.group(1)
        return zip_code[:3] if len(zip_code) > 2 else zip_code

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying ZIPCodeRule for column: {col_name}")
        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not found.")
            return df

        df = df.with_columns(
            pl.col(col_name)
            .cast(pl.Utf8)
            .map_elements(self.mask_zip, return_dtype=pl.Utf8)
            .alias(col_name)
        )
        nd_logger.info(f"[{self.__class__.__name__}] Completed.")
        return df
