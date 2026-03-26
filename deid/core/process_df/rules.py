import polars as pl
from datetime import timedelta, datetime
from typing import Dict
from enum import Enum
import re          # stdlib – kept for type hints (re.Match, re.Pattern)
try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already available
from dateutil import parser as date_parser
from deid.core.process_df.constants import DATE_PATTERN_GENERAL, ZIP_CODE_PATTERNS
from deid.core.logger import nd_logger


# Module-level cache: (table_name, column_name) -> list of detected strptime formats
_DATE_FORMAT_CACHE: dict[tuple[str, str], list[str]] = {}

_KNOWN_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m/%d/%y",
    "%m-%d-%y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%m.%d.%Y",
    "%Y/%m/%d",
]


def _detect_formats(values: list[str]) -> list[str]:
    """Sample values and return matching strptime formats."""
    detected = []
    for fmt in _KNOWN_DATE_FORMATS:
        for val in values:
            try:
                datetime.strptime(val.strip(), fmt)
                if fmt not in detected:
                    detected.append(fmt)
                break
            except (ValueError, AttributeError):
                continue
    return detected


def _fast_parse(val: str, formats: list[str]) -> datetime | None:
    """Try cached formats first, fall back to dateutil."""
    val = val.strip()
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt)
        except (ValueError, AttributeError):
            continue
    try:
        return date_parser.parse(val)
    except Exception:
        return None


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

    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# ID-replacement rules  (simple column alias — Polars expression, O(N))
# ---------------------------------------------------------------------------

class PatientIDRule(RuleBase):

    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying PatientIDRule for column: {column}")
        if "_resolved_nd_patient_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("_resolved_nd_patient_id").cast(pl.Int64, strict=False).alias(column))
        elif column in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] _resolved_nd_patient_id missing — "
                f"nulling '{column}' to prevent PHI leakage"
            )
            df = df.with_columns(pl.lit(None).cast(df[column].dtype).alias(column))
        return df


class EncounterIDRule(RuleBase):

    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying EncounterIDRule for column: {column}")
        if "nd_encounter_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("nd_encounter_id").cast(pl.Int64, strict=False).alias(column))
        elif column in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] nd_encounter_id missing — "
                f"nulling '{column}' to prevent PHI leakage"
            )
            df = df.with_columns(pl.lit(None).cast(df[column].dtype).alias(column))
        return df


class ReferencePIDRule(RuleBase):

    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying ReferencePIDRule for column: {column}")
        if "_resolved_nd_patient_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("_resolved_nd_patient_id").cast(pl.Int64, strict=False).alias(column))
        elif column in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] _resolved_nd_patient_id missing — "
                f"nulling '{column}' to prevent PHI leakage"
            )
            df = df.with_columns(pl.lit(None).cast(df[column].dtype).alias(column))
        return df


class AppointmentIDRule(RuleBase):

    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying AppointmentIDRule for column: {column}")
        if "nd_appointment_id" in df.columns and column in df.columns:
            df = df.with_columns(pl.col("nd_appointment_id").cast(pl.Int64, strict=False).alias(column))
        elif column in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] nd_appointment_id missing — "
                f"nulling '{column}' to prevent PHI leakage"
            )
            df = df.with_columns(pl.lit(None).cast(df[column].dtype).alias(column))
        return df


# ---------------------------------------------------------------------------
# Mask rule  (broadcast literal — O(1) metadata, O(N) Polars write)
# ---------------------------------------------------------------------------

class MaskRule(RuleBase):

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
    COMPILED_DATE_PATTERN = re.compile(DATE_PATTERN_GENERAL)

    def __init__(self, format_as_datetime: bool = True, is_notes: bool = False):
        self.format_as_datetime = format_as_datetime
        self.is_notes = is_notes


    def _get_offset_list(self, df: pl.DataFrame) -> list:
        """Return a per-row list of offset days.  Used only by the slow path."""
        raise NotImplementedError("Subclasses must implement _get_offset_list()")


    def _get_offset_expr(self, df: pl.DataFrame) -> pl.Expr:
        """Return a Polars Expr for the per-row offset in days.  Used by the fast path."""
        return pl.lit(0)


    def _shift_text(self, text, offset_days, formats=None) -> str:
        """Apply date-offset to every date pattern found in *text*."""
        _NULL_SENTINELS = {"", "0", "null", "nan", "None", "none", None}
        if text in _NULL_SENTINELS:
            return text

        try:
            offset_days = int(offset_days)
        except (ValueError, TypeError):
            offset_days = 0

        formats = formats or []

        def replace_fn(match):
            date_str = match.group(0)
            parsed = _fast_parse(date_str, formats)
            if parsed is None:
                for fmt in ("%m%d%Y", "%d%m%Y"):
                    try:
                        parsed = datetime.strptime(date_str.strip(), fmt)
                        break
                    except Exception:
                        continue
            if parsed is None:
                nd_logger.error(
                    f"[{self.__class__.__name__}] Failed to parse '{date_str}'"
                )
                return date_str

            shifted = parsed + timedelta(days=offset_days)
            if re.search(r"\d{2}:\d{2}:\d{2}", date_str):
                shifted_str = shifted.strftime("%Y-%m-%d %H:%M:%S")
            else:
                shifted_str = shifted.strftime("%Y-%m-%d")
            return f" {shifted_str} " if self.is_notes else shifted_str

        return self.COMPILED_DATE_PATTERN.sub(replace_fn, str(text))


    def _apply_vectorized(self, df: pl.DataFrame, col_name: str, formats: list) -> pl.DataFrame:
        """Vectorized date-shift using Polars native expressions — no Python per-row loops.

        Parses the column with ``str.to_datetime``, adds the offset via ``pl.duration``,
        and formats back — all in a single Rust-level pass.  Non-date values are
        preserved as-is; nulls and empty strings become null.
        """
        fmt = formats[0]
        has_time = any(x in fmt for x in ("%H", "%M", "%S", "%f"))
        output_fmt = "%Y-%m-%d %H:%M:%S" if (self.format_as_datetime or has_time) else "%Y-%m-%d"

        offset_expr = self._get_offset_expr(df)
        col_str = pl.col(col_name).cast(pl.Utf8)
        null_or_empty = col_str.is_null() | (col_str.str.strip_chars().str.len_chars() == 0)
        parsed = col_str.str.to_datetime(format=fmt, strict=False, ambiguous="earliest")
        shifted = (parsed + pl.duration(days=offset_expr)).dt.strftime(output_fmt)

        return df.with_columns(
            pl.when(null_or_empty)
            .then(pl.lit(None, dtype=pl.Utf8))
            .when(parsed.is_not_null())
            .then(shifted)
            .otherwise(col_str)
            .alias(col_name)
        )


    def apply(self, df: pl.DataFrame, column_config: dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying {self.__class__.__name__} for column: {col_name}"
        )
        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not in DataFrame.")
            return df

        # Detect date formats from a sample of this column's values
        table_name = column_config.get("table_name", "")
        cache_key = (table_name, col_name)
        if cache_key not in _DATE_FORMAT_CACHE:
            _s = df[col_name].cast(pl.Utf8).drop_nulls()
            texts_sample = _s.filter(_s.str.strip_chars().str.len_chars() > 0).head(20).to_list()
            _DATE_FORMAT_CACHE[cache_key] = _detect_formats(texts_sample)
        formats = _DATE_FORMAT_CACHE[cache_key]

        # Fast path: fully vectorized Polars — used for structured date columns.
        # Notes columns embed dates inside text, so they still need the regex sub path.
        if not self.is_notes and formats:
            nd_logger.info(
                f"[{self.__class__.__name__}] Vectorized fast path (fmt={formats[0]})."
            )
            return self._apply_vectorized(df, col_name, formats)

        # Slow path: Python loop for notes/embedded dates or unrecognised formats.
        texts = df[col_name].cast(pl.Utf8)
        contains = texts.str.contains(self.COMPILED_DATE_PATTERN.pattern)
        matched = contains.sum()
        nd_logger.info(f"[{self.__class__.__name__}] Found {matched} rows with date patterns.")

        if matched > 0:
            mask_list   = contains.to_list()
            text_list   = texts.to_list()
            offset_list = self._get_offset_list(df)
            result = [
                self._shift_text(t, o, formats) if m else t
                for t, o, m in zip(text_list, offset_list, mask_list)
            ]
            df = df.with_columns(pl.Series(col_name, result, dtype=pl.Utf8))

        if self.format_as_datetime and not self.is_notes:
            _formats = formats
            def _normalize_batch(s: pl.Series, _f=_formats) -> pl.Series:
                results = []
                for val in s.to_list():
                    if val is None or str(val).strip() == "":
                        results.append(None)
                    else:
                        parsed = _fast_parse(str(val), _f)
                        results.append(parsed.strftime("%Y-%m-%d %H:%M:%S") if parsed else None)
                return pl.Series(results, dtype=pl.Utf8)
            df = df.with_columns(
                pl.col(col_name).map_batches(_normalize_batch, return_dtype=pl.Utf8)
            )

        nd_logger.info(f"[{self.__class__.__name__}] Completed.")
        return df


class StaticDateOffsetRule(BaseDateOffsetRule):
    def __init__(self, offset_days: int = 34, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)
        self.static_offset = offset_days

    def _get_offset_list(self, df: pl.DataFrame) -> list:
        return [self.static_offset] * df.height

    def _get_offset_expr(self, df: pl.DataFrame) -> pl.Expr:
        return pl.lit(self.static_offset)


class DateOffsetRule(BaseDateOffsetRule):
    def __init__(self, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)

    def _get_offset_list(self, df: pl.DataFrame) -> list:
        if "_resolved_offset" in df.columns:
            return df["_resolved_offset"].to_list()
        return [0] * df.height

    def _get_offset_expr(self, df: pl.DataFrame) -> pl.Expr:
        if "_resolved_offset" in df.columns:
            return pl.col("_resolved_offset").fill_null(0)
        return pl.lit(0)


# ---------------------------------------------------------------------------
# Patient DOB rule  (extract birth year — row-wise Python UDF)
# ---------------------------------------------------------------------------

class PatientDOBRule(BaseDateOffsetRule):

    def _get_offset_list(self, df: pl.DataFrame) -> list:
        return [0] * df.height  # not used; apply() is overridden


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
            match_val = match.group(0) if match else text
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


    def apply(self, df: pl.DataFrame, column_config: dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying PatientDOBRule for column: {col_name}")
        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not in DataFrame.")
            return df

        texts = df[col_name].cast(pl.Utf8)
        contains = texts.str.contains(self.COMPILED_DATE_PATTERN.pattern)

        if contains.sum() == 0:
            nd_logger.info(
                f"[{self.__class__.__name__}] No date patterns found. Returning original DataFrame."
            )
            return df

        mask_list = contains.to_list()
        text_list = texts.to_list()
        result = [self.extract_year(t) if m else None for t, m in zip(text_list, mask_list)]

        # Store as nullable Int64 (null for rows that had no date or failed to parse).
        df = df.with_columns(pl.Series(col_name, result, dtype=pl.Int64))
        nd_logger.info(f"[{self.__class__.__name__}] Completed.")
        return df


# ---------------------------------------------------------------------------
# ZIP-code masking rule  (vectorized Polars when/then expressions)
# ---------------------------------------------------------------------------

class ZIPCodeRule(RuleBase):
    DEFAULT_COUNTRY = "US"

    def __init__(self):
        self.country = self.DEFAULT_COUNTRY
        self.pattern = self._get_zip_pattern()
        nd_logger.info(
            f"[{self.__class__.__name__}] Initialized with country: {self.country}"
        )


    def _get_zip_pattern(self):
        pattern = ZIP_CODE_PATTERNS.get(self.country)
        if not pattern:
            raise ValueError(
                f"[{self.__class__.__name__}] No ZIP pattern found for country '{self.country}'"
            )
        return pattern


    def mask_zip(self, zip_code) -> str | None:
        if not zip_code or str(zip_code).lower() in ("nan", "none"):
            return None
        zip_code = str(zip_code).strip()
        match = self.pattern.match(zip_code)
        if match:
            return match.group(1)
        return zip_code[:3] if len(zip_code) > 2 else zip_code


    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Applying ZIPCodeRule for column: {col_name}")
        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not found.")
            return df

        col = pl.col(col_name).cast(pl.Utf8).str.strip_chars()
        lowered = col.str.to_lowercase()
        zip_pattern = r"^(\d{3})\d{2}(?:-\d{4})?$"

        df = df.with_columns(
            pl.when(
                pl.col(col_name).is_null()
                | lowered.is_in(["nan", "none", ""])
            )
            .then(pl.lit(None, dtype=pl.Utf8))
            .when(col.str.contains(zip_pattern))
            .then(col.str.extract(r"^(\d{3})", 1))
            .when(col.str.len_chars() > 2)
            .then(col.str.slice(0, 3))
            .otherwise(col)
            .alias(col_name)
        )
        nd_logger.info(f"[{self.__class__.__name__}] Completed.")
        return df
