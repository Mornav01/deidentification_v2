import polars as pl
try:
    import re2 as re  # google-re2: RE2 engine, no catastrophic backtracking, Python 3.13 safe
except ImportError:
    import re  # type: ignore[no-redef]
from typing import Dict
from .utils import GENERIC_REGEX_DICT
from core.process_df.rules import RuleBase, BaseDateOffsetRule
from deIdentification.nd_logger import nd_logger
from core.process_df.constants import DATE_PATTERN_NOTES


def mask_address(match: re.Match) -> str:
    """Replacement function to mask address parts using named groups."""
    nd_logger.debug(f"Matched address: {match.group(0)}")
    groups = match.groupdict()
    zip_prefix = groups.get("zip", "")[:3] if groups.get("zip") else ""
    return f"((HouseNumber)) ((StreetName)), ((City)), ((State)) {zip_prefix}".strip()


class GenericDateShiftRule(BaseDateOffsetRule):
    """Shift dates found in free-text notes columns using the per-patient offset."""

    COMPILED_DATE_PATTERN = re.compile(DATE_PATTERN_NOTES)

    def __init__(self):
        super().__init__(format_as_datetime=False, is_notes=True)

    def _get_offset_list(self, df: pl.DataFrame) -> list:
        if "_resolved_offset" in df.columns:
            return df["_resolved_offset"].to_list()
        return [0] * df.height


class GenericNotesRule(RuleBase):
    """Apply a battery of generic regex-based PHI masks to free-text columns."""

    def apply(self, df: pl.DataFrame, column_config: Dict) -> pl.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying GenericNotesRule for column: {col_name}"
        )

        if col_name not in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Column '{col_name}' not found. Skipping."
            )
            return df

        # Normalise whitespace once up-front (Polars Rust-speed string replace).
        df = df.with_columns(
            pl.col(col_name)
            .cast(pl.Utf8)
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias(col_name)
        )

        for key, rule in GENERIC_REGEX_DICT.items():
            patterns = rule.get("regex")
            masking_value = rule.get("masking_value", "((MASKED))")
            processing_func = rule.get("processing_func", None)

            if not patterns:
                nd_logger.info(
                    f"[{self.__class__.__name__}] No patterns for key '{key}'. Skipping."
                )
                continue

            if not isinstance(patterns, list):
                patterns = [patterns]

            nd_logger.info(
                f"[{self.__class__.__name__}] Applying rule: {key} "
                f"({len(patterns)} pattern(s))."
            )

            if key == "date":
                # Date shifting uses the per-patient offset stored in _resolved_offset.
                date_rule = GenericDateShiftRule()
                df = date_rule.apply(df, column_config)

            elif key == "address":
                # Address masking uses a callable replacement (named-group substitution).
                for pattern in patterns:
                    try:
                        compiled = re.compile(pattern, re.IGNORECASE)
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Invalid address pattern: "
                            f"{pattern!r} ({e})"
                        )
                        continue
                    # map_elements is needed because the replacement is a Python callable
                    # (not a plain string), and Polars str.replace_all only accepts strings.
                    df = df.with_columns(
                        pl.col(col_name).map_elements(
                            lambda text: compiled.sub(mask_address, text)
                            if isinstance(text, str) else text,
                            return_dtype=pl.Utf8,
                        ).alias(col_name)
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Address rule pattern applied."
                    )

            elif processing_func:
                # Custom processing function (e.g. fuzzy replacement).
                for pattern in patterns:
                    compiled = re.compile(pattern, re.IGNORECASE)
                    df = df.with_columns(
                        pl.col(col_name).map_elements(
                            lambda text: processing_func(text, compiled, masking_value)
                            if isinstance(text, str) else text,
                            return_dtype=pl.Utf8,
                        ).alias(col_name)
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Custom rule pattern applied."
                    )

            else:
                # Simple regex replacement — use Polars' Rust-native str.replace_all
                # for pure-regex patterns (no callable replacement).  This is the
                # hot path and significantly faster than Pandas str.replace().
                for pattern in patterns:
                    try:
                        # Validate the pattern first; Polars raises on invalid regex.
                        re.compile(pattern)
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Invalid pattern for key '{key}': "
                            f"{pattern!r} ({e})"
                        )
                        continue
                    df = df.with_columns(
                        pl.col(col_name)
                        .str.replace_all(pattern, masking_value)
                        .alias(col_name)
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Regex replace rule '{key}' applied."
                    )

        nd_logger.info(f"[{self.__class__.__name__}] GenericNotesRule completed.")
        return df
