import polars as pl
import re          # stdlib – kept for type hints (re.Match, re.Pattern)
try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already available

from typing import Dict
from .utils import GENERIC_REGEX_DICT
from deid.core.process_df.rules import RuleBase, BaseDateOffsetRule
from deid.core.logger import nd_logger
from deid.core.process_df.constants import DATE_PATTERN_NOTES


_REGEX_CACHE: dict[str, re.Pattern | None] = {}


def _get_compiled(pattern: str) -> re.Pattern | None:
    """Return a cached compiled regex, or None if the pattern is invalid."""
    if pattern in _REGEX_CACHE:
        return _REGEX_CACHE[pattern]
    try:
        compiled = re.compile(pattern)
    except Exception:
        compiled = None
    _REGEX_CACHE[pattern] = compiled
    return compiled


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
                        compiled = re.compile(f"(?i){pattern}")
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Invalid address pattern: "
                            f"{pattern!r} ({e})"
                        )
                        continue
                    # map_batches amortizes per-element FFI overhead vs map_elements.
                    # The replacement is a Python callable so we can't use str.replace_all.
                    _c = compiled
                    df = df.with_columns(
                        pl.col(col_name).map_batches(
                            lambda s, _c=_c: pl.Series(
                                [_c.sub(mask_address, t) if isinstance(t, str) else t
                                 for t in s.to_list()],
                                dtype=pl.Utf8,
                            ),
                            return_dtype=pl.Utf8,
                        ).alias(col_name)
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Address rule pattern applied."
                    )

            elif processing_func:
                # Custom processing function (e.g. fuzzy replacement).
                for pattern in patterns:
                    compiled = re.compile(f"(?i){pattern}")
                    _c, _f, _v = compiled, processing_func, masking_value
                    df = df.with_columns(
                        pl.col(col_name).map_batches(
                            lambda s, _c=_c, _f=_f, _v=_v: pl.Series(
                                [_f(t, _c, _v) if isinstance(t, str) else t
                                 for t in s.to_list()],
                                dtype=pl.Utf8,
                            ),
                            return_dtype=pl.Utf8,
                        ).alias(col_name)
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Custom rule pattern applied."
                    )

            else:
                # Simple regex replacement.
                # Fast path: Polars str.replace_all (vectorized Rust regex).
                # Fallback: Python re.sub via map_batches for patterns the
                # Rust engine rejects (e.g. lookbehind).
                for pattern in patterns:
                    compiled = _get_compiled(pattern)
                    if compiled is None:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Invalid pattern for '{key}': {pattern!r}"
                        )
                        continue
                    repl = masking_value
                    try:
                        df = df.with_columns(
                            pl.col(col_name)
                            .str.replace_all(pattern, masking_value)
                            .alias(col_name)
                        )
                    except Exception:
                        # Pattern uses features Polars' Rust regex doesn't
                        # support — fall back to Python regex.
                        df = df.with_columns(
                            pl.col(col_name)
                            .map_batches(
                                lambda s, _c=compiled, _r=repl: pl.Series(
                                    [_c.sub(_r, t) if isinstance(t, str) else t
                                     for t in s.to_list()],
                                    dtype=pl.Utf8,
                                ),
                                return_dtype=pl.Utf8,
                            )
                            .alias(col_name)
                        )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Regex replace rule '{key}' applied."
                    )

        nd_logger.info(f"[{self.__class__.__name__}] GenericNotesRule completed.")
        return df
