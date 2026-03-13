import polars as pl
import re          # standard lib – re.Match type hint + fallback
try:
    import re2
except ImportError:
    import re as re2  # type: ignore[no-redef]
from typing import Dict
from .utils import GENERIC_REGEX_DICT
from deid.core.process_df.rules import RuleBase, BaseDateOffsetRule
from deid.core.logger import nd_logger
from deid.core.process_df.constants import DATE_PATTERN_NOTES


# Cache: pattern_string -> (is_re2_compatible, compiled_re2_or_None, compiled_stdlib_or_None)
_REGEX_CACHE: dict[str, tuple[bool, object, re.Pattern | None]] = {}


def _get_compiled(pattern: str) -> tuple[bool, object, re.Pattern | None]:
    """Return cached compiled regex, testing RE2 compatibility on first call."""
    if pattern in _REGEX_CACHE:
        return _REGEX_CACHE[pattern]
    try:
        compiled_re2 = re2.compile(pattern)
        _REGEX_CACHE[pattern] = (True, compiled_re2, None)
    except Exception:
        try:
            compiled_std = re.compile(pattern)
            _REGEX_CACHE[pattern] = (False, None, compiled_std)
        except Exception:
            _REGEX_CACHE[pattern] = (False, None, None)
    return _REGEX_CACHE[pattern]


_DELIMITERS = ["\x00", "\x01", "\x02", "\x03", "\x00\x01\x00"]


def _pick_delimiter(series: pl.Series) -> str:
    """Return a delimiter string not present anywhere in the series data."""
    for delim in _DELIMITERS:
        if not series.str.contains(re.escape(delim)).any():
            return delim
    return _DELIMITERS[-1]


def _can_match_empty(pattern: str) -> bool:
    """Return True if the pattern can match the empty string."""
    try:
        return re.match(pattern, "") is not None
    except Exception:
        return True


def mask_address(match: re.Match) -> str:
    """Replacement function to mask address parts using named groups."""
    nd_logger.debug(f"Matched address: {match.group(0)}")
    groups = match.groupdict()
    zip_prefix = groups.get("zip", "")[:3] if groups.get("zip") else ""
    return f"((HouseNumber)) ((StreetName)), ((City)), ((State)) {zip_prefix}".strip()


class GenericDateShiftRule(BaseDateOffsetRule):
    """Shift dates found in free-text notes columns using the per-patient offset."""

    COMPILED_DATE_PATTERN = re2.compile(DATE_PATTERN_NOTES)

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
                        compiled = re2.compile(f"(?i){pattern}")
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
                    compiled = re2.compile(f"(?i){pattern}")
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
                # Simple regex replacement.
                # Fast path: RE2-compatible patterns → Polars str.replace_all (Rust regex).
                # Fallback: patterns with lookahead/lookbehind that RE2 can't compile →
                #   standard `re.sub` via map_elements.  Slower but correct.
                for pattern in patterns:
                    is_re2, compiled_re2, compiled_std = _get_compiled(pattern)
                    if is_re2:
                        df = df.with_columns(
                            pl.col(col_name)
                            .str.replace_all(pattern, masking_value)
                            .alias(col_name)
                        )
                        nd_logger.info(
                            f"[{self.__class__.__name__}] Regex replace rule '{key}' applied (RE2 path)."
                        )
                    elif compiled_std:
                        repl = masking_value
                        # Concatenate-and-split: one regex call instead of N
                        if not _can_match_empty(pattern) and df.height > 1:
                            delim = _pick_delimiter(df[col_name])
                            combined = delim.join(df[col_name].fill_null("").to_list())
                            combined = compiled_std.sub(repl, combined)
                            parts = combined.split(delim)
                            if len(parts) != df.height:
                                nd_logger.warning(
                                    f"[{self.__class__.__name__}] Delimiter collision in concat-and-split "
                                    f"for key '{key}'; falling back to row-by-row replacement"
                                )
                                df = df.with_columns(
                                    pl.col(col_name)
                                    .map_elements(
                                        lambda text, _c=compiled_std, _r=repl: _c.sub(_r, text)
                                        if isinstance(text, str) else text,
                                        return_dtype=pl.Utf8,
                                    )
                                    .alias(col_name)
                                )
                            else:
                                df = df.with_columns(
                                    pl.Series(col_name, parts, dtype=pl.Utf8)
                                )
                        else:
                            df = df.with_columns(
                                pl.col(col_name)
                                .map_elements(
                                    lambda text, _c=compiled_std, _r=repl: _c.sub(_r, text)
                                    if isinstance(text, str) else text,
                                    return_dtype=pl.Utf8,
                                )
                                .alias(col_name)
                            )
                        nd_logger.info(
                            f"[{self.__class__.__name__}] Regex replace rule '{key}' applied (stdlib re fallback)."
                        )
                    else:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Pattern for '{key}' failed both RE2 and stdlib re: {pattern!r}"
                        )

        nd_logger.info(f"[{self.__class__.__name__}] GenericNotesRule completed.")
        return df
