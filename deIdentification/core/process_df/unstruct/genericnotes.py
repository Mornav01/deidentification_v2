import pandas as pd
import re
from typing import Dict
from .utils import GENERIC_REGEX_DICT
from core.process_df.rules import RuleBase, BaseDateOffsetRule
from deIdentification.nd_logger import nd_logger
from core.process_df.constants import DATE_PATTERN_NOTES


'''
def mask_address(text: str, compiled_pattern, mask_value: str = "") -> str:
    """Mask an address in the text using named groups."""
    if not isinstance(text, str):
        return text

    def _repl(match):
        zip_prefix = match.group("zip")[:3] if match.group("zip") else ""
        return f"((HouseNumber)) ((StreetName)), ((City)), ((State)) {zip_prefix}"

    return compiled_pattern.sub(_repl, text)
'''

def mask_address(match: re.Match) -> str:
    nd_logger.debug(f"Matched address: {match.group(0)}")
    """Replacement function to mask address parts using named groups."""
    groups = match.groupdict()
    zip_prefix = groups.get("zip", "")[:3] if groups.get("zip") else ""
    return f"((HouseNumber)) ((StreetName)), ((City)), ((State)) {zip_prefix}".strip()



class GenericDateShiftRule(BaseDateOffsetRule):
    COMPILED_DATE_PATTERN = re.compile(DATE_PATTERN_NOTES)

    def __init__(self):
        super().__init__(format_as_datetime=False, is_notes=True)

    def get_offset_series(self, df: pd.DataFrame) -> pd.Series:
        # shared logic
        return df.get("_resolved_offset", pd.Series(0, index=df.index))


class GenericNotesRule(RuleBase):
    def get_patterns(self, key: str) -> Dict:
        return GENERIC_REGEX_DICT.get(key, {})

    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {col_name}")

        df[col_name] = (df[col_name].astype(str).str.replace(r"\s+", " ", regex=True).str.strip())

        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not found in DataFrame. Skipping.")
            return df

        df[col_name] = df[col_name].astype(str)

        for key, rule in GENERIC_REGEX_DICT.items():
            patterns = rule.get("regex")
            masking_value = rule.get("masking_value", "((MASKED))")
            processing_func = rule.get("processing_func", None)

            if not patterns:
                nd_logger.info(f"[{self.__class__.__name__}] No patterns found for key '{key}'. Skipping.")
                continue

            # Normalize to list
            if not isinstance(patterns, list):
                patterns = [patterns]
            
            nd_logger.info(f"[{self.__class__.__name__}] Applying rule: {key}, patterns count: {len(patterns)}")

            if key == "date":
                # Apply GenericDateShiftRule once
                date_rule = GenericDateShiftRule()
                df = date_rule.apply(df, column_config)


            elif key == "address":
                # Apply address masking with each pattern
                for pattern in patterns:
                    try:
                        compiled = re.compile(pattern, re.IGNORECASE)
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Skipping invalid address pattern for key '{key}': "
                            f"{pattern!r} (error: {e})"
                        )
                        continue

                    df[col_name] = df[col_name].str.replace(compiled, mask_address, regex=True)
                    nd_logger.info(f"[{self.__class__.__name__}] Address rule: pattern completed")

            elif processing_func:
                # Apply custom processing function for each pattern
                for pattern in patterns:
                    compiled = re.compile(pattern, re.IGNORECASE)
                    df[col_name] = df[col_name].apply(
                        lambda x: processing_func(x, compiled, masking_value)
                    )
                    nd_logger.info(f"[{self.__class__.__name__}] Custom rule: pattern={pattern[:30]} completed")

            else:
                # Simple replacement path: apply each pattern individually to avoid
                # issues with inline flags when combining patterns.
                for pattern in patterns:
                    try:
                        compiled = re.compile(pattern, re.IGNORECASE)
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Skipping invalid pattern for key '{key}': "
                            f"{pattern!r} (error: {e})"
                        )
                        continue

                    df[col_name] = df[col_name].str.replace(
                        compiled, masking_value, regex=True
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Regex replace rule {key} pattern applied"
                    )

        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        return df
