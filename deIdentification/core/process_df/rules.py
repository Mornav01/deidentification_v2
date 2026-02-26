import pandas as pd
from datetime import timedelta
from typing import Dict, Any
from enum import Enum
import re
from datetime import timedelta, datetime
from dateutil import parser as date_parser
from core.process_df.constants import DATE_PATTERN_GENERAL, ZIP_CODE_PATTERNS
from django.conf import settings
from deIdentification.nd_logger import nd_logger
from numpy import nan



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
    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        raise NotImplementedError


class PatientIDRule(RuleBase):
    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        
        column = column_config['column_name']
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {column}")
        '''
        sources = []
        if "nd_patient_id_from_referencepid_mapping" in df.columns:
            sources.append(df["nd_patient_id_from_referencepid_mapping"])
        if "nd_patient_id_from_patient_mapping" in df.columns:
            sources.append(df["nd_patient_id_from_patient_mapping"])
        if "nd_patient_id_from_encounter_mapping" in df.columns:
            sources.append(df["nd_patient_id_from_encounter_mapping"])

        if sources:
            final_series = sources[0]
            for src in sources[1:]:
                final_series = final_series.fillna(src)
            df[column] = final_series
        else:
            nd_logger.warning("No matching source columns found.")
        '''

        if "_resolved_nd_patient_id" in df.columns and column in df.columns:
            df[column] = df["_resolved_nd_patient_id"]
        else:
            nd_logger.warning(f"[{self.__class__.__name__}] Required columns missing in DataFrame: _resolved_nd_patient_id or {column}")

        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed for column: {column}")

        return df


class EncounterIDRule(RuleBase):
    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {column}")
        if "nd_encounter_id" in df.columns and column in df.columns:
            df[column] = df["nd_encounter_id"]
        else:
            nd_logger.warning(f"[{self.__class__.__name__}] Required columns missing in DataFrame: nd_encounter_id or {column}")
        
        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        return df


class ReferencePIDRule(RuleBase):
    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {column}")
        if "_resolved_nd_patient_id" in df.columns and column in df.columns:
            df[column] = df["_resolved_nd_patient_id"]
        else:
            nd_logger.warning(f"[{self.__class__.__name__}] Required columns missing in DataFrame: _resolved_nd_patient_id or {column}")
        
        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        return df
    

class AppointmentIDRule(RuleBase):
    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        column = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {column}")
        if "nd_appointment_id" in df.columns and column in df.columns:
            df[column] = df["nd_appointment_id"]
        else:
            nd_logger.warning(f"[{self.__class__.__name__}] Required columns missing in DataFrame: nd_appointment_id or {column}")
        
        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        return df


class MaskRule(RuleBase):
    def apply(self, df : pd.DataFrame,  column_config: Dict) -> pd.DataFrame:
        column = column_config["column_name"]
        mask_value = column_config.get("mask_value", "<<>>") 
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {column} with mask value: {mask_value}")
        
        if column in df.columns:
            df[column] = '<<' + mask_value + '>>'
            nd_logger.info(f"[{self.__class__.__name__}] Column '{column}' masked with value: <<{mask_value}>>")
        else:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{column}' not found in DataFrame.")

        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        
        return df


class BaseDateOffsetRule(RuleBase):
    COMPILED_DATE_PATTERN = re.compile(DATE_PATTERN_GENERAL)

    def __init__(self, format_as_datetime: bool = True, is_notes: bool = False):
        self.format_as_datetime = format_as_datetime
        self.is_notes = is_notes

    def get_date_mask(self, df: pd.DataFrame, col_name: str) -> pd.Series:
        col_str = df[col_name].astype(str)
        mask = col_str.str.contains(self.COMPILED_DATE_PATTERN, regex=True, na=False)
        matched_rows = mask.sum()
        nd_logger.info(f"[{self.__class__.__name__}] Found {matched_rows} rows with date patterns.")
        return mask

    def get_offset_series(self, df: pd.DataFrame) -> pd.Series:
        """
        Should return a Series with same index as df indicating the offset for each row.
        """
        raise NotImplementedError("Subclasses must implement get_offset_series()")

    def apply(self, df: pd.DataFrame, column_config: dict) -> pd.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {col_name}")

        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not in DataFrame.")
            return df

        
        mask = self.get_date_mask(df, col_name)

        if mask.any():
            offset_series = self.get_offset_series(df)

            def shift_text_with_offset(text, offset_days):
                if pd.isna(text) or text in ["", 0, "0", None, "null", "nan", "None","none", nan]:
                    return text
                
                try:
                    offset_days = int(offset_days)
                except (ValueError, TypeError):
                    offset_days = 0
                
                def replace_fn(match):
                    date_str = match.group(0)
                    try:
                        parsed = date_parser.parse(date_str)
                        shifted = parsed + timedelta(days=int(offset_days))

                        # Detect if original string has a time component
                        if re.search(r"\d{2}:\d{2}:\d{2}", date_str):
                            shifted_str = shifted.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            shifted_str = shifted.strftime("%Y-%m-%d")

                        if self.is_notes:
                            return f" {shifted_str} "
                        return shifted_str
                    except Exception as e:
                        parsed = None
                        for fmt in ("%m%d%Y", "%d%m%Y"):
                            try:
                                parsed = datetime.strptime(str(match.group(0)).strip(), fmt)
                                shifted = parsed + timedelta(days=int(offset_days))
                                shifted_str = shifted.strftime("%Y-%m-%d")
                                #nd_logger.info(f"[{self.__class__.__name__}] Fallback succeeded for '{date_str}' with format {fmt} -> {shifted_str}")
                                if self.is_notes:
                                    return f" {shifted_str} "
                                return shifted_str
                            except Exception as inner_e:
                                nd_logger.debug(f"[{self.__class__.__name__}] Fallback failed for '{str(match.group(0))}' with {fmt}: {inner_e}")
                                continue
                        nd_logger.error(f"[{self.__class__.__name__}] Failed to parse '{date_str}': {e}")
                        return date_str

                return self.COMPILED_DATE_PATTERN.sub(replace_fn, str(text))#, count=1)

            df.loc[mask, col_name] = df.loc[mask].apply(
                lambda row: shift_text_with_offset(row[col_name], offset_series.get(row.name, 0)),
                axis=1)
            
        else:
            nd_logger.info(f"[{self.__class__.__name__}] No rows matched date patterns. Returning original DataFrame.")

        # Optional datetime formatting based on flag
        if self.format_as_datetime:
            def normalize_to_mysql_datetime(val):
                if pd.isna(val) or str(val).strip() == "":
                    return None
                try:
                    parsed = date_parser.parse(str(val))
                    return parsed.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return None

            # Vectorized apply
            df[col_name] = df[col_name].apply(normalize_to_mysql_datetime)

        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        return df



class StaticDateOffsetRule(BaseDateOffsetRule):
    def __init__(self, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)

        
    def __init__(self, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)
        self.static_offset = settings.DEFAULT_OFFSET_VALUE
        

    def get_offset_series(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series(self.static_offset, index=df.index)



class DateOffsetRule(BaseDateOffsetRule):
    def __init__(self, format_as_datetime: bool = True):
        super().__init__(format_as_datetime=format_as_datetime)


    def get_offset_series(self, df: pd.DataFrame) -> pd.Series:
        return df.get("_resolved_offset", pd.Series(0, index=df.index))



class PatientDOBRule(BaseDateOffsetRule):
    def get_offset_series(self, df: pd.DataFrame) -> pd.Series:
        # Not needed for this rule; return dummy to satisfy base class
        return pd.Series(0, index=df.index)

    def extract_year(self, text: str):
        if not text or text.strip().lower() in ("none", "nan", ""):
            return None
        try:
            match = self.COMPILED_DATE_PATTERN.search(text)
            if match:
                parsed = date_parser.parse(match.group(0))
                return parsed.year
        except Exception as e:
            match_val = match.group(0) if "match" in locals() and match else text
            for fmt in ("%m%d%Y", "%d%m%Y"):
                try:
                    parsed = datetime.strptime(str(match_val).strip(), fmt)
                    return parsed.year
                except Exception as inner_e:
                    nd_logger.debug(
                        f"[{self.__class__.__name__}] Fallback failed for '{match_val}' with {fmt}: {inner_e}"
                    )
                    continue
            nd_logger.error(
                f"[{self.__class__.__name__}] Failed to extract year from '{text}': {e}"
            )
        return None

    def apply(self, df: pd.DataFrame, column_config: dict) -> pd.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {col_name}")

        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not in DataFrame.")
            return df

        # Ensure string type for regex search
        df[col_name] = df[col_name].astype(str)

        mask = self.get_date_mask(df, col_name)
        if not mask.any():
            nd_logger.info(f"[{self.__class__.__name__}] No rows matched date patterns. Returning original DataFrame.")
            return df

        df.loc[mask, col_name] = df.loc[mask, col_name].apply(self.extract_year)

        # Convert everything to numeric (valid years stay, failed ones → NaN)
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed.")
        return df



class ZIPCodeRule(RuleBase):
    DEFAULT_COUNTRY = "US"

    def __init__(self):
        self.country = self.DEFAULT_COUNTRY
        self.pattern = self.get_zip_pattern_for_country()
        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} initialized with country: {self.country}")

    def get_zip_pattern_for_country(self):
        """Returns regex pattern for the default country (US for now)"""
        pattern = ZIP_CODE_PATTERNS.get(self.country)
        if not pattern:
            nd_logger.error(f"[{self.__class__.__name__}] No ZIP pattern found for country '{self.country}'")
            raise ValueError(f"[{self.__class__.__name__}] No ZIP pattern found for country '{self.country}'")
        nd_logger.info(f"[{self.__class__.__name__}] ZIP pattern loaded for country: {self.country}")
        return pattern

    def mask_zip(self, zip_code: str) -> str:
        """Applies masking logic based on ZIP code format"""
        if not zip_code or str(zip_code).lower() in ["nan", "none"]:
            return None

        zip_code = str(zip_code).strip()
        match = self.pattern.match(zip_code)
        if match:
            masked = f"{match.group(1)}"
            #nd_logger.debug(f"[ZIPCodeRule] ZIP '{zip_code}' matched pattern. Masked to: {masked}")
            return masked
        else:
            masked = zip_code[:3] if len(zip_code) > 2 else zip_code
            nd_logger.debug(f"[{self.__class__.__name__}] ZIP '{zip_code}' did not match pattern. Masked to: {masked}")
            return masked

    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {col_name}")

        if col_name not in df.columns:
            nd_logger.warning(f"[{self.__class__.__name__}] Column '{col_name}' not found in DataFrame.")
            return df

        df[col_name] = df[col_name].astype(str).map(self.mask_zip)
        nd_logger.info(f"[{self.__class__.__name__}] {self.__class__.__name__} completed for column: {col_name}")
        return df


