import polars as pl
import re          # standard lib – re.Match type hint + fallback
try:
    import re2
except ImportError:
    import re as re2  # type: ignore[no-redef]
import itertools
from typing import List
from core.process_df.rules import RuleBase
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select
from core.dbPkg import NDDBHandler
from deIdentification.nd_logger import nd_logger
from dateutil import parser as date_parser
from core.process_df.constants import DATE_PATTERN_NOTES
from core.process_df.exception import RaiseException
from core.process_df.unstruct.genericnotes import GenericNotesRule
from core.process_df.unstruct.xml import deidentify_xml_tags
from core.process_df.unstruct.xml_utils import xml_tag_replacements

Base = declarative_base()


# ---------------------------------------------------------------------------
# PII table loader (returns Polars DataFrame)
# ---------------------------------------------------------------------------

class PIITable:
    """Load PII data from an external PII database into a Polars DataFrame."""

    def __init__(self):
        self.engine = None
        self.master_session = None

    def _get_db_connection(self, connection_string: str):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.master_session = Session()

    def close_connection(self):
        if self.master_session:
            self.master_session.close()
        if self.engine:
            self.engine.dispose()

    def _get_table(
        self, table_name: str, connection_string: str, patient_ids: list[int]
    ) -> pl.DataFrame:
        self._get_db_connection(connection_string)
        metadata = MetaData()
        pii_table = Table(table_name, metadata, autoload_with=self.engine)
        stmt = select(pii_table).where(pii_table.c.patient_id.in_(patient_ids))
        result = self.master_session.execute(stmt)
        rows = result.fetchall()
        columns = list(result.keys())
        self.close_connection()
        if not rows:
            return pl.DataFrame(schema={c: pl.Utf8 for c in columns})
        return pl.DataFrame(
            [list(r) for r in rows],
            schema=columns,
            orient="row",
        )


# ---------------------------------------------------------------------------
# Notes de-identification rule
# ---------------------------------------------------------------------------

class NotesRule(RuleBase):
    """De-identify free-text note columns using patient-specific PII data.

    The rule is heavy: it fetches PII tables on first call and caches them.
    The *DeIdentifier* creates exactly one instance per table task so the NLP
    model and PII data are loaded only once, not once per batch.
    """

    def __init__(self, db_details_obj, key_phi_columns: tuple):
        self.pii_config = db_details_obj.get_pii_config()
        self.pii_db_config = db_details_obj.get_pii_db_config()
        self.secondary_pii_configs = db_details_obj.get_secondary_pii_config()
        self.pii_data_df: pl.DataFrame | None = None
        self.secondary_pii_data_dfs: dict[str, pl.DataFrame] = {}
        self.key_phi_columns = key_phi_columns
        nd_logger.info(f"[{self.__class__.__name__}] Initialized NotesRule.")

    # ------------------------------------------------------------------
    # Key-PHI column masking (regex replacement in note text)
    # ------------------------------------------------------------------

    def de_identify_key_phi_columns(
        self, df: pl.DataFrame, column_details: dict
    ) -> pl.DataFrame:
        text_column = column_details["column_name"]
        encounter_id_cols, patient_id_cols, reference_pid_cols, appointment_id_cols = (
            self.key_phi_columns
        )
        encounter_id_col = encounter_id_cols[0] if encounter_id_cols else None
        patient_id_col = (
            "_resolved_patient_id" if "_resolved_patient_id" in df.columns else None
        )
        reference_pid_col = reference_pid_cols[0] if reference_pid_cols else None
        appointment_id_col = appointment_id_cols[0] if appointment_id_cols else None

        nd_logger.info(
            f"[{self.__class__.__name__}] Key-PHI de-identification: "
            f"enc={encounter_id_col}, pid={patient_id_col}, "
            f"ref={reference_pid_col}, appt={appointment_id_col}"
        )

        text_list = df[text_column].cast(pl.Utf8).to_list()

        def build_replacements(row: dict) -> dict:
            replacements = {}

            # ----------------------------------------------------------------
            # dict.get(key, default) only uses `default` when the KEY is absent.
            # If the column exists but the value is None (no mapping found),
            # dict.get returns None — and str(None) = "None" which would corrupt
            # the notes text.  Always use the explicit `if val is not None` form.
            # ----------------------------------------------------------------

            if encounter_id_col and row.get(encounter_id_col) is not None:
                original = str(row[encounter_id_col])
                nd_enc = row.get("nd_encounter_id")
                replacement = str(nd_enc) if nd_enc is not None else "((ENCOUNTER_ID))"
                replacements[re2.escape(original)] = replacement

            if appointment_id_col and row.get(appointment_id_col) is not None:
                original = str(row[appointment_id_col])
                nd_appt = row.get("nd_appointment_id")
                replacement = str(nd_appt) if nd_appt is not None else "((APPOINTMENT_ID))"
                replacements[re2.escape(original)] = replacement

            for col in [patient_id_col, reference_pid_col]:
                if col and row.get(col) is not None:
                    original = str(row[col])
                    nd_pid = row.get("_resolved_nd_patient_id")
                    replacement = str(int(nd_pid)) if nd_pid is not None else "((PATIENT_ID))"
                    replacements[re2.escape(original)] = replacement

            return replacements

        rows_as_dicts = df.to_dicts()
        result = []
        for text, row in zip(text_list, rows_as_dicts):
            replacements = build_replacements(row)
            for pattern, repl in replacements.items():
                try:
                    # \b is RE2-safe and equivalent to (?<!\d)…(?!\d) for numeric IDs.
                    # Also fixes a latent bug: Polars str.replace_all (Rust/RE2) rejects
                    # lookbehind, so any path that forwarded this pattern to Polars would
                    # have silently failed.
                    text = re2.sub(rf"\b{pattern}\b", repl, text)
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] Regex error for pattern {pattern}: {e}"
                    )
            result.append(text)

        df = df.with_columns(pl.Series(text_column, result, dtype=pl.Utf8))
        nd_logger.info(
            f"[{self.__class__.__name__}] Key-PHI columns de-identified in '{text_column}'."
        )
        return df

    # ------------------------------------------------------------------
    # PII table loading
    # ------------------------------------------------------------------

    def _get_pii_data_table(self, patient_ids: list):
        if not self.pii_db_config:
            raise RaiseException("pii_db_config is not defined")
        connection_string = self.pii_db_config.get("master_connection_str")
        if not connection_string:
            raise RaiseException("master_connection_str not found in pii_db_config")
        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching primary PII data "
            f"for {len(patient_ids)} patients..."
        )
        pii_table_loader = PIITable()
        self.pii_data_df = pii_table_loader._get_table(
            "pii_data_table", connection_string, patient_ids
        )

    def _get_secondary_pii_data_table(self, patient_ids: list):
        if not self.pii_db_config or not self.secondary_pii_configs:
            return
        connection_string = self.pii_db_config.get("secondary_pii_connection_str")
        if not connection_string:
            return
        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching secondary PII data "
            f"for {len(patient_ids)} patients..."
        )
        for table_config in self.secondary_pii_configs:
            table_name = table_config.get("table_name")
            if not table_name:
                continue
            pii_table_loader = PIITable()
            self.secondary_pii_data_dfs[table_name] = pii_table_loader._get_table(
                table_name, connection_string, patient_ids
            )

    # ------------------------------------------------------------------
    # Primary PII masking
    # ------------------------------------------------------------------

    def deidentify_primary_pii_values(
        self, df: pl.DataFrame, column_details: dict
    ) -> pl.DataFrame:
        text_column = column_details["column_name"]
        if df.is_empty():
            return df

        df = df.with_columns(pl.col(text_column).fill_null(""))

        # Add a cumulative row number per patient to de-duplicate PII records.
        pii_df = self.pii_data_df.clone()
        pii_df = pii_df.with_columns(
            pl.col("patient_id").cum_count().over("patient_id").alias("_row_num")
        )
        max_row_num = pii_df["_row_num"].max()
        nd_logger.info(
            f"[{self.__class__.__name__}] Max PII records per patient: {max_row_num}"
        )

        mask_config = self.pii_config.get("mask", {})
        masked_col: pl.Series = df[text_column]
        continue_masking = True

        try:
            max_row_num = int(max_row_num)
            if max_row_num <= 0:
                raise ValueError("Non-positive row number")
        except (TypeError, ValueError):
            nd_logger.warning(
                f"[{self.__class__.__name__}] No PII records found. "
                "Skipping PII masking."
            )
            continue_masking = False

        if continue_masking:
            for row_num in range(1, max_row_num + 1):
                nd_logger.info(
                    f"[{self.__class__.__name__}] Applying PII from row_num {row_num}..."
                )
                pii_batch = pii_df.filter(pl.col("_row_num") == row_num).drop("_row_num")

                # Left-join: result has same row count as df (one row per source row).
                df_batch = df.join(
                    pii_batch,
                    left_on="_resolved_patient_id",
                    right_on="patient_id",
                    how="left",
                    suffix="_pii",
                )

                masked_col = self._apply_mask_batched(df_batch, masked_col, mask_config)
                masked_col = self._apply_dob(df_batch, masked_col)
                masked_col = self._apply_combine(df_batch, masked_col)

        masked_col = self._apply_regex(masked_col)
        masked_col = self._apply_replace_value(masked_col)
        df = df.with_columns(masked_col.alias(text_column))
        nd_logger.info(
            f"[{self.__class__.__name__}] Primary PII masking completed for '{text_column}'."
        )
        return df

    def deidentify_secondary_pii_values(
        self, df: pl.DataFrame, column_details: dict
    ) -> pl.DataFrame:
        text_column = column_details["column_name"]
        if df.is_empty() or not self.secondary_pii_data_dfs or not self.secondary_pii_configs:
            return df

        df = df.with_columns(pl.col(text_column).fill_null(""))
        masked_col: pl.Series = df[text_column]

        for table_config in self.secondary_pii_configs:
            table_name = table_config.get("table_name")
            if not table_name:
                continue

            pii_df_raw = self.secondary_pii_data_dfs.get(table_name)
            if pii_df_raw is None or pii_df_raw.is_empty():
                nd_logger.info(
                    f"[{self.__class__.__name__}] Skipping empty secondary table '{table_name}'."
                )
                continue

            mask_config = table_config.get("config", {})
            pii_df = pii_df_raw.clone().with_columns(
                pl.col("patient_id").cum_count().over("patient_id").alias("_row_num")
            )
            max_row_num = int(pii_df["_row_num"].max())
            nd_logger.info(
                f"[{self.__class__.__name__}] [{table_name}] Max PII per patient: {max_row_num}"
            )

            for row_num in range(1, max_row_num + 1):
                pii_batch = pii_df.filter(pl.col("_row_num") == row_num).drop("_row_num")
                df_batch = df.join(
                    pii_batch,
                    left_on="_resolved_patient_id",
                    right_on="patient_id",
                    how="left",
                    suffix="_pii",
                )
                masked_col = self._apply_mask_batched(df_batch, masked_col, mask_config)

        df = df.with_columns(masked_col.alias(text_column))
        nd_logger.info(
            f"[{self.__class__.__name__}] Secondary PII masking completed for '{text_column}'."
        )
        return df

    # ------------------------------------------------------------------
    # Masking helpers  (operate on Polars Series)
    # ------------------------------------------------------------------

    def _apply_mask_batched(
        self,
        df_batch: pl.DataFrame,
        masked_col: pl.Series,
        mask_config: dict,
    ) -> pl.Series:
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying exact-match masking from PII source..."
        )
        if not mask_config:
            return masked_col

        pii_columns = [col for col in mask_config if col in df_batch.columns]
        if not pii_columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No matching PII columns for masking."
            )
            return masked_col

        nd_logger.debug(
            f"[{self.__class__.__name__}] Masking PII columns: {pii_columns}"
        )

        # Build per-row replacement maps from the PII columns.
        pii_rows = (
            df_batch.select(pii_columns)
            .cast(pl.Utf8)
            .fill_null("")
            .to_dicts()
        )
        pii_replacements = []
        for row in pii_rows:
            replacement_map: dict = {}
            for col in pii_columns:
                val = row[col].strip()
                if not val:
                    continue
                word_count = len(val.split())
                config_min_words = mask_config[col].get("min_length", 0)
                min_allowed_words = max(config_min_words, 3)
                if word_count < min_allowed_words:
                    continue
                pattern = rf"(?i)\b{re2.escape(val)}\b"
                replacement_map[pattern] = mask_config[col]["masking_value"]
            pii_replacements.append(replacement_map)

        def replace_row(text: str, replacements: dict) -> str:
            if not replacements:
                return text
            for pattern, repl in replacements.items():
                try:
                    text = re2.sub(pattern, repl, text)
                except Exception:
                    pass
            return text

        text_list = masked_col.to_list()
        result = [replace_row(t, rmap) for t, rmap in zip(text_list, pii_replacements)]
        nd_logger.info(f"[{self.__class__.__name__}] Exact-match masking completed.")
        return pl.Series(result, dtype=pl.Utf8)

    def _apply_dob(self, df_batch: pl.DataFrame, masked_col: pl.Series) -> pl.Series:
        nd_logger.info(f"[{self.__class__.__name__}] Applying DOB masking...")
        dob_config = self.pii_config.get("dob", {})
        if not dob_config:
            return masked_col

        dob_columns = [col for col in dob_config if col in df_batch.columns]
        if not dob_columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No DOB columns found in df_batch."
            )
            return masked_col

        date_pattern = re2.compile(DATE_PATTERN_NOTES)
        dob_rows = df_batch.select(dob_columns).to_dicts()
        dob_replacements_list = []

        for row in dob_rows:
            row_map: dict = {}
            for col in dob_columns:
                val = row[col]
                if val is not None and str(val).strip():
                    try:
                        parsed_dob = date_parser.parse(str(val), fuzzy=True).date()
                        row_map[parsed_dob] = str(parsed_dob.year)
                    except Exception as e:
                        nd_logger.debug(
                            f"[{self.__class__.__name__}] Could not parse DOB '{val}': {e}"
                        )
            dob_replacements_list.append(row_map)

        def replace_dates_in_text(text: str, replacements: dict) -> str:
            if not isinstance(text, str) or not replacements:
                return text

            def replacer(match):
                date_str = match.group(0)
                try:
                    parsed_date = date_parser.parse(date_str, fuzzy=True).date()
                    return replacements.get(parsed_date, date_str)
                except Exception:
                    return date_str

            return date_pattern.sub(replacer, text)

        text_list = masked_col.to_list()
        result = [
            replace_dates_in_text(text, rmap)
            for text, rmap in zip(text_list, dob_replacements_list)
        ]
        nd_logger.info(f"[{self.__class__.__name__}] DOB masking completed.")
        return pl.Series(result, dtype=pl.Utf8)

    def _build_rowwise_patterns(self, df_batch: pl.DataFrame) -> dict:
        combine_config = self.pii_config.get("combine", {})
        if not combine_config:
            return {}

        pattern_map: dict = {}
        for rule_name, rule in combine_config.items():
            cols = rule.get("combine", [])
            masking_value = rule.get("masking_value", "")
            cols = [col for col in cols if col in df_batch.columns]
            if not cols:
                continue

            def generate_patterns(row: dict) -> List[str]:
                values = [
                    str(row[col]).strip()
                    for col in cols
                    if row[col] is not None and str(row[col]).strip()
                ]
                combinations: set = set()
                for r in range(1, len(values) + 1):
                    for perm in itertools.permutations(values, r):
                        combined = "".join(perm).strip().lower()
                        if len(combined) > 2:
                            combinations.add(combined)
                return list(combinations)

            rows_as_dicts = df_batch.select(cols).to_dicts()
            patterns_list = [generate_patterns(row) for row in rows_as_dicts]
            pattern_map[rule_name] = {
                "patterns_list": patterns_list,
                "masking_value": masking_value,
            }
        return pattern_map

    def _apply_combine(
        self, df_batch: pl.DataFrame, masked_col: pl.Series
    ) -> pl.Series:
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying combined-PII masking..."
        )
        pattern_map = self._build_rowwise_patterns(df_batch)
        if not pattern_map:
            return masked_col

        text_list = masked_col.to_list()
        for rule_name, info in pattern_map.items():
            patterns_list: list[List[str]] = info["patterns_list"]
            masking_value: str = info["masking_value"]

            def mask_row(note_text: str, patterns: List[str]) -> str:
                if not patterns or not isinstance(note_text, str):
                    return note_text
                try:
                    sorted_patterns = sorted(patterns, key=len, reverse=True)
                    compiled = re2.compile(
                        "(?i)" + "|".join(rf"\b{re2.escape(p)}\b" for p in sorted_patterns),
                    )
                    return compiled.sub(masking_value, note_text)
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] [{rule_name}] Regex failed: {e}"
                    )
                    return note_text

            text_list = [mask_row(t, p) for t, p in zip(text_list, patterns_list)]

        nd_logger.info(
            f"[{self.__class__.__name__}] Combined-PII masking completed."
        )
        return pl.Series(text_list, dtype=pl.Utf8)

    def _apply_regex(self, masked_col: pl.Series) -> pl.Series:
        nd_logger.info(f"[{self.__class__.__name__}] Applying regex-based masking...")
        regex_config = self.pii_config.get("regex", {})
        if not regex_config:
            return masked_col

        for key, conf in regex_config.items():
            patterns = conf["regex"] if isinstance(conf["regex"], list) else [conf["regex"]]
            masking_value = conf["masking_value"]
            for pat in patterns:
                try:
                    normalized_pat = pat.lstrip() if isinstance(pat, str) else pat
                    # Polars str.replace_all is Rust-native — significantly faster
                    # than Pandas str.replace() for large note columns.
                    masked_col = masked_col.str.replace_all(normalized_pat, masking_value)
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] Regex failed for '{pat}': {e}"
                    )
        return masked_col

    def _apply_replace_value(self, masked_col: pl.Series) -> pl.Series:
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying static string replacements..."
        )
        replace_rules = self.pii_config.get("replace_value", [])
        if not replace_rules:
            return masked_col

        for rule in replace_rules:
            old_value = rule.get("old_value")
            new_value = rule.get("new_value")
            if old_value and new_value:
                # \b is RE2-safe. The original (?<![A-Za-z0-9])…(?![A-Za-z0-9]) used
                # lookbehind which is unsupported in both google-re2 AND Polars' Rust
                # regex engine, so it was already silently failing via the except branch.
                pattern = r"(?i)\b{}\b".format(re2.escape(str(old_value)))
                try:
                    masked_col = masked_col.str.replace_all(pattern, new_value)
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] Replace-value pattern failed: {e}"
                    )
        return masked_col

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def apply(self, df: pl.DataFrame, column_details: dict) -> pl.DataFrame:
        """De-identify a note column using patient-specific PII data and generic rules."""
        nd_logger.info(
            f"[{self.__class__.__name__}] Starting de-identification for {df.height} rows..."
        )
        text_column = column_details.get("column_name")
        if not text_column or text_column not in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Column '{text_column}' not found. Skipping."
            )
            return df

        # Normalise text: collapse whitespace, strip control chars.
        df = df.with_columns(
            pl.col(text_column)
            .map_elements(
                lambda x: "" if x is None else str(x),
                return_dtype=pl.Utf8,
            )
            .str.replace_all(r"\s+", " ")
            .str.replace_all(r"\^", " ")
            .str.strip_chars()
            .alias(text_column)
        )

        # XML-tag masking (row-wise Python; only applies to XML-shaped notes).
        df = df.with_columns(
            pl.col(text_column).map_elements(
                lambda text: deidentify_xml_tags(text, xml_tag_replacements),
                return_dtype=pl.Utf8,
            ).alias(text_column)
        )
        nd_logger.info(
            f"[{self.__class__.__name__}] XML tag masking applied for '{text_column}'."
        )

        if "_resolved_patient_id" in df.columns:
            patient_ids = (
                df["_resolved_patient_id"].drop_nulls().unique().to_list()
            )

            df = self.de_identify_key_phi_columns(df, column_details)

            if self.pii_data_df is None:
                self._get_pii_data_table(patient_ids)

            if not self.secondary_pii_data_dfs:
                self._get_secondary_pii_data_table(patient_ids)

            df = self.deidentify_primary_pii_values(df, column_details)
            df = self.deidentify_secondary_pii_values(df, column_details)

        df = GenericNotesRule().apply(df, column_details)
        return df
