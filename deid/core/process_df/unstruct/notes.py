import polars as pl
import re          # standard lib – re.Match type hint + fallback
try:
    import re2
except ImportError:
    import re as re2  # type: ignore[no-redef]
import itertools
from typing import List
from deid.core.process_df.rules import RuleBase
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select
from deid.core.dbPkg import NDDBHandler
from deid.core.logger import nd_logger
from dateutil import parser as date_parser
from deid.core.process_df.constants import DATE_PATTERN_NOTES
from deid.core.process_df.exception import RaiseException
from deid.core.process_df.unstruct.genericnotes import GenericNotesRule
from deid.core.process_df.unstruct.xml import deidentify_xml_tags
from deid.core.process_df.unstruct.xml_utils import xml_tag_replacements

Base = declarative_base()


def _normalize_pid(val):
    """Coerce patient_id to int so PII-dict keys and source-df values
    always use the same type regardless of Float64/Utf8/Int64 origin."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return val


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

    def __init__(self, pii_config: dict | None, pii_db_conn_str: str | None,
                 secondary_pii_configs: list | None, key_phi_columns: tuple):
        self.pii_config = pii_config
        self.pii_db_config = pii_db_conn_str
        self.secondary_pii_configs = secondary_pii_configs or []
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
        # nd_logger.info(f"[{self.__class__.__name__}] df.columns: {df.columns}")
        # nd_logger.info(f"[{self.__class__.__name__}] df.head(): {df.head()}")

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
                    # _resolved_patient_id may be Float64 after a Polars join →
                    # str(9097.0) = "9097.0" which won't match "9097" in the text.
                    # Normalise to int string the same way we do for nd_pid below.
                    original = str(row[col])
                    nd_pid = row.get("_resolved_nd_patient_id")
                    if nd_pid is not None:
                        try:
                            # Use float() first to handle both "67890" and "67890.0"
                            # (Polars may emit float strings when the column is Float64).
                            replacement = str(int(float(nd_pid)))
                        except (ValueError, TypeError):
                            replacement = str(nd_pid)
                    else:
                        replacement = "((PATIENT_ID))"
                    replacements[re2.escape(original)] = replacement

            return replacements

        rows_as_dicts = df.to_dicts()
        result = []
        for text, row in zip(text_list, rows_as_dicts):
            try:
                replacements = build_replacements(row)
            except Exception as e:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] build_replacements failed for a row: {e}"
                )
                result.append(text)
                continue
            for pattern, repl in replacements.items():
                try:
                    # Use standard re (not re2) here: RE2 doesn't support lookbehind.
                    # (?<!\d){pattern}(?!\d) is the correct semantic — only skip when
                    # the ID is immediately adjacent to another digit (e.g. "12309097"
                    # should NOT replace the embedded 9097).  \b would also exclude
                    # word-chars like "_" which is too restrictive.
                    text = re.sub(rf"(?<!\d){pattern}(?!\d)", repl, text)
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
        """Apply primary-PII masking (names, DOB, combined patterns) to the notes column.

        Old approach: row_num loop → N × Polars JOIN + N × map build + N × regex pass.
        New approach: scan pii_data_df ONCE to build flat {pid → patterns} dicts,
                      then do ONE single pass over the source rows.

        For a 100k-row batch with 33k patients having up to 15 insurance records this
        reduces inner-loop work from ~15 × 300k ≈ 4.5M ops to ~33k + 100k ≈ 133k ops.
        """
        text_column = column_details["column_name"]
        if df.is_empty():
            return df

        pii_df = self.pii_data_df
        if pii_df is None or pii_df.is_empty():
            nd_logger.warning(
                f"[{self.__class__.__name__}] No PII records found. Skipping PII masking."
            )
            masked_col = df[text_column]
            masked_col = self._apply_regex(masked_col)
            masked_col = self._apply_replace_value(masked_col)
            return df.with_columns(masked_col.alias(text_column))

        df = df.with_columns(pl.col(text_column).fill_null(""))

        lookup_col = "_resolved_patient_id"
        has_pid = lookup_col in df.columns
        pid_list = (
            [_normalize_pid(v) for v in df[lookup_col].to_list()]
            if has_pid
            else [None] * df.height
        )

        mask_config   = self.pii_config.get("mask", {})
        dob_config    = self.pii_config.get("dob", {})
        combine_config = self.pii_config.get("combine", {})

        # ── Determine which PII-table columns are actually needed ─────────────
        pii_cols     = [c for c in mask_config     if c in pii_df.columns]
        dob_cols     = [c for c in dob_config      if c in pii_df.columns]
        combine_rules = {}
        for rule_name, rule in combine_config.items():
            requested_cols = rule.get("combine", [])
            matched_cols = [c for c in requested_cols if c in pii_df.columns]
            missing_cols = [c for c in requested_cols if c not in pii_df.columns]
            if missing_cols:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] combine rule '{rule_name}': "
                    f"columns {missing_cols} NOT found in pii_data_table "
                    f"(available: {pii_df.columns}). Only using {matched_cols}."
                )
            if matched_cols:
                combine_rules[rule_name] = {
                    "cols": matched_cols,
                    "masking_value": rule.get("masking_value", ""),
                }
            else:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] combine rule '{rule_name}' DROPPED: "
                    f"none of {requested_cols} exist in pii_data_table."
                )
        nd_logger.info(
            f"[{self.__class__.__name__}] combine_config has {len(combine_config)} rule(s), "
            f"{len(combine_rules)} survived column filtering."
        )

        all_select = list(
            {"patient_id"}
            | set(pii_cols)
            | set(dob_cols)
            | {c for r in combine_rules.values() for c in r["cols"]}
        )
        cast_to_utf8 = {
            c: pl.Utf8 for c in all_select if c != "patient_id"
        }

        nd_logger.info(
            f"[{self.__class__.__name__}] Building primary PII maps from "
            f"{pii_df.height} records for {pii_df['patient_id'].n_unique()} patients…"
        )

        # ── Phase 1: scan pii_data_df ONCE, collecting all records per patient ─
        pid_to_mask:    dict = {}   # pid → {pattern: masking_value}
        pid_to_dobs:    dict = {}   # pid → {date: year_str}
        pid_to_combine: dict = {}   # pid → list[{col: val}]

        for row in (
            pii_df
            .select(all_select)
            .with_columns([pl.col(c).cast(pl.Utf8).fill_null("") for c in cast_to_utf8])
            .to_dicts()
        ):
            pid = _normalize_pid(row.get("patient_id"))

            # --- exact-match mask patterns ---
            if pii_cols:
                entry = pid_to_mask.setdefault(pid, {})
                for col in pii_cols:
                    val = row[col].strip()
                    if not val:
                        continue
                    min_len = mask_config[col].get("min_length", 2)
                    if len(val) <= min_len:
                        continue
                    entry[rf"(?i)\b{re2.escape(val)}\b"] = mask_config[col]["masking_value"]

            # --- DOB ---
            if dob_cols:
                dob_map = pid_to_dobs.setdefault(pid, {})
                for col in dob_cols:
                    val = row[col].strip()
                    if not val:
                        continue
                    try:
                        parsed = date_parser.parse(val, fuzzy=True).date()
                        dob_map[parsed] = str(parsed.year)
                    except Exception:
                        pass

            # --- combine source rows ---
            if combine_rules:
                pid_to_combine.setdefault(pid, []).append(row)

        # Pre-compute combined-pattern compiled regexes per patient.
        # (itertools.permutations across ALL records for that patient.)
        pid_to_compiled_combine: dict = {}   # pid → list[(compiled_re, masking_value)]
        if combine_rules:
            for pid, rows_data in pid_to_combine.items():
                rule_list = []
                for rule_name, rule in combine_rules.items():
                    cols = rule["cols"]
                    masking_value = rule["masking_value"]
                    all_combos: set = set()
                    for rdata in rows_data:
                        values = [
                            str(rdata[c]).strip()
                            for c in cols
                            if rdata.get(c) and str(rdata.get(c, "")).strip()
                        ]
                        for r in range(1, len(values) + 1):
                            for perm in itertools.permutations(values, r):
                                combined = " ".join(perm).strip().lower()
                                if len(combined) > 2:
                                    all_combos.add(combined)
                    if all_combos:
                        try:
                            sorted_pats = sorted(all_combos, key=len, reverse=True)
                            compiled = re2.compile(
                                "(?i)" + "|".join(
                                    rf"\b{re2.escape(p)}\b" for p in sorted_pats
                                )
                            )
                            rule_list.append((compiled, masking_value))
                        except Exception as exc:
                            nd_logger.warning(
                                f"[{self.__class__.__name__}] combine compile failed "
                                f"for pid={pid}: {exc}"
                            )
                pid_to_compiled_combine[pid] = rule_list

        # Log mask diagnostic: how many patterns per sample patient
        if pid_to_mask:
            sample_pid = next(iter(pid_to_mask))
            sample_patterns = pid_to_mask[sample_pid]
            nd_logger.info(
                f"[{self.__class__.__name__}] [MASK DIAGNOSTIC] "
                f"Sample patient_id={sample_pid} has {len(sample_patterns)} mask pattern(s). "
                f"Preview: {dict(list(sample_patterns.items())[:3])}"
            )
            total_patterns = sum(len(v) for v in pid_to_mask.values())
            nd_logger.info(
                f"[{self.__class__.__name__}] [MASK DIAGNOSTIC] "
                f"Total: {total_patterns} patterns across {len(pid_to_mask)} patients."
            )
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] [MASK DIAGNOSTIC] "
                f"pid_to_mask is EMPTY — no exact-match mask patterns built!"
            )

        nd_logger.info(
            f"[{self.__class__.__name__}] PII maps ready — "
            f"mask={len(pid_to_mask)}, dob={len(pid_to_dobs)}, "
            f"combine={len(pid_to_compiled_combine)} patients."
        )

        # Log a sample of combine patterns for verification
        if pid_to_compiled_combine:
            sample_pid = next(iter(pid_to_compiled_combine))
            sample_rules = pid_to_compiled_combine[sample_pid]
            nd_logger.info(
                f"[{self.__class__.__name__}] [COMBINE DIAGNOSTIC] "
                f"Sample patient_id={sample_pid} (type={type(sample_pid).__name__}) "
                f"has {len(sample_rules)} combine regex(es). "
                f"Pattern preview: {sample_rules[0][0].pattern[:200] if sample_rules else 'N/A'}"
            )
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] [COMBINE DIAGNOSTIC] "
                f"pid_to_compiled_combine is EMPTY — no combine patterns built. "
                f"combine_rules={combine_rules}, pid_to_combine has {len(pid_to_combine)} pids."
            )

        # Log source pid_list sample types for type-mismatch detection
        if has_pid:
            sample_src_pids = [p for p in pid_list[:5] if p is not None]
            nd_logger.info(
                f"[{self.__class__.__name__}] [COMBINE DIAGNOSTIC] "
                f"Source _resolved_patient_id sample: {sample_src_pids} "
                f"(types: {[type(p).__name__ for p in sample_src_pids]})"
            )

        # ── Phase 2: ONE pass over source rows ────────────────────────────────
        date_re = re2.compile(DATE_PATTERN_NOTES) if dob_cols else None
        text_list = df[text_column].to_list()
        result: list = []
        combine_hit_count = 0

        for text, pid in zip(text_list, pid_list):
            if not isinstance(text, str):
                result.append(text)
                continue

            # exact-match mask
            for pattern, repl in pid_to_mask.get(pid, {}).items():
                try:
                    text = re2.sub(pattern, repl, text)
                except Exception:
                    pass

            # DOB
            if date_re:
                dob_replacements = pid_to_dobs.get(pid)
                if dob_replacements:
                    def _dob_replacer(match, _repl=dob_replacements):
                        ds = match.group(0)
                        try:
                            return _repl.get(date_parser.parse(ds, fuzzy=True).date(), ds)
                        except Exception:
                            return ds
                    text = date_re.sub(_dob_replacer, text)

            # combine
            text_before = text
            for compiled_re, masking_value in pid_to_compiled_combine.get(pid, []):
                try:
                    text = compiled_re.sub(masking_value, text)
                except Exception:
                    pass
            if text != text_before:
                combine_hit_count += 1

            result.append(text)

        nd_logger.info(
            f"[{self.__class__.__name__}] [COMBINE DIAGNOSTIC] "
            f"Combine replacements applied to {combine_hit_count}/{len(text_list)} rows."
        )

        masked_col = pl.Series(result, dtype=pl.Utf8)
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

        lookup_col = "_resolved_patient_id"
        has_pid = lookup_col in df.columns
        pid_list = (
            [_normalize_pid(v) for v in df[lookup_col].to_list()]
            if has_pid
            else [None] * df.height
        )

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
            pii_cols = [col for col in mask_config if col in pii_df_raw.columns]
            if not pii_cols:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] [{table_name}] "
                    "No matching PII columns in table. Skipping."
                )
                continue

            nd_logger.info(
                f"[{self.__class__.__name__}] [{table_name}] "
                f"Building merged PII maps from {pii_df_raw.height} records…"
            )
            pid_to_map: dict = {}
            select_cols = ["patient_id"] + pii_cols
            for row in (
                pii_df_raw
                .select(select_cols)
                .cast({c: pl.Utf8 for c in pii_cols})
                .fill_null("")
                .to_dicts()
            ):
                pid = _normalize_pid(row["patient_id"])
                entry = pid_to_map.setdefault(pid, {})
                for col in pii_cols:
                    val = row[col].strip()
                    if not val:
                        continue
                    min_len = mask_config[col].get("min_length", 2)
                    if len(val) <= min_len:
                        continue
                    pattern = rf"(?i)\b{re2.escape(val)}\b"
                    entry[pattern] = mask_config[col]["masking_value"]

            nd_logger.info(
                f"[{self.__class__.__name__}] [{table_name}] "
                f"Built maps for {len(pid_to_map)} unique patients."
            )

            # ── Single pass over source rows ─────────────────────────────────
            text_list = masked_col.to_list()
            result: list[str] = []
            for text, pid in zip(text_list, pid_list):
                rmap = pid_to_map.get(pid)
                if not rmap:
                    result.append(text)
                    continue
                for pattern, repl in rmap.items():
                    try:
                        text = re2.sub(pattern, repl, text)
                    except Exception:
                        pass
                result.append(text)
            masked_col = pl.Series(result, dtype=pl.Utf8)

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

        # ── Optimisation: build replacement map ONCE per unique patient ──────
        # A 100k-row batch may have only 33k unique patients.  Many notes rows
        # share the same patient → we'd re-compute the same map hundreds of
        # times.  Instead, build 33k maps (one per distinct patient_id), then
        # look up by _resolved_patient_id for each source row.
        lookup_col = "_resolved_patient_id"
        has_pid = lookup_col in df_batch.columns

        # Select just the columns we need (PII values + patient key).
        select_cols = pii_columns + ([lookup_col] if has_pid else [])
        pii_rows_df = (
            df_batch.select(select_cols)
            .cast({c: pl.Utf8 for c in pii_columns})
            .fill_null("")
        )

        def _build_map(row: dict) -> dict:
            replacement_map: dict = {}
            for col in pii_columns:
                val = row[col].strip()
                if not val:
                    continue
                min_len = mask_config[col].get("min_length", 2)
                if len(val) <= min_len:
                    continue
                pattern = rf"(?i)\b{re2.escape(val)}\b"
                replacement_map[pattern] = mask_config[col]["masking_value"]
            return replacement_map

        if has_pid:
            # Deduplicate: build one map per unique patient (33k instead of 100k).
            pii_cols_only = pii_rows_df.select(pii_columns)
            pid_series = pii_rows_df[lookup_col].to_list()

            # Map patient_id → replacement_map (computed lazily, cached in dict).
            pid_to_map: dict = {}
            for pid, row in zip(pid_series, pii_cols_only.to_dicts()):
                if pid not in pid_to_map:
                    pid_to_map[pid] = _build_map(row)

            pii_replacements = [pid_to_map.get(pid, {}) for pid in pid_series]
            nd_logger.debug(
                f"[{self.__class__.__name__}] Built {len(pid_to_map)} unique PII maps "
                f"for {len(pii_replacements)} rows."
            )
        else:
            # Fallback: no patient key available — build per-row as before.
            pii_replacements = [_build_map(row) for row in pii_rows_df.to_dicts()]

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
            raw_patterns = conf["regex"]
            if raw_patterns is None:
                continue
            patterns = raw_patterns if isinstance(raw_patterns, list) else [raw_patterns]
            masking_value = conf["masking_value"]
            for pat in patterns:
                if not pat:
                    continue
                try:
                    normalized_pat = pat.lstrip() if isinstance(pat, str) else pat
                    masked_col = masked_col.str.replace_all(normalized_pat, masking_value)
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] Regex failed for key='{key}', "
                        f"pattern='{pat[:80]}…': {e}"
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

        # ── De-identification ordering (highest → lowest priority) ──────────
        #
        # 1. KEY PHI VALUES  — replace the actual encounter / patient ID numbers
        #    pulled from the mapping table.  Must run first so the raw identifiers
        #    are removed before any later step could interfere with them.
        #    Runs unconditionally: if mapping has no entry the fallback placeholder
        #    "((ENCOUNTER_ID))" / "((PATIENT_ID))" is used instead.
        #
        # 2. XML TAG MASKING — tag-name-based static replacements
        #    (e.g. <PatientId>, <EncounterId>, <GuarantorName> …).
        #    Provides a second-pass safety net for any XML-tagged PHI that the
        #    value-based step above might have missed.
        #
        # 3. PII TABLE MASKING — patient-specific names, DOB, insurance, etc.
        #    from the external PII database.  Only runs when patient context
        #    (_resolved_patient_id) is available.
        #
        # 4. GENERIC RULES — phone numbers, addresses, dates, SSN, IP addresses,
        #    URLs, etc.  Runs last so earlier steps cannot accidentally block them.
        # ─────────────────────────────────────────────────────────────────────

        # Step 1 ── Key PHI values (encounter/patient IDs from mapping table)
        df = self.de_identify_key_phi_columns(df, column_details)
        nd_logger.info(
            f"[{self.__class__.__name__}] Step 1: key-PHI column replacement done."
        )

        # Step 2 ── XML tag masking
        df = df.with_columns(
            pl.col(text_column).map_elements(
                lambda text: deidentify_xml_tags(text, xml_tag_replacements),
                return_dtype=pl.Utf8,
            ).alias(text_column)
        )
        nd_logger.info(
            f"[{self.__class__.__name__}] Step 2: XML tag masking done for '{text_column}'."
        )

        # Step 3 ── PII table masking (patient-specific)
        if "_resolved_patient_id" in df.columns:
            patient_ids = [
                _normalize_pid(v)
                for v in df["_resolved_patient_id"].drop_nulls().unique().to_list()
            ]
            if self.pii_data_df is None:
                self._get_pii_data_table(patient_ids)
            if not self.secondary_pii_data_dfs:
                self._get_secondary_pii_data_table(patient_ids)
            df = self.deidentify_primary_pii_values(df, column_details)
            df = self.deidentify_secondary_pii_values(df, column_details)
            nd_logger.info(
                f"[{self.__class__.__name__}] Step 3: PII table masking done."
            )

        # Step 4 ── Generic rules (phone, address, dates, SSN, URLs, IPs …)
        df = GenericNotesRule().apply(df, column_details)
        nd_logger.info(
            f"[{self.__class__.__name__}] Step 4: generic rules done."
        )

        return df
