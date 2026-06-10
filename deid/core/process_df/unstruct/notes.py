import polars as pl
import re          # stdlib – kept for type hints (re.Pattern, re.Match)
import datetime
import decimal
try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already available

import itertools
from typing import List
from deid.core.process_df.rules import RuleBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, select
from deid.core.dbPkg import NDDBHandler
from deid.core.logger import nd_logger
from dateutil import parser as date_parser
from deid.core.process_df.constants import DATE_PATTERN_NOTES
from deid.core.process_df.exception import RaiseException
from deid.core.process_df.unstruct.genericnotes import GenericNotesRule
from deid.core.process_df.unstruct.xml import deidentify_xml_tags
from deid.core.process_df.unstruct.xml_utils import xml_tag_replacements

from deid.core.dbPkg.dbhandler import create_read_only_engine
from deid.core.process_df.rules import _fast_parse as _fast_parse_date, _KNOWN_DATE_FORMATS, _fix_two_digit_year


def _normalize_pid(val):
    """Coerce patient_id to int so PII-dict keys and source-df values
    always use the same type regardless of Float64/Utf8/Int64 origin."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return val


def _normalize_pii_value(v):
    """Convert SQLAlchemy/MySQL result values to Polars-safe scalars.
    Handles dates (incl. MySQL zero dates "0000-00-00"), decimals, bytes.
    Avoids ComputeError when Polars infers schema from mixed types."""
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, datetime.date):
        try:
            return v.isoformat()
        except (ValueError, OverflowError):
            return str(v)  # MySQL zero date etc.
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return v


def _match_pii_columns(config_keys: list, pii_columns: list) -> list[tuple[str, str]]:
    """Return [(config_key, actual_col)] for config keys that exist in pii (case-insensitive).
    Handles MySQL/etc returning different casing than config (e.g. users_Ufname vs users_ufname)."""
    result = []
    pii_lower_to_actual = {c.lower(): c for c in pii_columns}
    for config_key in config_keys:
        actual = pii_lower_to_actual.get(config_key.lower())
        if actual is not None:
            result.append((config_key, actual))
    return result


# ---------------------------------------------------------------------------
# PII table loader (returns Polars DataFrame)
# ---------------------------------------------------------------------------

class PIITable:
    """Load PII data from an external PII database into a Polars DataFrame."""

    def __init__(self):
        self.engine = None
        self.master_session = None

    
    def _get_db_connection(self, connection_string: str):
        self.engine = create_read_only_engine(connection_string)
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
        # Normalise all cells (dates, decimals, zero-dates) to avoid Polars ComputeError
        normalised = [[_normalize_pii_value(cell) for cell in row] for row in rows]
        # Force Utf8 schema — PII data is used for string matching; avoids "0000-00-00" type conflicts
        return pl.DataFrame(
            normalised,
            schema={c: pl.Utf8 for c in columns},
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
                 secondary_pii_configs: list | None, key_phi_columns: tuple,
                 possible_patient_identifier_columns: list | None = None):
        self.pii_config = pii_config
        self.pii_db_config = pii_db_conn_str
        self.secondary_pii_configs = secondary_pii_configs or []
        self.pii_data_df: pl.DataFrame | None = None
        self.secondary_pii_data_dfs: dict[str, pl.DataFrame] = {}
        self._known_patient_ids: set = set()
        self.key_phi_columns = key_phi_columns
        self.possible_patient_identifier_columns = possible_patient_identifier_columns or []
        nd_logger.info(f"[{self.__class__.__name__}] Initialized NotesRule.")

    # ------------------------------------------------------------------
    # Key-PHI column masking (regex replacement in note text)
    # ------------------------------------------------------------------

    
    def de_identify_key_phi_columns(
        self, df: pl.DataFrame, column_details: dict
    ) -> pl.DataFrame:
        text_column = column_details["column_name"]
        encounter_id_cols, patient_id_cols, reference_pid_cols, appointment_id_cols, chart_id_cols = (
            self.key_phi_columns
        )
        encounter_id_col = encounter_id_cols[0] if encounter_id_cols else None
        reference_pid_col = reference_pid_cols[0] if reference_pid_cols else None
        appointment_id_col = appointment_id_cols[0] if appointment_id_cols else None

        resolved_identifier_cols = [
            f"_resolved_{x}" for x in self.possible_patient_identifier_columns
            if f"_resolved_{x}" in df.columns
        ]

        nd_logger.info(
            f"[{self.__class__.__name__}] Key-PHI de-identification: "
            f"enc={encounter_id_col}, resolved_ids={resolved_identifier_cols}, "
            f"ref={reference_pid_col}, appt={appointment_id_col}"
        )
        # nd_logger.info(f"[{self.__class__.__name__}] df.columns: {df.columns}")
        # nd_logger.info(f"[{self.__class__.__name__}] df.head(): {df.head()}")

        text_list = df[text_column].cast(pl.Utf8).to_list()

        # Extract only the ~6 ID columns as lists — avoids converting the entire
        # (potentially 50+ column) DataFrame to Python dicts just to read 6 values.
        _cols = df.columns

        def _str_list(col):
            return df[col].cast(pl.Utf8).to_list() if col and col in _cols else [None] * df.height

        def _int_str_list(col):
            """Normalise Float64/Int64 ID column to int-string list (avoids "9097.0")."""
            if not col or col not in _cols:
                return [None] * df.height
            col_s = df[col]
            # Float64 → Int64 → Utf8 strips the decimal (9097.0 → "9097").
            # Where that cast yields null (non-numeric strings), fill_null falls back
            # to the original Utf8 representation; original nulls remain null.
            int_s = col_s.cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8)
            return int_s.fill_null(col_s.cast(pl.Utf8)).to_list()

        enc_orig_list    = _str_list(encounter_id_col)
        nd_enc_list      = _str_list("nd_encounter_id")
        appt_orig_list   = _str_list(appointment_id_col)
        nd_appt_list     = _str_list("nd_appointment_id")
        resolved_id_lists = [_int_str_list(col) for col in resolved_identifier_cols]
        ref_orig_list    = _int_str_list(reference_pid_col)
        nd_pid_list      = _int_str_list("_resolved_nd_patient_id")

        # Pre-compile regex patterns keyed by unique original-ID string.
        # Uses `regex` module (middle tier: supports lookbehind, faster than stdlib re).
        # Avoids re-cache thrashing when >512 unique encounter IDs exist per batch.
        _pattern_cache: dict = {}

        def _compiled(original: str):
            pat = _pattern_cache.get(original)
            if pat is None:
                pat = re.compile(rf"(?<!\d){re.escape(original)}(?!\d)")
                _pattern_cache[original] = pat
            return pat

        result = []
        for i, text in enumerate(text_list):
            if not isinstance(text, str):
                result.append(text)
                continue
            try:
                # Encounter ID
                enc = enc_orig_list[i]
                if enc is not None:
                    nd_enc = nd_enc_list[i]
                    repl = nd_enc if nd_enc is not None else "((ENCOUNTER_ID))"
                    text = _compiled(enc).sub(repl, text)
                # Appointment ID
                appt = appt_orig_list[i]
                if appt is not None:
                    nd_appt = nd_appt_list[i]
                    repl = nd_appt if nd_appt is not None else "((APPOINTMENT_ID))"
                    text = _compiled(appt).sub(repl, text)
                # All resolved identifier values + reference PID share the same replacement.
                nd_pid_repl = nd_pid_list[i] if nd_pid_list[i] is not None else "((PATIENT_ID))"
                for rid_list in resolved_id_lists:
                    rid = rid_list[i]
                    if rid is not None:
                        text = _compiled(rid).sub(nd_pid_repl, text)
                ref = ref_orig_list[i]
                if ref is not None:
                    text = _compiled(ref).sub(nd_pid_repl, text)
            except Exception as e:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] Key-PHI replacement failed for a row: {e}"
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
        nd_logger.info(
            f"[{self.__class__.__name__}] pii_data_table columns: {self.pii_data_df.columns}"
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

        lookup_col = next(
            (f"_resolved_{x}" for x in self.possible_patient_identifier_columns
             if f"_resolved_{x}" in df.columns),
            None,
        )
        has_pid = lookup_col is not None
        pid_list = (
            [_normalize_pid(v) for v in df[lookup_col].to_list()]
            if has_pid
            else [None] * df.height
        )

        mask_config   = self.pii_config.get("mask", {})
        dob_config    = self.pii_config.get("dob", {})
        combine_config = self.pii_config.get("combine", {})
        pii_df_cols = pii_df.columns

        # ── Determine which PII-table columns are actually needed (case-insensitive match) ─
        pii_cols     = _match_pii_columns(list(mask_config.keys()), pii_df_cols)
        dob_cols     = _match_pii_columns(list(dob_config.keys()), pii_df_cols)
        combine_rules = {}
        for rule_name, rule in combine_config.items():
            requested_cols = rule.get("combine", [])
            matched = _match_pii_columns(requested_cols, pii_df_cols)
            matched_actual = [t[1] for t in matched]
            missing = [c for c in requested_cols if not any(c.lower() == m[0].lower() for m in matched)]
            if missing:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] combine rule '{rule_name}': "
                    f"columns {missing} NOT found in pii_data_table "
                    f"(available: {pii_df_cols}). Using {matched_actual}."
                )
            if matched_actual:
                combine_rules[rule_name] = {
                    "cols": matched_actual,
                    "masking_value": rule.get("masking_value", ""),
                }
            else:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] combine rule '{rule_name}' DROPPED: "
                    f"none of {requested_cols} exist in pii_data_table."
                )
        nd_logger.info(
            f"[{self.__class__.__name__}] combine_config has {len(combine_config)} rule(s), "
            f"{len(combine_rules)} survived. "
            f"pii_cols: {[t[0] for t in pii_cols]} (matched {len(pii_cols)}/{len(mask_config)} mask columns)."
        )

        all_select = list(
            {"patient_id"}
            | {t[1] for t in pii_cols}
            | {t[1] for t in dob_cols}
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

        _pii_prepared = (
            pii_df
            .select(all_select)
            .with_columns([pl.col(c).cast(pl.Utf8).fill_null("") for c in cast_to_utf8])
        )
        _pii_col_lists = {col: _pii_prepared[col].to_list() for col in all_select}
        _pid_col = _pii_col_lists.get("patient_id", [None] * _pii_prepared.height)

        for i in range(_pii_prepared.height):
            pid = _normalize_pid(_pid_col[i])

            # --- exact-match mask patterns ---
            if pii_cols:
                entry = pid_to_mask.setdefault(pid, {})
                for config_key, actual_col in pii_cols:
                    val = _pii_col_lists[actual_col][i].strip()
                    if not val:
                        continue
                    min_len = mask_config[config_key].get("min_length", 2)
                    if len(val) <= min_len:
                        continue
                    entry[rf"(?i)\b{re.escape(val)}\b"] = mask_config[config_key]["masking_value"]

            # --- DOB ---
            if dob_cols:
                dob_map = pid_to_dobs.setdefault(pid, {})
                for config_key, actual_col in dob_cols:
                    val = _pii_col_lists[actual_col][i].strip()
                    if not val:
                        continue
                    try:
                        parsed = _fix_two_digit_year(date_parser.parse(val, fuzzy=True), max_future=0).date()
                        dob_map[parsed] = str(parsed.year)
                    except Exception:
                        pass

            # --- combine source rows ---
            if combine_rules:
                pid_to_combine.setdefault(pid, []).append(
                    {col: _pii_col_lists[col][i] for col in all_select}
                )

        # Compile per-patient mask alternation regexes (one regex per masking_value).
        # Replaces N × P individual re.sub calls with N × distinct_values calls.
        pid_to_compiled_mask: dict = {}  # pid → [(compiled_re, masking_value)]
        for pid, patterns in pid_to_mask.items():
            by_value: dict = {}
            for pat, val in patterns.items():
                by_value.setdefault(val, []).append(pat)
            compiled_list = []
            for masking_val, pats in by_value.items():
                try:
                    compiled_list.append((re.compile("|".join(pats)), masking_val))
                except Exception as exc:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] mask compile failed for pid={pid}: {exc}"
                    )
                    for p in pats:
                        try:
                            compiled_list.append((re.compile(p), masking_val))
                        except Exception:
                            pass
            if compiled_list:
                pid_to_compiled_mask[pid] = compiled_list

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
                            compiled = re.compile(
                                "(?i)" + "|".join(
                                    rf"\b{re.escape(p)}\b" for p in sorted_pats
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
                f"Sample patient_id={sample_pid} has {len(sample_patterns)} mask pattern(s)."
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
                f"has {len(sample_rules)} combine regex(es)."
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
        date_re = re.compile(DATE_PATTERN_NOTES) if dob_cols else None
        text_list = df[text_column].to_list()
        result: list = []
        mask_hit_count = 0
        combine_hit_count = 0

        for text, pid in zip(text_list, pid_list):
            if not isinstance(text, str):
                result.append(text)
                continue

            # exact-match mask — use pre-compiled alternation regex per masking_value
            text_before_mask = text
            for compiled_re, repl in pid_to_compiled_mask.get(pid, []):
                try:
                    text = compiled_re.sub(repl, text)
                except Exception:
                    pass
            if text != text_before_mask:
                mask_hit_count += 1

            # DOB
            if date_re:
                dob_replacements = pid_to_dobs.get(pid)
                if dob_replacements:
                    def _dob_replacer(match, _repl=dob_replacements, _fmts=_KNOWN_DATE_FORMATS):
                        ds = match.group(0)
                        parsed = _fast_parse_date(ds, _fmts)
                        if parsed is None:
                            return ds
                        # DOB must never be in the future — fix 2-digit years
                        # (e.g. "1/7/44" → strptime gives 2044, but DOB key is 1944)
                        parsed = _fix_two_digit_year(parsed, max_future=0)
                        return _repl.get(parsed.date(), ds)
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
            f"[{self.__class__.__name__}] [MASK DIAGNOSTIC] Mask replacements applied to {mask_hit_count}/{len(text_list)} rows."
        )
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

        lookup_col = next(
            (f"_resolved_{x}" for x in self.possible_patient_identifier_columns
             if f"_resolved_{x}" in df.columns),
            None,
        )
        has_pid = lookup_col is not None
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
            pii_cols_tuples = _match_pii_columns(list(mask_config.keys()), pii_df_raw.columns)
            if not pii_cols_tuples:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] [{table_name}] "
                    f"No matching PII columns (config: {list(mask_config.keys())}, "
                    f"table: {pii_df_raw.columns}). Skipping."
                )
                continue

            nd_logger.info(
                f"[{self.__class__.__name__}] [{table_name}] "
                f"Building merged PII maps from {pii_df_raw.height} records…"
            )
            pid_to_map: dict = {}
            select_cols = ["patient_id"] + [t[1] for t in pii_cols_tuples]
            _pii2_prepared = (
                pii_df_raw
                .select(select_cols)
                .cast({c: pl.Utf8 for c in select_cols if c != "patient_id"})
                .fill_null("")
            )
            _pii2_col_lists = {col: _pii2_prepared[col].to_list() for col in select_cols}
            _pid2_col = _pii2_col_lists["patient_id"]

            for i in range(_pii2_prepared.height):
                pid = _normalize_pid(_pid2_col[i])
                entry = pid_to_map.setdefault(pid, {})
                for config_key, actual_col in pii_cols_tuples:
                    val = _pii2_col_lists[actual_col][i].strip()
                    if not val:
                        continue
                    min_len = mask_config[config_key].get("min_length", 2)
                    if len(val) <= min_len:
                        continue
                    pattern = rf"(?i)\b{re.escape(val)}\b"
                    entry[pattern] = mask_config[config_key]["masking_value"]

            # ── Compile alternation regexes per patient ────────────────────
            # Group patterns by masking_value, join with |, compile once.
            # Reduces N×P individual re.sub calls to N×distinct_mask_values.
            pid_to_compiled: dict = {}  # pid → [(compiled_re, masking_value)]
            for pid, patterns in pid_to_map.items():
                by_value: dict = {}
                for pat, val in patterns.items():
                    by_value.setdefault(val, []).append(pat)
                compiled_list = []
                for masking_val, pats in by_value.items():
                    try:
                        compiled_list.append((re.compile("|".join(pats)), masking_val))
                    except Exception:
                        for p in pats:
                            try:
                                compiled_list.append((re.compile(p), masking_val))
                            except Exception:
                                pass
                if compiled_list:
                    pid_to_compiled[pid] = compiled_list

            nd_logger.info(
                f"[{self.__class__.__name__}] [{table_name}] "
                f"Built compiled maps for {len(pid_to_compiled)} unique patients."
            )

            # ── Single pass over source rows ─────────────────────────────────
            text_list = masked_col.to_list()
            result: list[str] = []
            for text, pid in zip(text_list, pid_list):
                compiled = pid_to_compiled.get(pid)
                if not compiled:
                    result.append(text)
                    continue
                for compiled_re, repl in compiled:
                    try:
                        text = compiled_re.sub(repl, text)
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
        # look up by the first _resolved_{identifier} for each source row.
        lookup_col = next(
            (f"_resolved_{x}" for x in self.possible_patient_identifier_columns
             if f"_resolved_{x}" in df_batch.columns),
            None,
        )
        has_pid = lookup_col is not None

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
                pattern = rf"(?i)\b{re.escape(val)}\b"
                replacement_map[pattern] = mask_config[col]["masking_value"]
            return replacement_map

        if has_pid:
            # Deduplicate: build one map per unique patient (33k instead of 100k).
            pii_cols_only = pii_rows_df.select(pii_columns)
            pid_series = pii_rows_df[lookup_col].to_list()
            pii_col_lists = {col: pii_cols_only[col].to_list() for col in pii_columns}

            # Map patient_id → replacement_map (computed lazily, cached in dict).
            pid_to_map: dict = {}
            for i, pid in enumerate(pid_series):
                if pid not in pid_to_map:
                    pid_to_map[pid] = _build_map({col: pii_col_lists[col][i] for col in pii_columns})

            pii_replacements = [pid_to_map.get(pid, {}) for pid in pid_series]
            nd_logger.debug(
                f"[{self.__class__.__name__}] Built {len(pid_to_map)} unique PII maps "
                f"for {len(pii_replacements)} rows."
            )
        else:
            # Fallback: no patient key available — build per-row.
            pii_col_lists = {col: pii_rows_df[col].to_list() for col in pii_columns}
            pii_replacements = [
                _build_map({col: pii_col_lists[col][i] for col in pii_columns})
                for i in range(pii_rows_df.height)
            ]

        
        def replace_row(text: str, replacements: dict) -> str:
            if not replacements:
                return text
            for pattern, repl in replacements.items():
                try:
                    text = re.sub(pattern, repl, text)
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

        date_pattern = re.compile(DATE_PATTERN_NOTES)
        dob_col_lists = {col: df_batch[col].to_list() for col in dob_columns}
        n_rows = df_batch.height
        dob_replacements_list = []

        for i in range(n_rows):
            row_map: dict = {}
            for col in dob_columns:
                val = dob_col_lists[col][i]
                if val is not None and str(val).strip():
                    try:
                        parsed_dob = _fix_two_digit_year(date_parser.parse(str(val), fuzzy=True), max_future=0).date()
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
                    parsed_date = _fix_two_digit_year(date_parser.parse(date_str, fuzzy=True), max_future=0).date()
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

            col_lists = {col: df_batch[col].to_list() for col in cols}

            def generate_patterns(i: int) -> List[str]:
                values = [
                    str(col_lists[col][i]).strip()
                    for col in cols
                    if col_lists[col][i] is not None and str(col_lists[col][i]).strip()
                ]
                combinations: set = set()
                for r in range(1, len(values) + 1):
                    for perm in itertools.permutations(values, r):
                        combined = "".join(perm).strip().lower()
                        if len(combined) > 2:
                            combinations.add(combined)
                return list(combinations)

            patterns_list = [generate_patterns(i) for i in range(df_batch.height)]
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

            # Pre-compile once per unique pattern set (many rows share the same patient →
            # same patterns). Avoids N re.compile() calls when U << N unique patients.
            _cache: dict = {}
            compiled_list = []
            for pats in patterns_list:
                if not pats:
                    compiled_list.append(None)
                    continue
                key = tuple(sorted(pats))
                if key not in _cache:
                    try:
                        _cache[key] = re.compile(
                            "(?i)" + "|".join(
                                rf"\b{re.escape(p)}\b"
                                for p in sorted(pats, key=len, reverse=True)
                            )
                        )
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] [{rule_name}] Regex failed: {e}"
                        )
                        _cache[key] = None
                compiled_list.append(_cache[key])

            text_list = [
                cr.sub(masking_value, t) if cr is not None and isinstance(t, str) else t
                for t, cr in zip(text_list, compiled_list)
            ]

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
                normalized_pat = pat.lstrip() if isinstance(pat, str) else pat
                try:
                    masked_col = masked_col.str.replace_all(normalized_pat, masking_value)
                except Exception:
                    try:
                        compiled = re.compile(normalized_pat)
                    except Exception as e:
                        nd_logger.warning(
                            f"[{self.__class__.__name__}] Regex failed for key='{key}', "
                            f"pattern='{pat[:80]}…': {e}"
                        )
                        continue
                    repl = masking_value
                    masked_col = pl.Series(
                        [compiled.sub(repl, t) if isinstance(t, str) else t
                         for t in masked_col.to_list()],
                        dtype=pl.Utf8,
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
                pattern = r"(?i)\b{}\b".format(re.escape(str(old_value)))
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
            .fill_null("")
            .cast(pl.Utf8)
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
        _xml_repl = xml_tag_replacements
        df = df.with_columns(
            pl.col(text_column).map_batches(
                lambda s, _r=_xml_repl: pl.Series(
                    [deidentify_xml_tags(t, _r) for t in s.to_list()],
                    dtype=pl.Utf8,
                ),
                return_dtype=pl.Utf8,
            ).alias(text_column)
        )
        nd_logger.info(
            f"[{self.__class__.__name__}] Step 2: XML tag masking done for '{text_column}'."
        )

        # Step 3 ── PII table masking (patient-specific)
        _pii_lookup_col = next(
            (f"_resolved_{x}" for x in self.possible_patient_identifier_columns
             if f"_resolved_{x}" in df.columns),
            None,
        )
        if _pii_lookup_col is not None:
            patient_ids = [
                _normalize_pid(v)
                for v in df[_pii_lookup_col].drop_nulls().unique().to_list()
            ]
            new_ids = set(patient_ids) - self._known_patient_ids - {None}
            if new_ids:
                self._get_pii_data_table(patient_ids)
                self._get_secondary_pii_data_table(patient_ids)
                self._known_patient_ids.update(new_ids)
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
