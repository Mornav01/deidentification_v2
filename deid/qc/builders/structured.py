from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime, timedelta
from deid.qc.builders.base import Detector
from deid.qc.schema import ColumnQCResult
from pydantic import validate_call


@validate_call(config=dict(arbitrary_types_allowed=True))
def _parse_date(value: Any) -> datetime | None:
    """Parse a date string, returning None if the value is not a valid date."""
    s = str(value).strip()
    if not s or s.lower() in ("none", "null", "nat", ""):
        return None
    # Keep only the date part of a datetime string (e.g. "2020-01-01 00:00:00").
    s = s.split(" ")[0]
    parts = s.split("-")
    if len(parts) != 3:
        return None
    if not all(p.isdigit() for p in parts):
        return None
    try:
        return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError:
        return None


# QC Framework Part 1 — DATE plausibility window: [1900-01-01, today].
_PLAUSIBLE_MIN_DATE = datetime(1900, 1, 1)


def _is_null_like(value: Any) -> bool:
    return value is None or str(value).strip().lower() in ("", "none", "null", "nat")


def _is_plausible_date(d: datetime | None, today: datetime) -> bool:
    """A de-identified date must land within [1900-01-01, today] (doc Part 1 DATE value check)."""
    if d is None:
        return True  # unparseable handled separately by format check
    return _PLAUSIBLE_MIN_DATE <= d <= today


class SZipCodeDetector(Detector):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        # QC Framework Part 1 / Decision D2: ZIP must be exactly 3 chars (Safe-Harbor truncation),
        # or null/empty. (Previously accepted len <= 3.)
        passed_count = sum(
            1 for row in after_rows
            if _is_null_like(row[self.column_name]) or len(str(row[self.column_name]).strip()) == 3
        )
        failed_count = len(after_rows) - passed_count
        return ColumnQCResult(passed_count=passed_count, failed_count=failed_count, remarks={})


class SDobDetector(Detector):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        passed_count = sum(
            1 for row in after_rows
            if (value := str(row[self.column_name]).lower()) in {None, '', 'null', 'none'} or (value.isdigit() and len(value) == 4)
        )
        failed_count = len(after_rows) - passed_count
        return ColumnQCResult(passed_count=passed_count, failed_count=failed_count, remarks={})

DEFAULT_OFFSET_VALUE = 34


class SStaticOffestDetector(Detector):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_offset(self, row: dict):
        return self.qc_config.get("default_offset_value", DEFAULT_OFFSET_VALUE)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        column_name = self.column_config["column_name"]
        today = datetime.now()

        before_dict = {row['nd_auto_increment_id']: row for row in before_rows}

        remarks = []
        format_remarks = []

        for after_row in after_rows:
            nd_id = after_row.get('nd_auto_increment_id')
            before_row = before_dict.get(nd_id)

            if not before_row:
                continue

            after_raw = after_row.get(column_name, '')
            after_date = _parse_date(after_raw)

            # QC Framework Part 1 DATE check: format (YYYY-MM-DD) + plausibility [1900, today].
            if not _is_null_like(after_raw):
                if after_date is None:
                    format_remarks.append({'nd_auto_increment_id': nd_id, 'value': str(after_raw), 'issue': 'bad_format'})
                    column_qc_result['failed_count'] += 1
                    continue
                if not _is_plausible_date(after_date, today):
                    format_remarks.append({'nd_auto_increment_id': nd_id, 'value': str(after_date), 'issue': 'implausible'})
                    column_qc_result['failed_count'] += 1
                    continue

            before_date = _parse_date(before_row.get(column_name, ''))
            if before_date is None or after_date is None:
                continue

            offset_value = self.get_offset(after_row)
            date_diff = (after_date - before_date).days

            if date_diff == offset_value:
                column_qc_result['passed_count'] += 1
            else:
                remarks.append({
                    'nd_auto_increment_id': nd_id,
                    'source date': str(before_date),
                    'dest date': str(after_date)
                })
                column_qc_result['failed_count'] += 1

        column_qc_result['remarks'] = {'remarks': remarks, 'format_remarks': format_remarks}
        return column_qc_result

class SMaskDetector(Detector):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        assert "mask_value" in self.column_config, "column_config must contain 'mask_value'"
        mask_value = self.column_config["mask_value"]
        passed_count = sum(1 for row in after_rows if row[self.column_name] == f'<<{mask_value}>>')
        failed_count = len(after_rows) - passed_count
        return ColumnQCResult(passed_count=passed_count, failed_count=failed_count, remarks={})

class SDateOffestDetector(Detector):
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_offset(self, row: dict):
        # Resolve the patient's date offset from patient_mapping_table, reached either directly
        # via the PATIENT_ID column (dest value = nd_patient_id) or via the ENCOUNTER_ID column
        # (dest value = nd_encounter_id → encounter's nd_patient_id bridge). A missing mapping row
        # falls back to the default offset rather than aborting the whole table's scan.
        default = self.qc_config.get("default_offset_value", DEFAULT_OFFSET_VALUE)
        if self.patient_id_column is not None:
            entry = self.patient_mapping_dict.get(row.get(self.patient_id_column))
            return entry["offset"] if entry else default
        elif self.enc_id_column is not None:
            enc = self.enc_mapping_dict.get(row.get(self.enc_id_column))
            nd_pid = enc.get("patient_id") if enc else None
            entry = self.patient_mapping_dict.get(nd_pid)
            return entry["offset"] if entry else default
        return default

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        column_name = self.column_config["column_name"]
        today = datetime.now()

        before_dict = {row['nd_auto_increment_id']: row for row in before_rows}

        remarks = []
        format_remarks = []

        for after_row in after_rows:
            nd_id = after_row.get('nd_auto_increment_id')
            before_row = before_dict.get(nd_id)

            if not before_row:
                continue

            after_raw = after_row.get(column_name, '')
            after_date = _parse_date(after_raw)

            # QC Framework Part 1 DATE check: format (YYYY-MM-DD) + plausibility [1900, today].
            if not _is_null_like(after_raw):
                if after_date is None:
                    format_remarks.append({'nd_auto_increment_id': nd_id, 'value': str(after_raw), 'issue': 'bad_format'})
                    column_qc_result['failed_count'] += 1
                    continue
                if not _is_plausible_date(after_date, today):
                    format_remarks.append({'nd_auto_increment_id': nd_id, 'value': str(after_date), 'issue': 'implausible'})
                    column_qc_result['failed_count'] += 1
                    continue

            before_date = _parse_date(before_row.get(column_name, ''))
            if before_date is None or after_date is None:
                continue

            offset_value = self.get_offset(after_row)
            date_diff = (after_date - before_date).days

            if date_diff == offset_value:
                column_qc_result['passed_count'] += 1
            else:
                remarks.append({
                    'nd_auto_increment_id': nd_id,
                    'source date': str(before_date),
                    'dest date': str(after_date)
                })
                column_qc_result['failed_count'] += 1

        column_qc_result['remarks'] = {'remarks': remarks, 'format_remarks': format_remarks}
        return column_qc_result

class SPatientIdDetector(Detector):

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _verify_length(self, col_value: Any, ignore_condition: dict) -> bool:
        length_of_value = self.qc_config.get("PATIENT_ID", {}).get("length_of_value", None)
        if length_of_value is not None:
            return len(str(col_value)) == length_of_value
        return True

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _verify_prefix(self, col_value: Any, ignore_condition: dict) -> bool:
        prefix_value = self.qc_config.get("PATIENT_ID", {}).get("prefix_value", None)
        if prefix_value is not None:
            return str(col_value).startswith(prefix_value)
        return True

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={"length_verification_failed": 0, "prefix_verification_failed": 0})
        column_name = self.column_config["column_name"]
        for row in after_rows:
            is_deidentify = True
            if row[column_name] is not None and not self._verify_length(row[column_name], ignore_condition):
                is_deidentify = False
                column_qc_result["remarks"]["length_verification_failed"] += 1
            if row[column_name] is not None and  not self._verify_prefix(row[column_name], ignore_condition):
                is_deidentify = False
                column_qc_result["remarks"]["prefix_verification_failed"] += 1
            if is_deidentify:
                column_qc_result["passed_count"] += 1
            else:
                column_qc_result["failed_count"] += 1
        return column_qc_result

class SReferencePIDDetector(Detector):

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _verify_length(self, col_value: Any, ignore_condition: dict) -> bool:
        length_of_value = self.qc_config.get("PATIENT_ID", {}).get("length_of_value", None)
        if length_of_value is not None:
            return len(str(col_value)) == length_of_value
        return True

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _verify_prefix(self, col_value: Any, ignore_condition: dict) -> bool:
        prefix_value = self.qc_config.get("PATIENT_ID", {}).get("prefix_value", None)
        if prefix_value is not None:
            return str(col_value).startswith(prefix_value)
        return True

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={"length_verification_failed": 0, "prefix_verification_failed": 0})
        column_name = self.column_config["column_name"]
        for row in after_rows:
            is_deidentify = True
            if row[column_name] is not None and not self._verify_length(row[column_name], ignore_condition):
                is_deidentify = False
                column_qc_result["remarks"]["length_verification_failed"] += 1
            if row[column_name] is not None and  not self._verify_prefix(row[column_name], ignore_condition):
                is_deidentify = False
                column_qc_result["remarks"]["prefix_verification_failed"] += 1
            if is_deidentify:
                column_qc_result["passed_count"] += 1
            else:
                column_qc_result["failed_count"] += 1
        return column_qc_result

class SEncounterIDDetector(Detector):

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _verify_length(self, col_value: Any, ignore_condition: dict) -> bool:
        length_of_value = self.qc_config.get("ENCOUNTER_ID", {}).get("length_of_value", None)
        if length_of_value is not None:
            return len(str(col_value)) == length_of_value
        return True

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _verify_prefix(self, col_value: Any, ignore_condition: dict) -> bool:
        prefix_value = self.qc_config.get("ENCOUNTER_ID", {}).get("prefix_value", None)
        if prefix_value is not None:
            return str(col_value).startswith(prefix_value)
        return True

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={"length_verification_failed": 0, "prefix_verification_failed": 0})
        column_name = self.column_config["column_name"]

        for row in after_rows:
            is_de_identify = True
            if row[column_name] is not None and not self._verify_length(row[column_name], ignore_condition):
                is_de_identify = False
                column_qc_result["remarks"]["length_verification_failed"] += 1
            if row[column_name] is not None and not self._verify_prefix(row[column_name], ignore_condition):
                is_de_identify = False
                column_qc_result["remarks"]["prefix_verification_failed"] += 1
            if is_de_identify:
                column_qc_result["passed_count"] += 1
            else:
                column_qc_result["failed_count"] += 1

        return column_qc_result


# ── Offender-capturing ID length/prefix base ───────────────────────────────────
# QC Framework Part 1 failure action: "log row identifier, column name, actual length".
# APPOINTMENT_ID and CHART_ID had no detector at all (KeyError risk in the scanner); these add
# them and also record the offending nd_auto_increment_id + actual value (capped) in remarks.
_MAX_OFFENDER_SAMPLES = 50


class _SIdLengthPrefixDetector(Detector):
    """Length + prefix check for an ID column, driven by ``qc_config[CONFIG_KEY]``."""
    CONFIG_KEY: str = ""

    def _rule(self) -> dict:
        return self.qc_config.get(self.CONFIG_KEY, {}) or {}

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        rule = self._rule()
        length_of_value = rule.get("length_of_value", None)
        prefix_value = rule.get("prefix_value", None)
        column_name = self.column_config["column_name"]
        result = ColumnQCResult(
            passed_count=0, failed_count=0,
            remarks={"length_verification_failed": 0, "prefix_verification_failed": 0, "offenders": []},
        )
        for row in after_rows:
            val = row.get(column_name)
            if val is None:
                result["passed_count"] += 1
                continue
            sval = str(val)
            ok = True
            if length_of_value is not None and len(sval) != length_of_value:
                ok = False
                result["remarks"]["length_verification_failed"] += 1
            if prefix_value is not None and not sval.startswith(str(prefix_value)):
                ok = False
                result["remarks"]["prefix_verification_failed"] += 1
            if ok:
                result["passed_count"] += 1
            else:
                result["failed_count"] += 1
                if len(result["remarks"]["offenders"]) < _MAX_OFFENDER_SAMPLES:
                    result["remarks"]["offenders"].append({
                        "nd_auto_increment_id": row.get("nd_auto_increment_id"),
                        "column": column_name,
                        "value": sval,
                        "length": len(sval),
                    })
        return result


class SAppointmentIdDetector(_SIdLengthPrefixDetector):
    CONFIG_KEY = "APPOINTMENT_ID"


class SChartIdDetector(_SIdLengthPrefixDetector):
    CONFIG_KEY = "CHART_ID"
