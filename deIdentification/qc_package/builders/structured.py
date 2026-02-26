from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime, timedelta
from qc_package.builders.base import Detector
from qc_package.schema import ColumnQCResult
from django.conf import settings

class SZipCodeDetector(Detector):
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        passed_count = sum(1 for row in after_rows if len(str(row[self.column_name])) <= 3 or str(row[self.column_name]).lower() in ["none", "null"])
        failed_count = len(after_rows) - passed_count
        return ColumnQCResult(passed_count=passed_count, failed_count=failed_count, remarks={})

            
class SDobDetector(Detector):
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        passed_count = sum(
            1 for row in after_rows 
            if (value := str(row[self.column_name]).lower()) in {None, '', 'null', 'none'} or (value.isdigit() and len(value) == 4)
        )
        failed_count = len(after_rows) - passed_count
        return ColumnQCResult(passed_count=passed_count, failed_count=failed_count, remarks={})
    
class SStaticOffestDetector(Detector):
    def get_offset(self, row: dict):
        return settings.DEFAULT_OFFSET_VALUE
    
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        column_name = self.column_config["column_name"]

        # Convert before_rows to dictionary for O(1) lookup
        before_dict = {row['nd_auto_increment_id']: row for row in before_rows}

        remarks = []

        for after_row in after_rows:
            nd_id = after_row.get('nd_auto_increment_id')
            before_row = before_dict.get(nd_id)

            if not before_row:
                continue  # Skip if there's no matching row

            try:
                before_date = datetime.strptime(str(before_row.get(column_name, '')), '%Y-%m-%d')
                after_date = datetime.strptime(str(after_row.get(column_name, '')), '%Y-%m-%d')
            except (ValueError, TypeError):
                continue  # Skip invalid dates
            
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

        column_qc_result['remarks'] = {'remarks': remarks}
        return column_qc_result

class SMaskDetector(Detector):
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        mask_value = self.column_config["mask_value"]
        passed_count = sum(1 for row in after_rows if row[self.column_name] == f'<<{mask_value}>>')
        failed_count = len(after_rows) - passed_count
        return ColumnQCResult(passed_count=passed_count, failed_count=failed_count, remarks={})

class SDateOffestDetector(Detector):
    def get_offset(self, row: dict):
        enc_id, patient_id = None, None
        if self.patient_id_column is not None:
            pid = row[self.patient_id_column]
            return self.patient_mapping_dict[pid]['offset']
        elif self.enc_id_column is not None:
            encid = row[self.enc_id_column]
            pid = self.enc_mapping_dict[encid]['patient_id']
            return  self.patient_mapping_dict[pid]['offset']
        return settings.DEFAULT_OFFSET_VALUE
        raise Exception(f"not able to find the patient-id and enc-id")
    
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        column_name = self.column_config["column_name"]

        # Convert before_rows to dictionary for O(1) lookup
        before_dict = {row['nd_auto_increment_id']: row for row in before_rows}

        remarks = []

        for after_row in after_rows:
            nd_id = after_row.get('nd_auto_increment_id')
            before_row = before_dict.get(nd_id)

            if not before_row:
                continue  # Skip if there's no matching row

            try:
                before_date = datetime.strptime(str(before_row.get(column_name, '')), '%Y-%m-%d')
                after_date = datetime.strptime(str(after_row.get(column_name, '')), '%Y-%m-%d')
            except (ValueError, TypeError):
                continue  # Skip invalid dates
            
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

        column_qc_result['remarks'] = {'remarks': remarks}
        return column_qc_result

class SPatientIdDetector(Detector):

    def _verify_length(self, col_value: Any, ignore_condition: dict) -> bool:
        length_of_value = self.qc_config.get("PATIENT_ID", {}).get("length_of_value", None)
        if length_of_value is not None:
            return len(str(col_value)) == length_of_value
        return True
    
    def _verify_prefix(self, col_value: Any, ignore_condition: dict) -> bool:
        prefix_value = self.qc_config.get("PATIENT_ID", {}).get("prefix_value", None)
        if prefix_value is not None:
            return str(col_value).startswith(prefix_value)
        return True
    
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

    def _verify_length(self, col_value: Any, ignore_condition: dict) -> bool:
        length_of_value = self.qc_config.get("PATIENT_ID", {}).get("length_of_value", None)
        if length_of_value is not None:
            return len(str(col_value)) == length_of_value
        return True
    
    def _verify_prefix(self, col_value: Any, ignore_condition: dict) -> bool:
        prefix_value = self.qc_config.get("PATIENT_ID", {}).get("prefix_value", None)
        if prefix_value is not None:
            return str(col_value).startswith(prefix_value)
        return True
    
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
    
    def _verify_length(self, col_value: Any, ignore_condition: dict) -> bool:
        length_of_value = self.qc_config.get("ENCOUNTER_ID", {}).get("length_of_value", None)
        if length_of_value is not None:
            return len(str(col_value)) == length_of_value
        return True
    
    def _verify_prefix(self, col_value: Any, ignore_condition: dict) -> bool:
        prefix_value = self.qc_config.get("ENCOUNTER_ID", {}).get("prefix_value", None)
        if prefix_value is not None:
            return str(col_value).startswith(prefix_value)
        return True
    
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
