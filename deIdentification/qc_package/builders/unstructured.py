from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime, timedelta
from qc_package.builders.base import Detector
from qc_package.schema import ColumnQCResult
from presidio_analyzer import AnalyzerEngine

analyzer = AnalyzerEngine()

class UnstructuredDetector(Detector):
    
    def _exact_match(self, text: str, pii_info: dict):
        found_pii_values = []
        for key, value in pii_info.items():
            if value in text:
                found_pii_values.append(value)
        return found_pii_values

    def _presidio_analyzer(self, text: str):
        results = analyzer.analyze(text=str(text), entities=["PHONE_NUMBER", "EMAIL_ADDRESS", "PERSON"], language="en")
        found_entities = []
        if results:
            for result in results:
                value = (result.entity_type, text[result.start:result.end])
                if value not in found_entities:
                    found_entities.append((result.entity_type, text[result.start:result.end]))
        return found_entities

    
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict, pii_info: dict) -> ColumnQCResult:
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        all_failed_remarks = {"exact_match_remarks": [], "presidio_remarks": []}
        for row in after_rows:
            exact_entities_found = self._exact_match(row[self.column_name], pii_info)
            presidio_entities_found = self._presidio_analyzer(row[self.column_name])
            if len(exact_entities_found)>0 or len(presidio_entities_found)>0:
                column_qc_result["failed_count"] += 1
            else:
                column_qc_result["passed_count"] += 1
            all_failed_remarks["exact_match_remarks"].extend(exact_entities_found)
            all_failed_remarks["presidio_remarks"].extend(presidio_entities_found)
        all_failed_remarks["presidio_remarks"] = list(set(all_failed_remarks["presidio_remarks"]))
        column_qc_result['remarks'] = all_failed_remarks
        return column_qc_result
