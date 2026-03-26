from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime, timedelta
from deid.qc.builders.base import Detector
from deid.qc.schema import ColumnQCResult
from pydantic import validate_call

_analyzer = None


def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        from presidio_analyzer import AnalyzerEngine
        _analyzer = AnalyzerEngine()
    return _analyzer

class UnstructuredDetector(Detector):

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _exact_match(self, text: str, pii_info: dict):
        assert isinstance(pii_info, dict), "pii_info must be a dict"
        found_pii_values = []
        for key, value in pii_info.items():
            if value in text:
                found_pii_values.append(value)
        return found_pii_values

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _presidio_analyzer(self, text: str):
        results = _get_analyzer().analyze(text=str(text), entities=["PHONE_NUMBER", "EMAIL_ADDRESS", "PERSON"], language="en")
        found_entities = []
        if results:
            for result in results:
                value = (result.entity_type, text[result.start:result.end])
                if value not in found_entities:
                    found_entities.append((result.entity_type, text[result.start:result.end]))
        return found_entities


    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict, pii_info: dict) -> ColumnQCResult:
        assert isinstance(after_rows, list), "after_rows must be a list"
        assert isinstance(pii_info, dict), "pii_info must be a dict"

        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        all_failed_remarks = {"exact_match_remarks": [], "presidio_remarks": []}
        for row in after_rows:
            cell_value = row.get(self.column_name)
            if cell_value is None:
                column_qc_result["passed_count"] += 1
                continue
            cell_value = str(cell_value)
            exact_entities_found = self._exact_match(cell_value, pii_info)
            presidio_entities_found = self._presidio_analyzer(cell_value)
            if len(exact_entities_found)>0 or len(presidio_entities_found)>0:
                column_qc_result["failed_count"] += 1
            else:
                column_qc_result["passed_count"] += 1
            all_failed_remarks["exact_match_remarks"].extend(exact_entities_found)
            all_failed_remarks["presidio_remarks"].extend(presidio_entities_found)
        all_failed_remarks["presidio_remarks"] = list(set(all_failed_remarks["presidio_remarks"]))
        column_qc_result['remarks'] = all_failed_remarks
        return column_qc_result
