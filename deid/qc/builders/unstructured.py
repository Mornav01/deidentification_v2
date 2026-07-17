from typing import Any
from deid.qc.builders.base import Detector
from deid.qc.schema import ColumnQCResult
from deid.qc.llm_scan import ResidualPIIScanner
from pydantic import validate_call


class UnstructuredDetector(Detector):
    """QC scan for de-identified free-text columns.

    Two complementary checks per note:
    1. **Master exact-match** — any known PHI value (from ``pii_info``, loaded from the PHI master)
       still present in the text.
    2. **Residual-PII scan** — a pluggable backend (regex by default; ``none`` to disable)
       that flags residual PHI regardless of the master. Replaces the former Presidio dependency;
       backend is chosen via ``qc_config['residual_pii_backend']`` (see deid/qc/llm_scan.py).
    """

    def _scanner(self) -> ResidualPIIScanner:
        # Built once per detector instance from qc_config.
        if not hasattr(self, "_residual_scanner"):
            self._residual_scanner = ResidualPIIScanner.from_qc_config(self.qc_config)
        return self._residual_scanner

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _exact_match(self, text: str, pii_info: dict):
        assert isinstance(pii_info, dict), "pii_info must be a dict"
        found_pii_values = []
        for key, value in pii_info.items():
            if value in text:
                found_pii_values.append(value)
        return found_pii_values

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict, pii_info: dict) -> ColumnQCResult:
        assert isinstance(after_rows, list), "after_rows must be a list"
        assert isinstance(pii_info, dict), "pii_info must be a dict"

        scanner = self._scanner()
        column_qc_result = ColumnQCResult(passed_count=0, failed_count=0, remarks={})
        all_failed_remarks = {"exact_match_remarks": [], "residual_pii_remarks": []}
        for row in after_rows:
            cell_value = row.get(self.column_name)
            if cell_value is None:
                column_qc_result["passed_count"] += 1
                continue
            cell_value = str(cell_value)
            exact_entities_found = self._exact_match(cell_value, pii_info)
            residual_found = scanner.scan(cell_value)  # [{'type','text'}]
            if len(exact_entities_found) > 0 or len(residual_found) > 0:
                column_qc_result["failed_count"] += 1
            else:
                column_qc_result["passed_count"] += 1
            all_failed_remarks["exact_match_remarks"].extend(exact_entities_found)
            all_failed_remarks["residual_pii_remarks"].extend(
                (e["type"], e["text"]) for e in residual_found
            )
        all_failed_remarks["residual_pii_remarks"] = list(set(all_failed_remarks["residual_pii_remarks"]))
        column_qc_result['remarks'] = all_failed_remarks
        return column_qc_result
