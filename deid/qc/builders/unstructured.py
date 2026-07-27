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
        # Pass/fail is gated ONLY on the master exact-match (ground truth). Residual-regex hits are
        # advisory — recorded for manual review but never a FAIL (regex over-flags free-text notes).
        # Each entry carries nd_auto_increment_id so a reviewer can pull the exact row.
        exact_match_failures = []
        residual_advisory = []
        for row in after_rows:
            row_id = row.get("nd_auto_increment_id")
            cell_value = row.get(self.column_name)
            if cell_value is None:
                column_qc_result["passed_count"] += 1
                continue
            cell_value = str(cell_value)
            exact_entities_found = self._exact_match(cell_value, pii_info)
            residual_found = scanner.scan(cell_value)  # [{'type','text'}]
            if exact_entities_found:
                column_qc_result["failed_count"] += 1
                exact_match_failures.append({"nd_auto_increment_id": row_id, "values": exact_entities_found})
            else:
                column_qc_result["passed_count"] += 1
            if residual_found:
                residual_advisory.append({
                    "nd_auto_increment_id": row_id,
                    "residual": sorted({(e["type"], e["text"]) for e in residual_found}),
                })
        column_qc_result['remarks'] = {
            "exact_match_failures": exact_match_failures,
            "residual_advisory": residual_advisory,
        }
        return column_qc_result
