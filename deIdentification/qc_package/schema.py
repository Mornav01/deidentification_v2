from typing import TypedDict

class ColumnRemarks(TypedDict, total=False):
    length_verification_failed: int
    prefix_verification_failed: int

class ColumnQCResult(TypedDict):
    passed_count: int
    failed_count: int
    remarks: dict
    
class FinalQCResult(TypedDict):
    is_qc_passed: bool
    reason: str

class OutputSchemaForTable(TypedDict):
    sample_size: int
    table_name: str
    source_rows_count: int
    dest_rows_count: int
    ColumnsQCResult: dict[str, ColumnQCResult]
    final_qc_result: FinalQCResult

