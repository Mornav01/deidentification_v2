from typing import TypedDict, Literal


class Condition(TypedDict):
    source_column: str
    reference_table: str
    column_name: str


class JoinCondition(TypedDict):
    source_table: str
    conditions: list[Condition]
    destination_column: str
    destination_column_type: Literal["patient_id", "encounter_id"]


class IgnoreRowsColumn(TypedDict):
    name: str
    value: list[str]
    condition: str


class IgnoreRowsConfig(TypedDict):
    operation: str
    columns: list[IgnoreRowsColumn]


class ColumnDetailsForUI(TypedDict, total=False):
    column_name: str
    is_phi: bool
    de_identification_rule: str
    add_to_phi_table: bool
    column_name_for_phi_table: str
    ignore_rows: IgnoreRowsConfig
    reference_mapping: JoinCondition = {}


class _TableDetailsRequired(TypedDict):
    columns_details: list[ColumnDetailsForUI]


class TableDetailsForUI(_TableDetailsRequired, total=False):
    ignore_rows: IgnoreRowsConfig
    batch_size: int
    reference_patient_id_column: str
    reference_enc_id_column: str
    reference_mapping: JoinCondition
