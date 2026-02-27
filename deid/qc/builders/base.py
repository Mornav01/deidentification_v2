from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime, timedelta
from deid.qc.schema import ColumnQCResult
from deid.config.table_schemas import ColumnDetailsForUI

class Detector(ABC):
    
    def __init__(self, patient_mapping_dict: dict, enc_mapping_dict: dict, qc_config: dict, column_config: ColumnDetailsForUI, patient_id_column: str, enc_id_column: str):
        self.patient_mapping_dict = patient_mapping_dict
        self.enc_mapping_dict = enc_mapping_dict
        self.qc_config = qc_config
        self.column_config = column_config
        self.column_name = self.column_config["column_name"]

        self.patient_id_column = patient_id_column
        self.enc_id_column = enc_id_column
    
    @abstractmethod
    def is_deidentified(self, before_rows: list[dict], after_rows: list[dict], ignore_condition: dict) -> ColumnQCResult:
        pass
