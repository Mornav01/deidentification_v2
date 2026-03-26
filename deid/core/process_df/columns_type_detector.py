from .base import Rules
from sqlalchemy import (
    BigInteger,
    Integer,
    String,
    Text, DateTime
)
from sqlalchemy.dialects.mysql import LONGTEXT
from deid.config.table_schemas import TableDetailsForUI


class ColumnsTypeDetector:

    @classmethod
    def get_columns_definations(cls, table_config: TableDetailsForUI):
        SchemaToRulesMapping = {
            Rules.PATIENT_ID: ColumnsTypeDetector._get_column_type_for_patient_id(),
            Rules.ENCOUNTER_ID: ColumnsTypeDetector._get_column_type_for_enc_id(),
            Rules.REFERENCE_PID: ColumnsTypeDetector._get_column_type_for_reference_pid(),
            Rules.APPOINTMENT_ID: ColumnsTypeDetector._get_column_type_for_appointment_id(),
            Rules.PATIENT_DOB: ColumnsTypeDetector._get_column_type_for_dob_id(),
            Rules.DATE_OFFSET: ColumnsTypeDetector._get_column_type_for_dateoffset_id(),
            Rules.ZIP_CODE: ColumnsTypeDetector._get_column_type_for_zipcode_id(),
            Rules.MASK: {"type": String, "length": 100},
            Rules.NOTES: {"type": LONGTEXT},
            Rules.GENERIC_NOTES: {"type": LONGTEXT},
            Rules.STATIC_OFFSET: ColumnsTypeDetector._get_column_type_for_dateoffset_id(),
        }
        return SchemaToRulesMapping

    @classmethod
    def _get_column_type_for_patient_id(cls):
        return {"type": BigInteger, "null":True}

    @classmethod
    def _get_column_type_for_enc_id(cls):
        return {"type": BigInteger, "null":True}

    @classmethod
    def _get_column_type_for_reference_pid(cls):
        return {"type": BigInteger, "null":True}

    @classmethod
    def _get_column_type_for_appointment_id(cls):
        return {"type": BigInteger, "null":True}

    @classmethod
    def _get_column_type_for_dob_id(cls):
        return {"type": Integer, "null": True}

    @classmethod
    def _get_column_type_for_dateoffset_id(cls):
        return {"type": DateTime, "length": 100, "null":True}

    @classmethod
    def _get_column_type_for_zipcode_id(cls):
        return {"type": String, "length": 50, "null": True}
