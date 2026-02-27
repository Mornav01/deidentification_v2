from typing import TypedDict


class PatientMappingDict(TypedDict):
    patient_id: int
    nd_patient_id: int
    offset: int


class EncounterMappingDict(TypedDict):
    encounter_id: int
    nd_encounter_id: int
    patient_id: int


class MappingDbConfig(TypedDict):
    connection_string: str
    inhouse_mapping_table: bool
    # patient_table_name: str
    # encounter_table_name: str
    # patient_id_column: str
    # encounter_id_column: str
    # offset_column: str
    # nd_patient_id_column: str
    # nd_encounter_id_column: str



class SecondaryPIIConfig(TypedDict):
    table_name: str
    config: dict
