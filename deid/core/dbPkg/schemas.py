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
    connection_str: str
    inhouse_mapping_table: bool
