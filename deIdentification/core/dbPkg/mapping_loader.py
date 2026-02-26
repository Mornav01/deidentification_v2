from .schemas import PatientMappingDict, EncounterMappingDict, MappingDbConfig

from typing import TypedDict
from core.dbPkg import NDDBHandler
from .schemas import (
    PatientMappingDict,
    EncounterMappingDict,
    MappingDbConfig,
)
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    MetaData,
    create_engine,
    or_,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


# import os
# from sqlalchemy import Column, Integer, String, MetaData, Table
# from sqlalchemy.orm import registry
# patient_mapper_registry = registry()
# patient_metadata = MetaData()
# columns_config = os.environ.get("PATIENT_MAPPING_COLUMNS", "patient_id,nd_patient_id,offset, reference_mapping")
# columns_list = [col.strip() for col in columns_config.split(",")]
# patient_columns = []
# for column in columns_list:
#     if column == 'patient_id':
#         patient_columns.append(Column("patient_id", Integer, primary_key=True))
#     else:
#         patient_columns.append(Column("nd_patient_id", Integer))

# PatientMappingTable = Table("patient_mapping_table", patient_metadata, *patient_columns)

# @patient_mapper_registry.mapped
# class PatientMapping:
#     __table__ = PatientMappingTable

class PatientMapping(Base):
    __tablename__ = "patient_mapping_table"

    patient_id = Column(Integer, primary_key=True)
    nd_patient_id = Column(Integer)
    offset = Column(Integer)
    reference_mapping = Column(Integer)
    

class EncounterMapping(Base):
    __tablename__ = "encounter_mapping_table"

    encounter_id = Column(Integer, primary_key=True)
    nd_encounter_id = Column(Integer)
    patient_id = Column(Integer)


class MappingDb:
    def __init__(self, mapping_db_config: MappingDbConfig):
        self.mapping_db_config = mapping_db_config
        self.inhouse_mapping_table = mapping_db_config.get(
            "inhouse_mapping_table", False
        )
        self._get_mapping_table_connection()
        # self.connection = self._get_mapping_table_connection()

    def _get_mapping_table_connection(self):
        if self.inhouse_mapping_table:
            raise NotImplementedError("In-house mapping table is not implemented")
        else:
            connection_string = self.mapping_db_config["connection_str"]
            self.engine = create_engine(connection_string)
            Base.metadata.create_all(self.engine)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            return
            # return NDDBHandler(connection_string)

    def close_connection(self):
        self.session.close()
        self.engine.dispose()
    
    def get_nd_patients_dict(
        self,
        ids: list[int],
        id_column: str = "patient_id",
    ) -> dict[int, dict]:
        column_attr = getattr(PatientMapping, id_column)

        query = (
            self.session.query(PatientMapping)
            .filter(column_attr.in_(ids))
            .all()
        )

        mapping_dict = {}
        for row in query:
            key = getattr(row, id_column)
            mapping_dict[key] = {
                "nd_patient_id": row.nd_patient_id,
                "offset": row.offset,
                id_column: key,
                'patient_id': row.patient_id
            }
        return mapping_dict
    
    # def get_nd_patients_dict(
    #     self, patient_ids: list[int]
    # ) -> dict[int, PatientMappingDict]:
    #     query = (
    #         self.session.query(PatientMapping)
    #         .filter(PatientMapping.patient_id.in_(patient_ids))
    #         .all()
    #     )
    #     mapping_dict = {}
    #     for row in query:
    #         mapping_dict[row.patient_id] = {
    #             "patient_id": row.patient_id,
    #             "nd_patient_id": row.nd_patient_id,
    #             "offset": row.offset,
    #         }
    #     return mapping_dict

    def get_nd_encounter_dict(
        self, encounter_ids: list[int]
    ) -> dict[int, EncounterMappingDict]:
        query = (
            self.session.query(EncounterMapping)
            .filter(EncounterMapping.encounter_id.in_(encounter_ids))
            .all()
        )
        mapping_dict = {}
        for row in query:
            mapping_dict[row.encounter_id] = {
                "encounter_id": row.encounter_id,
                "nd_encounter_id": row.nd_encounter_id,
                "patient_id": row.patient_id,
            }
        return mapping_dict
    
    def get_reverse_patients_dict(
        self, nd_patient_ids: list[int]
    ) -> dict[int, PatientMappingDict]:
        query = (
            self.session.query(PatientMapping)
            .filter(PatientMapping.nd_patient_id.in_(nd_patient_ids))
            .all()
        )
        mapping_dict = {}
        for row in query:
            mapping_dict[row.nd_patient_id] = {
                "patient_id": row.patient_id,
                "offset": row.offset,
            }
        return mapping_dict

    def get_reverse_encounter_dict(
        self, nd_encounter_ids: list[int]
    ) -> dict[int, EncounterMappingDict]:
        query = (
            self.session.query(EncounterMapping)
            .filter(EncounterMapping.nd_encounter_id.in_(nd_encounter_ids))
            .all()
        )
        mapping_dict = {}
        for row in query:
            mapping_dict[row.nd_encounter_id] = {
                "encounter_id": row.encounter_id,
                "patient_id": row.patient_id,
            }
        return mapping_dict

class MappingTableLoader:
    def __init__(self, mapping_db_config: MappingDbConfig):
        self.mapping_db_connection: MappingDb = None
        self.mapping_db_config = mapping_db_config

    def load_mapping_table(
        self, patient_ids: list[int], encounter_ids: list[int]
    ) -> tuple[dict[int, PatientMappingDict], dict[int, EncounterMappingDict]]:
        if self.mapping_db_connection is None:
            self.mapping_db_connection = MappingDb(self.mapping_db_config)

        encounter_mapping_dict = self.get_nd_encounter_dict(encounter_ids)
        patient_ids = self._fill_missing_patient_ids(patient_ids, encounter_ids, encounter_mapping_dict)
        patient_mapping_dict = self.get_nd_patients_dict(patient_ids, "patient_id")
        self.mapping_db_connection.close_connection()
        return patient_ids, patient_mapping_dict, encounter_mapping_dict
    
    def load_reference_mapping(self, reference_col: str, reference_ids: list[int]) -> dict:
        if self.mapping_db_connection is None:
            self.mapping_db_connection = MappingDb(self.mapping_db_config)

        reference_mapping = self.get_nd_patients_dict(reference_ids, "reference_mapping")
        mapping = {}
        for refer_val, mapping_dict in reference_mapping.items():
            mapping[refer_val] = mapping_dict['patient_id']

        reference_mapping_output = {
            "source_column": reference_col,
            "reference_mapping": mapping
        }
        self.mapping_db_connection.close_connection()
        return reference_mapping_output

    def _fill_missing_patient_ids(self, patient_ids: list[int], encounter_ids: list[int], encounter_mapping_dict: dict[int, EncounterMappingDict]) -> list[int]:
        missing_pids = []
        for encid in encounter_ids:
            # if (encid is None) or (encid not in encounter_mapping_dict):
            #     continue
            missing_pids.append(encounter_mapping_dict.get(encid, {}).get('patient_id', None))
        patient_ids = list(set(patient_ids + missing_pids))
        return patient_ids

    def get_nd_patients_dict(
        self, ids: list[int], id_column: str="patient_id"
    ) -> dict[int, PatientMappingDict]:
        return self.mapping_db_connection.get_nd_patients_dict(ids, id_column)

    def get_nd_encounter_dict(
        self, encounter_ids: list[int]
    ) -> dict[int, EncounterMappingDict]:
        return self.mapping_db_connection.get_nd_encounter_dict(encounter_ids)
