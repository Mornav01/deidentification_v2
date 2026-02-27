from deid.config.table_schemas import TableDetailsForUI
from .schemas import EncounterMappingDict

from typing import TypedDict
from deid.core.dbPkg import NDDBHandler
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
from sqlalchemy import create_engine, MetaData, Table, select

Base = declarative_base()


def _get_patient_ids_from_enc_ids(encounter_ids: list[int], encounter_id_mapping: dict[int, EncounterMappingDict]) -> list[int]:
    patient_ids = []
    for encid in encounter_ids:
        if (encid is None) or (encid not in encounter_id_mapping):
            continue
        patient_ids.append(encounter_id_mapping[encid]["patient_id"])
    return patient_ids


class PIITable(Base):
    __tablename__ = "pii_data_table"
    patient_id = Column(Integer, primary_key=True)

class InsuranceTable(Base):
    __tablename__ = "master_insurance_table"
    encounter_id = Column(Integer, primary_key=True)
    patient_id = Column(Integer)
    # offset_value = Column(Integer)

class PIIDb:
    def __init__(self, pii_db_config: dict):
        self.pii_db_config = pii_db_config
        self.master_connection = self._get_master_db_connection()
        self.insurance_connection = self._get_insurance_db_connection()

    
    def _get_master_db_connection(self):
        connection_string = self.pii_db_config["master_connection_str"]
        self.master_engine = create_engine(connection_string)
        Base.metadata.create_all(self.master_engine)
        Session = sessionmaker(bind=self.master_engine)
        self.master_session = Session()
        return NDDBHandler(connection_string)

    def _get_insurance_db_connection(self):
        connection_string = self.pii_db_config["insurance_connection_str"]
        self.insurance_engine = create_engine(connection_string)
        Base.metadata.create_all(self.insurance_engine)
        Session = sessionmaker(bind=self.insurance_engine)
        self.insurance_session = Session()
        return NDDBHandler(connection_string)

    def close_master_connection(self):
        self.master_session.close()
        self.master_engine.dispose()

    def close_insurance_connection(self):
        self.insurance_session.close()
        self.insurance_engine.dispose()


    def get_pii_data(
        self, patient_ids: list[int]
    ) -> dict[int, dict]:
        metadata = MetaData()
        pii_table = Table('pii_data_table', metadata, autoload_with=self.master_session.bind)

        # Build the select statement
        stmt = select(pii_table).where(pii_table.c.patient_id.in_(patient_ids))

        # Execute the query
        results = self.master_session.execute(stmt).fetchall()

        # Convert results to a dictionary
        pii_data_dict = {}
        for row in results:
            row_dict = row._asdict()
            pii_data_dict[row_dict["patient_id"]] = row_dict

        return pii_data_dict


class PIITableLoader:
    def __init__(self, pii_db_config: dict):
        self.pii_db_connection: PIIDb = None
        self.pii_db_config = pii_db_config

    def _has_notes_columns(self, table_config: TableDetailsForUI):
        for col_conf in table_config["columns_details"]:
            if col_conf["de_identification_rule"] == "NOTES":
                return True
        return False

    def load_pii_table(self, table_config: TableDetailsForUI, patient_ids: list[int], encounter_ids: list[int], encounter_id_mapping: dict[int, EncounterMappingDict]) -> tuple[dict[int, dict], dict[int, dict]]:
        pii_data = {}
        if not self._has_notes_columns(table_config=table_config):
            return pii_data
        if self.pii_db_connection is None:
            self.pii_db_connection = PIIDb(self.pii_db_config)
        if len(patient_ids)>0:
            pii_data = self.get_nd_patients_dict(patient_ids)
        else:
            patientids = _get_patient_ids_from_enc_ids(encounter_ids, encounter_id_mapping)
            pii_data = self.get_nd_patients_dict(patientids)
        self.pii_db_connection.close_master_connection()
        return pii_data
    
    def load_insurance_table(self, table_config: TableDetailsForUI, patient_ids: list[int], encounter_ids: list[int], encounter_id_mapping: dict[int, EncounterMappingDict]):
        pii_data = {}
        if not self._has_notes_columns(table_config=table_config):
            return pii_data
        if self.pii_db_connection is None:
            self.pii_db_connection = PIIDb(self.pii_db_config)
        insurnace_data_dict = {}
        if len(patient_ids)>0:
            metadata = MetaData()
            insurance_table = Table('master_insurance_table', metadata, autoload_with=self.pii_db_connection.insurance_session.bind)
            # Build the select statement
            stmt = select(insurance_table).where(insurance_table.c.patient_id.in_(patient_ids))
            # Execute the query
            results = self.pii_db_connection.insurance_session.execute(stmt).fetchall()
            # Convert results to a dictionary
            for row in results:
                row_dict = row._asdict()
                if row_dict["patient_id"] not in insurnace_data_dict:
                    insurnace_data_dict[row_dict["patient_id"]] = []
                insurnace_data_dict[row_dict["patient_id"]].append(row_dict)
            return insurnace_data_dict
        else:
            metadata = MetaData()
            insurance_table = Table('master_insurance_table', metadata, autoload_with=self.pii_db_connection.insurance_session.bind)
            # Build the select statement
            stmt = select(insurance_table).where(insurance_table.c.encounter_id.in_(encounter_ids))
            # Execute the query
            results = self.pii_db_connection.insurance_session.execute(stmt).fetchall()
            # Convert results to a dictionary
            for row in results:
                row_dict = row._asdict()
                if row_dict["patient_id"] not in insurnace_data_dict:
                    insurnace_data_dict[row_dict["patient_id"]] = []
                insurnace_data_dict[row_dict["patient_id"]].append(row_dict)
            
        self.pii_db_connection.close_insurance_connection()
        return {'metadata': self.pii_db_config.get("insurance_metadata", {}), "rows": insurnace_data_dict}

    def get_nd_patients_dict(
        self, patient_ids: list[int]
    ) -> dict[int, dict]:
        return self.pii_db_connection.get_pii_data(patient_ids)
