import random
from typing import TypedDict
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, TIMESTAMP, text, BigInteger, Numeric, UniqueConstraint, Index
from sqlalchemy.sql import select
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__file__)

PATIENT_MAPPING_TABLE_NAME = "patient_mapping_table"
ENCOUNTER_MAPPING_TABLE_NAME = "encounter_mapping_table"
OFFSET_RANGE  = list(range(-38, -29)) + list(range(30, 39))

class MappingTableConfig(TypedDict):
    source_connection_str: str
    dest_connection_str: str
    mapping_query: str


class MappingTable:
    USERNAME = "nd-admin"

    def __init__(self, mapping_config: MappingTableConfig):
        self.source_engine = create_engine(mapping_config['source_connection_str'])
        self.dest_engine = create_engine(mapping_config['dest_connection_str'])

        self.patient_start_value = mapping_config['patient_start_value'] #100100030000001
        """
        SELECT 
            u.uid AS client_patient_id, 
            e.date AS client_visit_date, 
            e.encounterID AS client_encounter_id 
            e.register_date AS patient_registration_date
        FROM users AS u
        LEFT JOIN enc AS e ON u.uid = e.patientID
        WHERE u.UserType = 3
        ORDER BY 1, 2, 3
        """
        self.mapping_query = mapping_config['mapping_query']

        self.mapping_config = mapping_config

    def create_patient_mapping_table(self):
        metadata = MetaData()
        patient_mapping_table = Table(PATIENT_MAPPING_TABLE_NAME, metadata,
            Column('nd_patient_id', BigInteger, primary_key=True, autoincrement=True),
            Column('patient_id', Integer, nullable=False),
            Column('patient_registration_date', TIMESTAMP, nullable=False),
            Column('offset', Integer, nullable=False),
            Column('created_by', String(50), nullable=False),
            Column('created_at', TIMESTAMP, nullable=False),
            Column('updated_by', String(50), nullable=False),
            Column('updated_at', TIMESTAMP, nullable=False),
            UniqueConstraint('patient_id', name='uq_patient_id'),
            Index('ix_patient_id', 'patient_id')
        )
        metadata.create_all(self.dest_engine)

        with self.dest_engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE patient_mapping_table AUTO_INCREMENT = {self.patient_start_value};"))
        
        logger.info(f"created {PATIENT_MAPPING_TABLE_NAME} table in destination dbs")

        return patient_mapping_table

    
    def create_encounter_mapping_table(self):
        metadata = MetaData()
        encounter_mapping_table = Table(ENCOUNTER_MAPPING_TABLE_NAME, metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('patient_id', Integer, nullable=False),
            Column('encounter_id', Integer, nullable=False),
            Column('nd_encounter_id', Numeric(20, 0), nullable=False),
            Column('created_by', String(50), nullable=False),
            Column('created_at', TIMESTAMP, nullable=False),
            Column('updated_by', String(50), nullable=False),
            Column('updated_at', TIMESTAMP, nullable=False),
            UniqueConstraint('nd_encounter_id', name='uq_nd_encounter_id'),
            UniqueConstraint('encounter_id', name='uq_encounter_id'),
            Index('ix_patient_id', 'patient_id'),
            Index('ix_encounter_id', 'encounter_id'),
        )

        logger.info(f"created {ENCOUNTER_MAPPING_TABLE_NAME} table in destination dbs")
        return encounter_mapping_table
    

    def insert_data(self):
        batch_size = 10000
        patient_mapping_table = self.create_patient_mapping_table()
        encounter_mapping_table = self.create_encounter_mapping_table()
        
        with self.source_engine.connect() as conn:
            query = text(self.mapping_query)
            result = conn.execute(query)
            data = [dict(row) for row in result.mappings()]

        total_batches = len(data) // batch_size + 1
        created_at = datetime.now(timezone.utc)
        updated_at = datetime.now(timezone.utc)

        with self.dest_engine.connect() as conn:
            existing_patient_ids = {}
            patient_encounters = {}

            for batch_number in range(total_batches):
                batch_start = batch_number * batch_size
                batch_end = min((batch_number + 1) * batch_size, len(data))
                batch_data = data[batch_start:batch_end]

                patients_data = []
                encounters_data = []

                trans = conn.begin()

                for row in batch_data:
                    patient_id = row['client_patient_id']
                    patient_registration_date = row['patient_registration_date']

                    if patient_id not in existing_patient_ids:
                        offset = random.choice(OFFSET_RANGE)
                        patients_data.append({
                            'patient_id': patient_id,
                            'offset': offset,
                            'patient_registration_date': patient_registration_date,
                            'created_by': self.USERNAME,
                            'created_at': created_at,
                            'updated_by': self.USERNAME,
                            'updated_at': updated_at
                        })
                        existing_patient_ids[patient_id] = {'offset': offset}
                try:
                    if patients_data:
                        result = conn.execute(patient_mapping_table.insert(), patients_data)

                    for row in batch_data:
                        patient_id = row['client_patient_id']
                        encounter_id = row["client_encounter_id"]

                        if encounter_id:
                            nd_patient_id_query = patient_mapping_table.select().where(
                                patient_mapping_table.c.patient_id == patient_id
                            )
                            nd_patient_id = conn.execute(nd_patient_id_query).scalar()

                            if not nd_patient_id:
                                raise ValueError(f"nd_patient_id not found for patient_id: {patient_id}")
                            
                            if patient_id not in patient_encounters:
                                encid_counter = 1
                            else:
                                encid_counter += 1

                            nd_encounter_id = f"{nd_patient_id}{encid_counter:04d}"

                            patient_encounters[patient_id] = [nd_encounter_id, nd_encounter_id]

                            encounters_data.append({
                                'patient_id': patient_id,
                                'encounter_id': encounter_id,
                                'nd_encounter_id': nd_encounter_id,
                                'created_by': self.USERNAME,
                                'created_at': created_at,
                                'updated_by': self.USERNAME,
                                'updated_at': updated_at
                            })

                    if encounters_data:
                        conn.execute(encounter_mapping_table.insert(), encounters_data)

                    trans.commit()
                    logger.info(f"Batch {batch_number + 1}/{total_batches} inserted successfully.")
                except SQLAlchemyError as e:
                    trans.rollback()
                    logger.error(f"Error in batch {batch_number + 1}/{total_batches}: {e}")
                    break


