from .schemas import PatientMappingDict, EncounterMappingDict, MappingDbConfig

from pydantic import validate_call
from sqlalchemy import MetaData, Table, create_engine, select
from deid.core.logger import nd_logger


class MappingDb:
    def __init__(self, mapping_db_config: MappingDbConfig):
        self.mapping_db_config = mapping_db_config
        self.inhouse_mapping_table = mapping_db_config.get(
            "inhouse_mapping_table", False
        )
        self._connect()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _connect(self):
        if self.inhouse_mapping_table:
            raise NotImplementedError("In-house mapping table is not implemented")
        connection_string = self.mapping_db_config["connection_str"]
        self.engine = create_engine(connection_string)
        self.metadata = MetaData()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def close_connection(self):
        self.engine.dispose()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_nd_patients_dict(
        self,
        ids: list[int],
        id_column: str = "patient_id",
    ) -> dict[int, dict]:
        table = Table("patient_mapping_table", self.metadata, autoload_with=self.engine)
        col_attr = table.c[id_column]
        stmt = select(table).where(col_attr.in_(ids))

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        mapping_dict = {}
        for row in rows:
            row_dict = row._asdict()
            key = row_dict[id_column]
            mapping_dict[key] = row_dict
        return mapping_dict

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_nd_encounter_dict(
        self, encounter_ids: list[int]
    ) -> dict[int, EncounterMappingDict]:
        table = Table("encounter_mapping_table", self.metadata, autoload_with=self.engine)
        stmt = select(table).where(table.c.encounter_id.in_(encounter_ids))

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        mapping_dict = {}
        for row in rows:
            row_dict = row._asdict()
            mapping_dict[row_dict["encounter_id"]] = {
                "encounter_id": row_dict["encounter_id"],
                "nd_encounter_id": row_dict["nd_encounter_id"],
                "patient_id": row_dict["patient_id"],
            }
        return mapping_dict

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_reverse_patients_dict(
        self, nd_patient_ids: list[int]
    ) -> dict[int, PatientMappingDict]:
        table = Table("patient_mapping_table", self.metadata, autoload_with=self.engine)
        stmt = select(table).where(table.c.nd_patient_id.in_(nd_patient_ids))

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        mapping_dict = {}
        for row in rows:
            row_dict = row._asdict()
            mapping_dict[row_dict["nd_patient_id"]] = {
                "patient_id": row_dict["patient_id"],
                "offset": row_dict["offset"],
            }
        return mapping_dict

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_reverse_encounter_dict(
        self, nd_encounter_ids: list[int]
    ) -> dict[int, EncounterMappingDict]:
        table = Table("encounter_mapping_table", self.metadata, autoload_with=self.engine)
        stmt = select(table).where(table.c.nd_encounter_id.in_(nd_encounter_ids))

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        mapping_dict = {}
        for row in rows:
            row_dict = row._asdict()
            mapping_dict[row_dict["nd_encounter_id"]] = {
                "encounter_id": row_dict["encounter_id"],
                "patient_id": row_dict["patient_id"],
            }
        return mapping_dict
