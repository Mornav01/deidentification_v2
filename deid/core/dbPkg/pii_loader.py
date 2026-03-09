from deid.config.table_schemas import TableDetailsForUI
from .schemas import EncounterMappingDict

from pydantic import validate_call
from sqlalchemy import MetaData, Table, create_engine, select


class PIIDb:
    def __init__(self, pii_db_config: dict):
        self.pii_db_config = pii_db_config
        self._connect_master()
        self._connect_insurance()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _connect_master(self):
        connection_string = self.pii_db_config["master_connection_str"]
        self.master_engine = create_engine(connection_string)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _connect_insurance(self):
        connection_string = self.pii_db_config["insurance_connection_str"]
        self.insurance_engine = create_engine(connection_string)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def close_master_connection(self):
        self.master_engine.dispose()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def close_insurance_connection(self):
        self.insurance_engine.dispose()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_pii_data(self, patient_ids: list[int]) -> dict[int, dict]:
        metadata = MetaData()
        pii_table = Table("pii_data_table", metadata, autoload_with=self.master_engine)
        stmt = select(pii_table).where(pii_table.c.patient_id.in_(patient_ids))

        with self.master_engine.connect() as conn:
            results = conn.execute(stmt).fetchall()

        pii_data_dict = {}
        for row in results:
            row_dict = row._asdict()
            pii_data_dict[row_dict["patient_id"]] = row_dict
        return pii_data_dict


@validate_call(config=dict(arbitrary_types_allowed=True))
def _get_patient_ids_from_enc_ids(
    encounter_ids: list[int],
    encounter_id_mapping: dict[int, EncounterMappingDict],
) -> list[int]:
    patient_ids = []
    for encid in encounter_ids:
        if (encid is None) or (encid not in encounter_id_mapping):
            continue
        patient_ids.append(encounter_id_mapping[encid]["patient_id"])
    return patient_ids


class PIITableLoader:
    def __init__(self, pii_db_config: dict):
        self.pii_db_connection: PIIDb | None = None
        self.pii_db_config = pii_db_config

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _has_notes_columns(self, table_config: dict):
        for col_conf in table_config["columns_details"]:
            if col_conf["de_identification_rule"] == "NOTES":
                return True
        return False

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def load_pii_table(
        self,
        table_config: dict,
        patient_ids: list[int],
        encounter_ids: list[int],
        encounter_id_mapping: dict[int, EncounterMappingDict],
    ) -> dict[int, dict]:
        pii_data = {}
        if not self._has_notes_columns(table_config=table_config):
            return pii_data
        if self.pii_db_connection is None:
            self.pii_db_connection = PIIDb(self.pii_db_config)
        if len(patient_ids) > 0:
            pii_data = self.get_nd_patients_dict(patient_ids)
        else:
            patientids = _get_patient_ids_from_enc_ids(encounter_ids, encounter_id_mapping)
            pii_data = self.get_nd_patients_dict(patientids)
        self.pii_db_connection.close_master_connection()
        return pii_data

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def load_insurance_table(
        self,
        table_config: dict,
        patient_ids: list[int],
        encounter_ids: list[int],
        encounter_id_mapping: dict[int, EncounterMappingDict],
    ) -> dict:
        if not self._has_notes_columns(table_config=table_config):
            return {"metadata": {}, "rows": {}}
        if self.pii_db_connection is None:
            self.pii_db_connection = PIIDb(self.pii_db_config)

        metadata = MetaData()
        insurance_table = Table(
            "master_insurance_table", metadata,
            autoload_with=self.pii_db_connection.insurance_engine,
        )

        if len(patient_ids) > 0:
            stmt = select(insurance_table).where(
                insurance_table.c.patient_id.in_(patient_ids)
            )
        else:
            stmt = select(insurance_table).where(
                insurance_table.c.encounter_id.in_(encounter_ids)
            )

        with self.pii_db_connection.insurance_engine.connect() as conn:
            results = conn.execute(stmt).fetchall()

        insurnace_data_dict = {}
        for row in results:
            row_dict = row._asdict()
            if row_dict["patient_id"] not in insurnace_data_dict:
                insurnace_data_dict[row_dict["patient_id"]] = []
            insurnace_data_dict[row_dict["patient_id"]].append(row_dict)

        self.pii_db_connection.close_insurance_connection()
        return {
            "metadata": self.pii_db_config.get("insurance_metadata", {}),
            "rows": insurnace_data_dict,
        }

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_nd_patients_dict(self, patient_ids: list[int]) -> dict[int, dict]:
        return self.pii_db_connection.get_pii_data(patient_ids)
