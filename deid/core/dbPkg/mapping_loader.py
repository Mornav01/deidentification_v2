from .schemas import MappingDbConfig

from pydantic import validate_call
from sqlalchemy import MetaData, Table, select

from deid.core.dbPkg.dbhandler import create_read_only_engine


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
        self.engine = create_read_only_engine(connection_string)
        self.metadata = MetaData()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def close_connection(self):
        self.engine.dispose()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_nd_patients_dict(
        self,
        ids: list,
        id_column: str = "patient_id",
    ) -> dict:
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
        self, encounter_ids: list
    ) -> dict:
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

    def _source_patient_id_column(self) -> str:
        """Physical source-identifier column of ``patient_mapping_table``.

        The modernized mapping table names this column after the source identifier
        (``mapping_tables.patient.identifier_columns[0]``, e.g. ``PATIENT_PATIENTID``)
        rather than a fixed ``patient_id`` — ``mapping_populator.bulk_insert_patient_mappings``
        builds it that way. The QC path passes that name in as ``patient_identifier_columns``;
        fall back to ``patient_id`` for the legacy fixed-schema tables.
        """
        cols = self.mapping_db_config.get("patient_identifier_columns") or []
        return cols[0] if cols else "patient_id"

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_reverse_patients_dict(
        self, nd_patient_ids: list
    ) -> dict:
        table = Table("patient_mapping_table", self.metadata, autoload_with=self.engine)
        stmt = select(table).where(table.c.nd_patient_id.in_(nd_patient_ids))

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        src_id_col = self._source_patient_id_column()
        mapping_dict = {}
        for row in rows:
            row_dict = row._asdict()
            mapping_dict[row_dict["nd_patient_id"]] = {
                "patient_id": row_dict.get(src_id_col),
                "offset": row_dict["offset"],
            }
        return mapping_dict

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_reverse_encounter_dict(
        self, nd_encounter_ids: list
    ) -> dict:
        table = Table("encounter_mapping_table", self.metadata, autoload_with=self.engine)
        stmt = select(table).where(table.c.nd_encounter_id.in_(nd_encounter_ids))

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        # The encounter → patient link used for the DATE_OFFSET chain must be the ``nd_patient_id``
        # bridge (encounter_mapping_table carries nd_patient_id in the modernized schema), because
        # the caller keys the patient reverse dict — which holds the offset — by nd_patient_id.
        # ``.get`` so a legacy table without an nd_patient_id column degrades to None, not KeyError.
        mapping_dict = {}
        for row in rows:
            row_dict = row._asdict()
            mapping_dict[row_dict["nd_encounter_id"]] = {
                "encounter_id": row_dict.get("encounter_id"),
                # Kept under 'patient_id' for the detector contract. Prefer the modernized
                # nd_patient_id bridge (keys the patient reverse dict → offset); fall back to the
                # legacy source patient_id column for the old fixed-schema mapping table.
                "patient_id": row_dict.get("nd_patient_id", row_dict.get("patient_id")),
            }
        return mapping_dict
