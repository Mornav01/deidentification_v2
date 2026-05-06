import logging
import time

from deid.config.table_schemas import TableDetailsForUI
from deid.config.task_models import DataCountResult
from deid.qc.generator import DataGenerator
from deid.qc.builders import DectorMapping, Detector
from deid.qc.schema import OutputSchemaForTable, FinalQCResult, ColumnQCResult
from deid.core.dbPkg.mapping_loader import MappingDb
from deid.core.dbPkg.dbhandler import NDDBHandler
from pydantic import validate_call

logger = logging.getLogger("deid.qc")


class LoadMappingData:

    @classmethod
    def load(cls, sample_data: list[dict], table_config: TableDetailsForUI, mapping_db_config: dict):
        assert isinstance(sample_data, list), "sample_data must be a list"
        assert isinstance(table_config, dict), "table_config must be a dict"

        patient_dict, enc_dict = {}, {}
        if table_config["reference_patient_id_column"] is not None:
            col_name = table_config["reference_patient_id_column"]
            nd_patient_ids = [row[col_name] for row in sample_data]
            patient_dict = MappingDb(mapping_db_config).get_reverse_patients_dict(nd_patient_ids)
        if table_config["reference_enc_id_column"] is not None:
            col_name = table_config["reference_enc_id_column"]
            nd_enc_ids = [row[col_name] for row in sample_data]
            enc_dict = MappingDb(mapping_db_config).get_reverse_encounter_dict(nd_enc_ids)
        return patient_dict, enc_dict


class DbScanner:
    def __init__(self, source_connection_string: str, dest_connection_string: str, mapping_db_config: dict, qc_config: dict):
        assert source_connection_string, "source_connection_string must not be empty"
        assert dest_connection_string, "dest_connection_string must not be empty"

        self.source_handler = NDDBHandler(source_connection_string, read_only=True)
        self.dest_handler = NDDBHandler(dest_connection_string)
        self.mapping_db_config = mapping_db_config
        self.qc_config = qc_config

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_important_columns(self, table_config: dict):
        important_cols = []
        for col_conf in table_config["columns_details"]:
            if col_conf["is_phi"]:
                important_cols.append(col_conf["column_name"])
        return important_cols

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_structured_detectors(self, sample_data, table_config: dict) -> list[tuple[str, Detector]]:
        detectors = []
        patinet_dict, enc_dict = LoadMappingData.load(sample_data, table_config, self.mapping_db_config)
        for col_conf in table_config["columns_details"]:
            if col_conf["is_phi"] and col_conf["de_identification_rule"] not in ("NOTES", "GENERIC_NOTES"):
                detector_cls: Detector = DectorMapping[col_conf["de_identification_rule"]]
                detector_obj = detector_cls(patient_mapping_dict=patinet_dict, enc_mapping_dict=enc_dict, qc_config=self.qc_config, column_config=col_conf, patient_id_column=table_config["reference_patient_id_column"], enc_id_column=table_config["reference_enc_id_column"])
                detectors.append((col_conf["column_name"], detector_obj))
        return detectors

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_unstructured_detectors(self, sample_data, table_config: dict) -> list[tuple[str, Detector]]:
        detectors = []
        patinet_dict, enc_dict = LoadMappingData.load(sample_data, table_config, self.mapping_db_config)
        for col_conf in table_config["columns_details"]:
            if col_conf["is_phi"] and col_conf["de_identification_rule"] in ("NOTES", "GENERIC_NOTES"):
                detector_cls: Detector = DectorMapping[col_conf["de_identification_rule"]]
                detector_obj = detector_cls(patient_mapping_dict=patinet_dict, enc_mapping_dict=enc_dict, qc_config=self.qc_config, column_config=col_conf, patient_id_column=table_config["reference_patient_id_column"], enc_id_column=table_config["reference_enc_id_column"])
                detectors.append((col_conf["column_name"], detector_obj))
        return detectors

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_pii_info(self):
        return {}


    @validate_call(config=dict(arbitrary_types_allowed=True))
    def scan_table(self, table_name: str, table_config: dict, ignore_row_count: int = 0) -> OutputSchemaForTable:
        assert table_name, "table_name must not be empty"
        assert table_config, "table_config must not be empty"

        t0 = time.monotonic()
        logger.info("[QC] [%s] Starting scan...", table_name)

        data_generator = DataGenerator(self.source_handler.engine, self.dest_handler.engine)
        important_cols = self.get_important_columns(table_config)
        logger.info("[QC] [%s] PHI columns to check: %s", table_name, important_cols)

        # ── Structured checks ─────────────────────────────────────────────
        logger.info("[QC] [%s] Generating structured sample...", table_name)
        t1 = time.monotonic()
        sample_size, source_data, sample_data = data_generator.generate_sample(table_name, important_cols, is_structured=True)
        logger.info(
            "[QC] [%s] Structured sample ready: %d rows (%.1fs)",
            table_name, len(sample_data), time.monotonic() - t1,
        )

        detectors = self.get_structured_detectors(sample_data, table_config)
        logger.info("[QC] [%s] Running %d structured detector(s)...", table_name, len(detectors))
        columns_qc_result = {}
        for i, (col_name, detector) in enumerate(detectors, 1):
            t2 = time.monotonic()
            result = detector.is_deidentified(before_rows=source_data, after_rows=sample_data, ignore_condition=table_config.get("ignore_config", {}))
            columns_qc_result[col_name] = result
            status = "PASS" if result["failed_count"] == 0 else f"FAIL({result['failed_count']})"
            logger.info(
                "[QC] [%s]   [%d/%d] %s: %s (passed=%d, failed=%d, %.1fs)",
                table_name, i, len(detectors), col_name, status,
                result["passed_count"], result["failed_count"],
                time.monotonic() - t2,
            )

        # ── Unstructured checks ───────────────────────────────────────────
        logger.info("[QC] [%s] Generating unstructured sample...", table_name)
        t1 = time.monotonic()
        sample_size, source_data, sample_data = data_generator.generate_sample(table_name, important_cols, is_structured=False)
        logger.info(
            "[QC] [%s] Unstructured sample ready: %d rows (%.1fs)",
            table_name, len(sample_data), time.monotonic() - t1,
        )

        unstructured_detectors = self.get_unstructured_detectors(sample_data, table_config)
        if unstructured_detectors:
            logger.info("[QC] [%s] Running %d unstructured detector(s)...", table_name, len(unstructured_detectors))
            pii_info = self.get_pii_info()
            for i, (col_name, detector) in enumerate(unstructured_detectors, 1):
                t2 = time.monotonic()
                result = detector.is_deidentified(before_rows=source_data, after_rows=sample_data, ignore_condition=table_config.get("ignore_config", {}), pii_info=pii_info)
                columns_qc_result[col_name] = result
                status = "PASS" if result["failed_count"] == 0 else f"FAIL({result['failed_count']})"
                logger.info(
                    "[QC] [%s]   [%d/%d] %s: %s (passed=%d, failed=%d, %.1fs)",
                    table_name, i, len(unstructured_detectors), col_name, status,
                    result["passed_count"], result["failed_count"],
                    time.monotonic() - t2,
                )

        # ── Row count check ───────────────────────────────────────────────
        logger.info("[QC] [%s] Checking row counts...", table_name)
        data_count = is_data_discrepancy_present(self.source_handler, self.dest_handler, table_name, ignore_row_count)
        data_count_dict = data_count.model_dump()
        logger.info(
            "[QC] [%s] Row counts: source=%d, dest=%d, ignored=%d",
            table_name,
            data_count_dict["source_rows_count"],
            data_count_dict["dest_rows_count"],
            data_count_dict["ignore_rows_count"],
        )

        final_qc_result = self.get_final_result(data_count_dict, columns_qc_result)
        verdict = "PASSED" if final_qc_result["is_qc_passed"] else "FAILED"
        logger.info(
            "[QC] [%s] Scan complete: %s — %d column(s) checked in %.1fs. %s",
            table_name, verdict, len(columns_qc_result),
            time.monotonic() - t0,
            final_qc_result["reason"] or "",
        )

        output_result = OutputSchemaForTable(
            unstruct_sample_size=len(sample_data),
            table_name=table_name,
            **data_count_dict,
            ColumnsQCResult=columns_qc_result,
            final_qc_result=final_qc_result
        )
        return output_result

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_final_result(self, data_count_result: dict, columns_qc_result: dict[str, ColumnQCResult]):
        final_qc_result = FinalQCResult(
            is_qc_passed=True,
            reason=""
        )
        if data_count_result["source_rows_count"] != (data_count_result["dest_rows_count"] + data_count_result["ignore_rows_count"]
        ):
            final_qc_result["is_qc_passed"] = False
            final_qc_result["reason"] += f"data discrepancy present. "

        columns_failed = []
        for colname, result in columns_qc_result.items():
            if result["failed_count"] > 0:
                final_qc_result["is_qc_passed"] = False
                for key, value in result.get("remarks", {}).items():
                    columns_failed.append(colname)
        if len(columns_failed)>0:
            final_qc_result["reason"] += "QC Failed on columns: " + ", ".join(columns_failed)

        return final_qc_result




@validate_call(config=dict(arbitrary_types_allowed=True))
def is_data_discrepancy_present(source_handler: NDDBHandler, dest_handler: NDDBHandler, table_name: str, ignore_row_count: int = 0) -> DataCountResult:
    assert table_name, "table_name must not be empty"

    actual_count = source_handler.get_rows_count(table_name)
    dest_count = dest_handler.get_rows_count(table_name)
    return DataCountResult(source_rows_count=actual_count, dest_rows_count=dest_count, ignore_rows_count=ignore_row_count)
