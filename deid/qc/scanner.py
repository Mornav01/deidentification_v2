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
        available = set(sample_data[0].keys()) if sample_data else set()
        mdb: MappingDb | None = None
        # Reverse-map ALL patient-rule columns present in the dest sample, not just the primary
        # reference column — a table may carry more than one patient id (e.g. mergelogs From/To),
        # and each must be verifiable by the id mapping-correctness detector.
        patient_cols = [c["column_name"] for c in table_config.get("columns_details", [])
                        if c.get("de_identification_rule") in ("PATIENT_ID", "REFERENCE_PID")]
        pat_col = table_config["reference_patient_id_column"]
        if pat_col and pat_col not in patient_cols:
            patient_cols.append(pat_col)
        for c in patient_cols:
            if c not in available:
                logger.warning("[QC] patient column '%s' not in dest sample — skipping its mapping", c)
        present_pat_cols = [c for c in patient_cols if c in available]
        if present_pat_cols:
            # These dest columns hold the ND id value (the deid pipeline wrote the surrogate into
            # each), so we reverse-map them back to source id + offset.
            nd_patient_ids = [row[c] for c in present_pat_cols for row in sample_data if row.get(c) is not None]
            if nd_patient_ids:
                mdb = MappingDb(mapping_db_config)
                patient_dict = mdb.get_reverse_patients_dict(list(dict.fromkeys(nd_patient_ids)))
        enc_col = table_config["reference_enc_id_column"]
        if enc_col is not None and enc_col in available:
            nd_enc_ids = [row[enc_col] for row in sample_data if row.get(enc_col) is not None]
            mdb = mdb or MappingDb(mapping_db_config)
            enc_dict = mdb.get_reverse_encounter_dict(nd_enc_ids)
            # DATE_OFFSET via the encounter route needs the offset of each encounter's patient.
            # enc_dict maps nd_encounter_id → {'patient_id': nd_patient_id}; make sure those
            # nd_patient_ids are in patient_dict (they won't be for an encounter-only table).
            enc_nd_pids = [v["patient_id"] for v in enc_dict.values() if v.get("patient_id") is not None]
            missing = [p for p in enc_nd_pids if p not in patient_dict]
            if missing:
                patient_dict.update(mdb.get_reverse_patients_dict(missing))
        elif enc_col is not None:
            logger.warning("[QC] reference_enc_id_column '%s' not in dest sample — skipping encounter mapping", enc_col)
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
    def get_pii_info(self, sample_data: list | None = None, table_config: dict | None = None) -> dict:
        """Load master PHI values for the sampled patients (flat {value: value}) for the exact-match
        scan. No-op ({}) unless ``qc_config['pii_master_conn_str']`` is set.

        This is the in-pipeline scan's PHI feed (previously always empty, i.e. the master reference
        was unwired). The precise per-patient, master-referenced Part-3 audit lives in
        ``deid.qc.master_phi`` — this remains a conservative complement.
        """
        conn_str = (self.qc_config or {}).get("pii_master_conn_str")
        if not conn_str or not sample_data or not table_config:
            return {}
        id_col = table_config.get("reference_patient_id_column")
        if not id_col:
            return {}
        nd_ids = list({row[id_col] for row in sample_data if row.get(id_col) is not None})
        if not nd_ids:
            return {}
        try:
            from deid.qc.master_phi import make_pii_loader
            loader = make_pii_loader(conn_str, self.qc_config.get("pii_columns"))
            phi_by_nd = loader(nd_ids)
        except Exception as exc:
            logger.warning("[QC] get_pii_info could not load master PHI: %s", exc)
            return {}
        flat: dict = {}
        for cols in phi_by_nd.values():
            for val in cols.values():
                if val is None:
                    continue
                s = str(val).strip()
                if s:
                    flat[s] = s
        return flat


    @staticmethod
    def _prune_config_to_available(table_config: dict, available: set, table_name: str) -> None:
        """Reconcile configured columns with the dest sample's *actual* columns (in place).

        Dest column casing is not guaranteed (the main deid path lowercases, but decrypt/
        pass-through tables preserve source case), so match case-INSENSITIVELY and rewrite each
        kept column to the real dest casing — detectors read ``row[column_name]``, whose keys are
        the actual dest column names. Columns with no case-insensitive match are dropped.
        """
        by_lower = {c.lower(): c for c in available}
        cols = table_config.get("columns_details", [])
        kept, dropped = [], []
        for c in cols:
            actual = by_lower.get(str(c["column_name"]).lower())
            if actual is None:
                dropped.append(c["column_name"])
            else:
                c["column_name"] = actual  # remap to the real dest casing for row access
                kept.append(c)
        if dropped:
            logger.warning("[QC] [%s] columns in rules but not in dest — skipped: %s", table_name, dropped)
        table_config["columns_details"] = kept
        for ref_key in ("reference_patient_id_column", "reference_enc_id_column"):
            v = table_config.get(ref_key)
            if v is not None:
                table_config[ref_key] = by_lower.get(str(v).lower())  # actual casing, or None if absent

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

        # Prune config to columns that actually exist in the dest sample. The rules CSV can
        # reference columns absent from a given dest table (stale/over-broad rules); a detector
        # reading such a column would KeyError. Only prune when we have a non-empty sample.
        if sample_data:
            self._prune_config_to_available(table_config, set(sample_data[0].keys()), table_name)

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
            pii_info = self.get_pii_info(sample_data, table_config)
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
