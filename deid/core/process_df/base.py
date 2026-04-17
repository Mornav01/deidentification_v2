import polars as pl
from typing import Dict, Any
from pydantic import validate_call
from deid.config.task_models import LogLevel
from deid.core.log_publisher import make_log_record, maybe_log
from .rules import (
    Rules,
    PatientIDRule,
    EncounterIDRule,
    ReferencePIDRule,
    AppointmentIDRule,
    MaskRule,
    DateOffsetRule,
    StaticDateOffsetRule,
    ZIPCodeRule,
    PatientDOBRule,
    RuleBase,
)
from .unstruct.genericnotes import GenericNotesRule
from deid.core.logger import nd_logger


RULE_DISPATCHER: Dict[str, RuleBase] = {
    Rules.PATIENT_ID.value: PatientIDRule,
    Rules.ENCOUNTER_ID.value: EncounterIDRule,
    Rules.REFERENCE_PID.value: ReferencePIDRule,
    Rules.APPOINTMENT_ID.value: AppointmentIDRule,
    Rules.MASK.value: MaskRule,
    Rules.DATE_OFFSET.value: DateOffsetRule,
    Rules.STATIC_OFFSET.value: StaticDateOffsetRule,
    Rules.ZIP_CODE.value: ZIPCodeRule,
    Rules.PATIENT_DOB.value: PatientDOBRule,
    Rules.GENERIC_NOTES.value: GenericNotesRule,
}


class DeIdentifier:
    """Orchestrate all de-identification rules for a single table batch.

    Created once per table (not per batch) so that the NotesRule NLP model
    is loaded lazily and cached across all batches of the same table.
    """

    def __init__(
        self,
        df: pl.DataFrame,
        config: list[Dict[str, Any]],
        pii_config: dict | None = None,
        pii_db_conn_str: str | None = None,
        secondary_pii_configs: list | None = None,
        key_phi_columns: tuple = (),
        offset_days: int = 34,
        run_config: dict | None = None,
    ) -> None:
        self.df = df
        self.config = config
        self.pii_config = pii_config
        self.pii_db_conn_str = pii_db_conn_str
        self.secondary_pii_configs = secondary_pii_configs
        self._notes_rule = None  # lazy-loaded to avoid reloading NLP model per batch
        self.key_phi_columns = key_phi_columns
        self.offset_days = offset_days
        self.run_config = run_config or {}

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def apply_rules(self) -> pl.DataFrame:
        notes_configs = [
            c for c in self.config
            if c.get("de_identification_rule") == Rules.NOTES.value and c["is_phi"]
        ]
        other_configs = [
            c for c in self.config
            if c.get("de_identification_rule") != Rules.NOTES.value and c["is_phi"]
        ]

        # Apply NotesRule first (NLP-heavy; must run before column values are overwritten).
        for column_config in notes_configs:
            nd_logger.info("####################################################")
            if self._notes_rule is None:
                from .unstruct.notes import NotesRule  # local import — avoids circular dep
                self._notes_rule = NotesRule(
                    self.pii_config, self.pii_db_conn_str,
                    self.secondary_pii_configs, self.key_phi_columns
                )
            else:
                # Refresh key_phi_columns in case reference mapping added new columns.
                self._notes_rule.key_phi_columns = self.key_phi_columns
            self.df = self._notes_rule.apply(self.df, column_config)

        # Apply all other structured rules.
        for column_config in other_configs:
            nd_logger.info("####################################################")
            rule_type = column_config.get("de_identification_rule")
            col_name = column_config.get("column_name", "")
            rule_class = RULE_DISPATCHER.get(rule_type)
            # Dynamic PATIENT_* rules (e.g. PATIENT_PATIENTID, PATIENT_CHARTID) all
            # resolve to _resolved_nd_patient_id, so PatientIDRule handles them all.
            if rule_class is None and rule_type and rule_type.startswith("PATIENT_"):
                rule_class = PatientIDRule
            if rule_class:
                if rule_class is StaticDateOffsetRule:
                    rule_instance = rule_class(offset_days=self.offset_days)
                else:
                    rule_instance = rule_class()

                before_nulls = self.df[col_name].null_count() if col_name in self.df.columns else 0
                self.df = rule_instance.apply(self.df, column_config)
                after_nulls = self.df[col_name].null_count() if col_name in self.df.columns else 0

                new_nulls = after_nulls - before_nulls
                if new_nulls > 0:
                    maybe_log(self.run_config, make_log_record(
                        LogLevel.WARNING, self.run_config.get("table_name", "unknown"), "deidentify",
                        f"{rule_type} on column '{col_name}': {new_nulls} rows got null values (possible missing mappings)",
                        column=col_name,
                    ))

        return self.df
