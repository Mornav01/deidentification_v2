import polars as pl
from typing import Dict, Any
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
from nd_api.models import DbDetailsModel
from deIdentification.nd_logger import nd_logger


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
        db_details_obj: DbDetailsModel,
        key_phi_columns: tuple,
    ) -> None:
        self.df = df
        self.config = config
        self.db_details_obj = db_details_obj
        self._notes_rule = None  # lazy-loaded to avoid reloading NLP model per batch
        self.key_phi_columns = key_phi_columns

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
                self._notes_rule = NotesRule(self.db_details_obj, self.key_phi_columns)
            else:
                # Refresh key_phi_columns in case reference mapping added new columns.
                self._notes_rule.key_phi_columns = self.key_phi_columns
            self.df = self._notes_rule.apply(self.df, column_config)

        # Apply all other structured rules.
        for column_config in other_configs:
            nd_logger.info("####################################################")
            rule_type = column_config.get("de_identification_rule")
            rule_class = RULE_DISPATCHER.get(rule_type)
            if rule_class:
                self.df = rule_class().apply(self.df, column_config)

        return self.df
