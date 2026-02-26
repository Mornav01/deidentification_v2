from .rules import Rules, PatientIDRule, EncounterIDRule, ReferencePIDRule, AppointmentIDRule, MaskRule, DateOffsetRule, RuleBase, StaticDateOffsetRule, ZIPCodeRule, PatientDOBRule
from .unstruct.genericnotes import GenericNotesRule
import pandas as pd
from typing import Dict, Any
import re
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
    Rules.ZIP_CODE.value:ZIPCodeRule,
    Rules.PATIENT_DOB.value: PatientDOBRule,
    Rules.GENERIC_NOTES.value: GenericNotesRule
}

class DeIdentifier:
    def __init__(self, df: pd.DataFrame, config: list[Dict[str, Any]], db_details_obj:DbDetailsModel, key_phi_columns: tuple) -> None:
        self.df = df
        self.config = config
        self.db_details_obj = db_details_obj
        self._notes_rule = None  # cache for NotesRule
        self.key_phi_columns = key_phi_columns

    def apply_rules(self) -> pd.DataFrame:
        # Split configs into NotesRule configs and others
        notes_configs = [c for c in self.config if c.get("de_identification_rule") == Rules.NOTES.value and c["is_phi"]]
        other_configs = [c for c in self.config if not (c.get("de_identification_rule") == Rules.NOTES.value) and c["is_phi"]]

        # Process NotesRule first
        for column_config in notes_configs:
            nd_logger.info("####################################################")
            if self._notes_rule is None:
                from .unstruct.notes import NotesRule  # local import to avoid circular issues
                self._notes_rule = NotesRule(self.db_details_obj, self.key_phi_columns)
            self.df = self._notes_rule.apply(self.df, column_config)

        # Process all other rules
        for column_config in other_configs:
            nd_logger.info("####################################################")
            rule_type = column_config.get("de_identification_rule")
            rule_class = RULE_DISPATCHER.get(rule_type)
            if rule_class:
                rule_impl = rule_class()
                self.df = rule_impl.apply(self.df, column_config)

        return self.df
