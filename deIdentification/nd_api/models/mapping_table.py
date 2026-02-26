from django.db import models
from django.conf import settings


def _generate_new_nd_patient_id(patient_id: int) -> int:
    return settings.PATIENT_ID_PREFIX


def _generate_new_nd_encounter_id(encounter_id: int) -> int:
    return 100


def _generate_date_offset_value() -> int:
    return 10


class PatientMappingTable(models.Model):
    id = models.AutoField(primary_key=True)
    patient_id = models.BigIntegerField(db_index=True)
    nd_patient_id = models.BigIntegerField()
    date_offset = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"PatientMappingTable(patient_id={self.patient_id}, nd_patient_id={self.nd_patient_id}, id={self.id})"

    @classmethod
    def get_nd_patient_id(cls, patient_id: int) -> int:
        patient_mapping_table, created = cls.objects.get_or_create(
            patient_id=patient_id
        )
        if created:
            patient_mapping_table.nd_patient_id = _generate_new_nd_patient_id(
                patient_id
            )
            patient_mapping_table.date_offset = _generate_date_offset_value()
            patient_mapping_table.save()
        return patient_mapping_table.nd_patient_id


class EncounterMappingTable(models.Model):
    id = models.AutoField(primary_key=True)
    patient = models.ForeignKey(PatientMappingTable, on_delete=models.CASCADE)
    encounter_id = models.BigIntegerField(db_index=True)
    nd_encounter_id = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"EncounterMappingTable(patient={self.patient}, encounter_id={self.encounter_id}, nd_encounter_id={self.nd_encounter_id}, id={self.id})"

    @classmethod
    def get_nd_encounter_id(cls, encounter_id: int) -> int:
        encounter_mapping_table, created = cls.objects.get_or_create(
            encounter_id=encounter_id
        )
        if created:
            encounter_mapping_table.nd_encounter_id = _generate_new_nd_encounter_id(
                encounter_id
            )
            encounter_mapping_table.save()
        return encounter_mapping_table.nd_encounter_id
