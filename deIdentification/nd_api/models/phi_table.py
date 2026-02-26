from django.db import models


class PhiTable(models.Model):
    id = models.AutoField(primary_key=True)
    patient_id = models.BigIntegerField(db_index=True)
    phi_details = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"PhiTable(patient_id={self.patient_id}, id={self.id})"

    @classmethod
    def update_phi_details(
        cls, patient_id: int, phi_column_name: str, phi_column_value: str
    ):
        phi_table = cls.objects.get_or_create(patient_id=patient_id)
        phi_table.phi_details[phi_column_name] = phi_column_value
        phi_table.save()
