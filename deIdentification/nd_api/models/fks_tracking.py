from django.db import models


class FksTracking(models.Model):
    id = models.AutoField(primary_key=True)
    from_table = models.CharField(max_length=255)
    to_table = models.CharField(max_length=255)
    from_column = models.CharField(max_length=255)
    to_column = models.CharField(max_length=255)
    db = models.ForeignKey(
        "DbDetailsModel", on_delete=models.CASCADE, related_name="fks_tracking"
    )

    class Meta:
        unique_together = ("from_table", "from_column", "to_table", "to_column")

    def __str__(self):
        return f"{self.id} - {self.from_table} - {self.from_column} -> {self.to_table} - {self.to_column}"
