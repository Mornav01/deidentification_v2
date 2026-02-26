from django.db import models


class IgnoreRowsDeIdentificaiton(models.Model):
    id = models.AutoField(primary_key=True)
    db_name = models.CharField(max_length=200)
    table_name = models.CharField(max_length=200)
    row = models.JSONField(default={})
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"IgnoreRowsDeIdentificaiton(id={self.id}, {self.db_name}, {self.table_name})"

