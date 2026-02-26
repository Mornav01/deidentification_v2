from django.db import models

class DumpDataStatus:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3
    INTERRUPTED = 4


class DataDump(models.Model):
    id = models.AutoField(primary_key=True)
    dump_name = models.CharField(unique=True, max_length=200)
    config = models.JSONField()
    status  = models.IntegerField(
        choices=[
            (DumpDataStatus.NOT_STARTED, "Not Started"),
            (DumpDataStatus.IN_PROGRESS, "In Progress"),
            (DumpDataStatus.COMPLETED, "Completed"),
            (DumpDataStatus.FAILED, "Failed"),
            (DumpDataStatus.INTERRUPTED, "Interrupted"),
        ],
        default=DumpDataStatus.NOT_STARTED,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"DataDump(id={self.id})"


class RestoreDump(models.Model):
    id = models.AutoField(primary_key=True)
    dump = models.ForeignKey(
        "DataDump", on_delete=models.CASCADE, related_name="restore_details"
    )
    config = models.JSONField()
    status  = models.IntegerField(
        choices=[
            (DumpDataStatus.NOT_STARTED, "Not Started"),
            (DumpDataStatus.IN_PROGRESS, "In Progress"),
            (DumpDataStatus.COMPLETED, "Completed"),
            (DumpDataStatus.FAILED, "Failed"),
            (DumpDataStatus.INTERRUPTED, "Interrupted"),
        ],
        default=DumpDataStatus.NOT_STARTED,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"RestoreDump(id={self.id}, dump={self.dump.id})"

