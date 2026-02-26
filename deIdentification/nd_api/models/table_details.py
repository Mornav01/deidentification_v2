from django.db import models
from .db_details import DbDetailsModel
from deIdentification.nd_logger import nd_logger

class TableDeIdntStatus:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3
    PARTIAL_COMPLETED = 4

class TableQCStatus:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3
    PARTIAL_COMPLETED = 4


class TableDetailsModel(models.Model):
    id = models.AutoField(primary_key=True)
    table_name = models.CharField(max_length=255)
    size = models.CharField(max_length=100, null=True)
    rows_count = models.IntegerField(null=True)
    db = models.ForeignKey(
        "DbDetailsModel", on_delete=models.CASCADE, related_name="tables_details"
    )
    table_details_for_ui = models.JSONField(default=dict)
    table_status = models.IntegerField(
        choices=[
            (TableDeIdntStatus.NOT_STARTED, "Not Started"),
            (TableDeIdntStatus.IN_PROGRESS, "In Progress"),
            (TableDeIdntStatus.COMPLETED, "Completed"),
            (TableDeIdntStatus.FAILED, "Failed"),
            # (TableDeIdntStatus.PARTIAL_COMPLETED, "Partial Completed"),
        ],
        default=TableDeIdntStatus.NOT_STARTED,
    )
    failure_remarks = models.JSONField(default=dict, null=True)
    is_phi_marking_done = models.BooleanField(default=False)

    qc_status = models.IntegerField(
        choices=[
            (TableQCStatus.NOT_STARTED, "Not Started"),
            (TableQCStatus.IN_PROGRESS, "In Progress"),
            (TableQCStatus.COMPLETED, "Completed"),
            (TableQCStatus.FAILED, "Failed"),
        ],
        default=TableQCStatus.NOT_STARTED,
    )

    is_phi_marking_locked = models.BooleanField(default=None, null=True)
    qc_result = models.JSONField(default=dict)
    run_config = models.JSONField(default=dict)

    is_required = models.BooleanField(default=True)
    cloud_uploaded = models.BooleanField(default=False)
    md5sum = models.CharField(default=None, null=True)

    class Meta:
        unique_together = ("table_name", "db")

    def __str__(self):
        return f"{self.table_name} - {self.db.db_name} - {self.id}"

    @classmethod
    def register_table(cls, table_name: str, db: DbDetailsModel):
        table_obj, created = cls.objects.get_or_create(table_name=table_name, db=db)
        return table_obj, created

    def get_chain_reference_uuid(self):
        return f"db_{self.db.id}_table_{self.id}"

    def get_qc_chain_reference_uuid(self):
        return f"qc_db_{self.db.id}_table_{self.id}"
    
    @classmethod
    def get_table_id_from_chain_reference_uuid(cls, reference_uuid: str):
        return int(reference_uuid.split("_")[-1])

    def marked_as_failed(self):
        self.table_status = TableDeIdntStatus.FAILED
        self.save()

    def marked_as_completed(self):
        self.table_status = TableDeIdntStatus.COMPLETED
        self.save()

    def marked_as_in_progress(self):
        if self.table_status != TableDeIdntStatus.FAILED:
            self.table_status = TableDeIdntStatus.IN_PROGRESS
            self.save()
    
        
        
    def marked_as_in_progress_if_required(self):
        nd_logger.info("inside marking as in progress if required")
        self.table_status = TableDeIdntStatus.IN_PROGRESS
        self.save()
        
        # nd_logger.info("done marking as in progress if required")
        # nd_logger.info("inside marking as in progress if required")
        # # breakpoint()
        # if self.table_status == TableDeIdntStatus.NOT_STARTED:
        #     self.table_status = TableDeIdntStatus.IN_PROGRESS
        #     self.save()
        # nd_logger.info("done marking as in progress if required")
        

    def marked_as_not_started(self):
        self.table_status = TableDeIdntStatus.NOT_STARTED
        self.save()

    def get_qc_config(self):
        qc_config = self.db.run_config.get("qc_config", {})
        return qc_config

    def update_qc_status(self, status):
        if status == 'not_started':
            self.qc_status = TableQCStatus.NOT_STARTED
        elif status == 'in_progress':
            self.qc_status = TableQCStatus.IN_PROGRESS
        elif status == 'completed':
            self.qc_status = TableQCStatus.COMPLETED
        elif status == 'failed':
            self.qc_status = TableQCStatus.FAILED
        
        self.save()
