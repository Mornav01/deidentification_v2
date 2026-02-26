from django.db import models
from typing import TypedDict, List
from core.dbPkg.dbhandler import NDDBHandler
from core.dbPkg.schemas import MappingDbConfig, SecondaryPIIConfig
from core.dbPkg.unversal_table import UniversalTableConfig


class DbConfigType(TypedDict):
    connection_str: str

class DbStatsGeneratedStatus:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3


class DbDetailsModel(models.Model):
    id = models.AutoField(primary_key=True)
    db_name = models.CharField(max_length=255, unique=True)

    source_db_config = models.JSONField(default=dict)   #DbConfigType
    destination_db_config = models.JSONField(default=dict)  #DbConfigType

    """
     mapping_db_config:
     phi-db-config: 
    """
    run_config = models.JSONField(default=dict)
    stats_generated_status = models.IntegerField(
        choices=[
            (DbStatsGeneratedStatus.NOT_STARTED, "Not Started"),
            (DbStatsGeneratedStatus.IN_PROGRESS, "In Progress"),
            (DbStatsGeneratedStatus.COMPLETED, "Completed"),
            (DbStatsGeneratedStatus.FAILED, "Failed"),
        ],
        default=DbStatsGeneratedStatus.NOT_STARTED,
    )
    db_stats = models.JSONField(default=dict)
    is_phi_marking_locked = models.BooleanField(default=False)

    def get_source_db_connection(self) -> NDDBHandler:
        return NDDBHandler(self.source_db_config["connection_str"])

    def get_destination_db_connection(self) -> NDDBHandler:
        return NDDBHandler(self.destination_db_config["connection_str"])

    def marked_stats_generation_as_completed(self):
        self.stats_generated_status = DbStatsGeneratedStatus.COMPLETED
        self.save()

    def marked_stats_generation_as_failed(self):
        self.stats_generated_status = DbStatsGeneratedStatus.FAILED
        self.save()

    def get_mapping_db_config(self) -> MappingDbConfig:
        if "mapping_db_config" not in self.run_config:
            raise Exception(
                "mapping db config is not defined, please define it using the notebook"
            )
        return self.run_config["mapping_db_config"]
    
    def get_pii_config(self) -> MappingDbConfig:
        if "pii_config" not in self.run_config:
            raise Exception(
                "pii config is not defined, please define it using the notebook"
            )
        return self.run_config["pii_config"]
    
    def get_pii_db_config(self) -> MappingDbConfig:
        if "pii_db_config" not in self.run_config:
            raise Exception(
                "pii db config is not defined, please define it using the notebook"
            )
        return self.run_config["pii_db_config"]
    

    def get_secondary_pii_config(self) -> List[SecondaryPIIConfig]:
        if "secondary_pii_config" not in self.run_config:
            raise Exception(
                "secondary pii config is not defined, please define it using the notebook"
            )
        return self.run_config["secondary_pii_config"]
    
    def get_universal_tables_config(self) -> list[UniversalTableConfig]:
        if "universal_tables_config" not in self.run_config:
            raise Exception(
                "universal_tables_config is not defined, please define it using the notebook"
            )
        return self.run_config["universal_tables_config"]
