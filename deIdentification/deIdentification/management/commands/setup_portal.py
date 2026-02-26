import os
import json
import logging
from django.core.management.base import BaseCommand
from nd_scripts.create_account import create_account
from keycloakauth.keycloakapi import NDKeyCloakAPI, UserDetails

logger = logging.getLogger(__name__)

_CPU_COUNT = os.cpu_count()
if not _CPU_COUNT:
    logger.warning(f"unable to find cpu count. setting cpu count to 8")
    _CPU_COUNT = 8

DE_IDENTIFICATION_SETUP_CONFIG = os.environ.get("DE_IDENTIFICATION_SETUP_CONFIG", "/nd_deployment/setup_config.json")

PERMISSION_TO_ROLE_MAPPING = {
    "admin": {
    "AddDataBase": {"has_permission": True},
    "UploadPHIConfigCSV": {"has_permission": True},
    "PHIMarkingCompletedTick": {"has_permission": True},
    "TableQCTick": {"has_permission": True},
    "UnLockPHIMarkingDB": {"has_permission": True},
    "LockPHIMarkingDB": {"has_permission": True},
    "LockPHIMarkingTable": {"has_permission": True},
    "UnLockPHIMarkingTable": {"has_permission": True},
    "StartDeIdentificationButton": {"has_permission": True},
},
    "project_manager": {
    "AddDataBase": {"has_permission": False},
    "UploadPHIConfigCSV": {"has_permission": True},
    "PHIMarkingCompletedTick": {"has_permission": True},
    "TableQCTick": {"has_permission": True},
    "UnLockPHIMarkingDB": {"has_permission": False},
    "LockPHIMarkingDB": {"has_permission": True},
    "LockPHIMarkingTable": {"has_permission": True},
    "UnLockPHIMarkingTable": {"has_permission": False},
    "StartDeIdentificationButton": {"has_permission": False},
},
    "project_supporter": {
    "AddDataBase": {"has_permission": False},
    "UploadPHIConfigCSV": {"has_permission": False},
    "PHIMarkingCompletedTick": {"has_permission": True},
    "TableQCTick": {"has_permission": True},
    "UnLockPHIMarkingDB": {"has_permission": False},
    "LockPHIMarkingDB": {"has_permission": False},
    "LockPHIMarkingTable": {"has_permission": False},
    "UnLockPHIMarkingTable": {"has_permission": False},
    "StartDeIdentificationButton": {"has_permission": False},
}
}


class Command(BaseCommand):
    """provides options to add args and start the taskworker"""

    help = "Setting up the portal"
    
    def handle(self, *args, **options):
        """calls teh work function of the Taskworker class"""
        if not os.path.exists(DE_IDENTIFICATION_SETUP_CONFIG):
            logger.info("No config found for setup ......")
            return
        setup_config = load_setup_config()
        if setup_config.get("setup_completed", False):
            logger.info("Setup already completed .....")
            return
        create_all_accounts(setup_config)
        write_setup_status()


def create_all_accounts(setup_config: dict):
    users_details = setup_config.get("users_details", {})
    for user_details in users_details:
        user_details = UserDetails(
            email=user_details["user_name"],
            password=user_details["password"],
            username=user_details["user_name"],
            first_name=user_details["user_name"],
            last_name="ND"
        )
        user_role = user_details.get("role", "project_supporter")
        permissions = PERMISSION_TO_ROLE_MAPPING[user_role]
        creation_resposne = create_account(user_details, permissions)
        logger.info(creation_resposne)
        

def load_setup_config():
    with open(DE_IDENTIFICATION_SETUP_CONFIG, "r") as fp:
        return json.load(fp)

def write_setup_status():
    setup_config = load_setup_config()
    setup_config["setup_completed"] = True
    with open(DE_IDENTIFICATION_SETUP_CONFIG, "w") as fp:
        json.dump(setup_config, fp)

