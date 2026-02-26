import os
import django
import sys
# Set up Django environment
sys.path.append('/Users/neurodiscoveryai/Desktop/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.models import DbDetailsModel, TableDetailsModel
from worker.models import Task, Chain
from django.contrib.auth.models import User
from keycloakauth.rolemodel import RoleModel, get_default_permissions
from nd_scripts.create_account import create_account
from nd_api.views.db_views import create_stats_generation_tasks
from core.process.main import start_de_identification_for_table
from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.views.db_views import run_stats_generation_task
from qc_package.scanner import DbScanner
import pandas as pd
import concurrent.futures
import threading

def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()

qc_config = {
    "PATIENT_ID": {"prefix_value": "1001000", "length_of_value": 15},
    "ENCOUNTER_ID": {"prefix_value": "1001000", "length_of_value": 19},
}
db = DbDetailsModel.objects.get(db_name="deid_prod")

db_scanner = DbScanner(
    db.source_db_config['connection_str'],
    db.destination_db_config['connection_str'],
    db.run_config["mapping_db_config"],
    qc_config
)

failure_lock = threading.Lock()
code_failure = []

def scan_and_save_table(table_obj):
    table_name = table_obj.table_name
    
    if len(table_obj.qc_result) >= 5:
        return table_name 

    print(f"Scanning table: {table_name}") 
    
    try:
        table_config = table_obj.table_details_for_ui
        output_result = db_scanner.scan_table(table_name, table_config, table_obj.id)
        table_obj.qc_result = output_result
        table_obj.save()
        
        return table_name

    except Exception as e:
        print(f"ERROR processing table {table_name}: {e}")
        with failure_lock:
            code_failure.append(table_name)
        return None

tables_to_process = TableDetailsModel.objects.filter(table_status=2)
print(f"Deidentified tables count: {len(tables_to_process)}")

MAX_WORKERS = 2

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [
        executor.submit(scan_and_save_table, table_obj) 
        for table_obj in tables_to_process
        if len(table_obj.qc_result) < 5
    ]

    for future in concurrent.futures.as_completed(futures):
        try:
            result = future.result()
        except Exception as e:
            print(f"A future generated an unhandled exception: {e}")

print("\n--- Summary ---")
print(f"Total tables where code failed: {len(code_failure)}")
print(f"Failed tables: {code_failure}")