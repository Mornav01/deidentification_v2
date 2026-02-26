import os
import traceback
from nd_api.models import TableDetailsModel, TableQCStatus
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from deIdentification.nd_logger import nd_logger
import hashlib
from worker.models import Task, Chain
from urllib.parse import urlparse, parse_qs
import subprocess
from google.cloud import storage
import tempfile
from django.conf import settings
from nd_api.decorator import conditional_authentication


def calculate_checksum(file_path, algorithm="md5"):
    hash_func = hashlib.new(algorithm)
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        return str(e)

def parse_mysql_connection_string(conn_str):
    url = urlparse(conn_str)
    return {
        "host": url.hostname,
        "user": url.username,
        "password": url.password,
        "database": url.path.lstrip("/"),
        "port": url.port or 3306
    }
@conditional_authentication
class CloudMovement(APIView):
    authentication_classes = []

    def get(self, request, table_id: int):
        try:
            table_obj = TableDetailsModel.objects.get(id=table_id)
            
            if table_obj.qc_status == TableQCStatus.COMPLETED :
                chain, created = Chain.all_objects.get_or_create(
                    reference_uuid=table_obj.get_chain_reference_uuid()
                )
                if not created:
                    chain.revive_and_save()
                task = Task.create_task(
                    fn=take_dump_and_upload_to_cloud,
                    chain=chain,
                    arguments={
                        "table_id": table_obj.id,
                    },
                    hooks={"failure": failure_hook_clouad_movement},
                )
                return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            else:
                Response(f"Table is not qc Passed: {table_id}", status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def take_dump_and_upload_to_cloud(table_id: int, reupload=False):
    table_obj = TableDetailsModel.objects.get(id=table_id)
    if table_obj.cloud_uploaded and (not reupload):
        nd_logger.info(f"Table {table_obj.table_name} is already uploaded on the cloud, not uploading again")
    conn_str = table_obj.db.destination_db_config["connection_str"]
    creds = parse_mysql_connection_string(conn_str)
    blob_name = f"{settings.CLIENT_NAME}/{table_obj.db.db_name}/{table_obj.table_name}.sql"
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as temp_file:
        dump_path = temp_file.name
        
        cmd = [
            "mysqldump",
            "-h", creds["host"],
            "-u", creds["user"],
            f"--password={creds['password']}",
            creds["database"],
            table_obj.table_name,
            "--single-transaction",
            "--quick",
            "--no-create-db"
        ]

        with open(dump_path, "w") as f:
            subprocess.run(cmd, stdout=f, check=True)
        md5sum = calculate_checksum(dump_path)
        upload_to_gcs(dump_path, blob_name)
        table_obj.md5sum = md5sum
        table_obj.cloud_uploaded = True
        table_obj.save()


def upload_to_gcs(file_path, blob_name):
    client = storage.Client()
    bucket = client.bucket(settings.CLOUD_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    nd_logger.info(f"Uploaded {file_path} to gs://{settings.CLOUD_BUCKET_NAME}/{blob_name}")


def failure_hook_clouad_movement(chain: Chain):
    pass
