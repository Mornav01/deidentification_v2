import traceback
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import DbDetailsModel, DbConfigType, TableDeIdntStatus, TableDetailsModel
from worker.models import Task, Chain
from django.db import transaction
from deIdentification.settings import CREATE_SAVEPOINT_IN_TRANSACTION
from nd_api.schemas.table_config import TableDetailsForUI, ColumnDetailsForUI
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from nd_api.decorator import conditional_authentication
from django.conf import settings

class RequestCtx(TypedDict):
    source_db_config: DbConfigType
    destination_db_config: DbConfigType

@conditional_authentication
class RegisterNewDbView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        try:
            data: RequestCtx = request.data
            db_name = data["db_name"]
            source_config = data["source_db_config"]
            destination_config = data["destination_db_config"]
            is_exists = DbDetailsModel.objects.filter(db_name=db_name).count() > 0
            if is_exists:
                return Response({"message": f"db name: {db_name}, already exists in the database, please choose different name"}, status=status.HTTP_200_OK)
            db_details = DbDetailsModel(
                db_name=db_name,
                source_db_config=source_config,
                destination_db_config=destination_config,
            )
            db_details.save()
            create_stats_generation_tasks(db_obj=db_details)
            nd_logger.info(f"Db registered successfully by user {request.user}")
            return Response({"message": "Db registered successfully"}, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f'Internal server error: {e}')
            nd_logger.error(traceback.format_exc())
            return Response({"message": 'Internal server error: {e}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@conditional_authentication
class GetAllDbsView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        try:
            db_details = DbDetailsModel.objects.all()
            db_details_list = []
            for db_detail in db_details:
                db_details_list.append(
                    {
                        "id": db_detail.id,
                        "db_name": db_detail.db_name,
                    }
                )
            nd_logger.info(f"get all db call, completed successfully")
            return Response(db_details_list, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f'Internal server error: {e}')
            nd_logger.error(traceback.format_exc())
            return Response({"message": 'Internal server error: {e}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@conditional_authentication
class GetDbDetailsView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, db_id: int):
        try:
            db_obj = DbDetailsModel.objects.get(id=db_id)
        except DbDetailsModel.DoesNotExist:
            return Response({"message": f"db with db_id: {db_id} not exists", "success": False}, status=status.HTTP_400_BAD_REQUEST)
        try:
            response_json = {
                "db_id": db_id,
                "db_name": db_obj.db_name,
                "source_db_config": db_obj.source_db_config,
                "destination_db_config": db_obj.destination_db_config,
                "stats_generated_status": db_obj.stats_generated_status,
                "is_phi_marking_locked": db_obj.is_phi_marking_locked,
                "tables_status": {
                    "not_started": {
                        "count": db_obj.tables_details.filter(
                            table_status=TableDeIdntStatus.NOT_STARTED
                        ).count(),
                    },
                    "in_progress": {
                        "count": db_obj.tables_details.filter(
                            table_status=TableDeIdntStatus.IN_PROGRESS
                        ).count(),
                    },
                    "completed": {
                        "count": db_obj.tables_details.filter(
                            table_status=TableDeIdntStatus.COMPLETED
                        ).count(),
                    },
                    "failed": {
                        "count": db_obj.tables_details.filter(
                            table_status=TableDeIdntStatus.FAILED
                        ).count(),
                    },
                    "tables_phi_marking_locked": {
                        "count": db_obj.tables_details.filter(
                            is_phi_marking_locked=True
                        ).count(),
                    },
                    "tables_phi_marking_done": {
                        "count": db_obj.tables_details.filter(
                            is_phi_marking_done=True
                        ).count(),
                    },
                }
            }
            return Response(response_json, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error, {e} for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def create_stats_generation_tasks(db_obj: DbDetailsModel):
    with transaction.atomic(savepoint=CREATE_SAVEPOINT_IN_TRANSACTION):
        chain, created = Chain.all_objects.get_or_create(
            reference_uuid=f"stats_generation_{db_obj.id}"
        )
        if created:
            task = Task.create_task(
                chain=chain,
                fn=run_stats_generation_task,
                arguments={"db_details_id": db_obj.id},
                dependencies=[],
            )
            return {"message": f"Stats Generation Task created. task id: {task.id}"}
        else:
            return {"message": f"Stats Generation Task already exists. chain id: {chain.id}"}

def process_table(table, db_details_obj: DbDetailsModel, rerun: bool = False):
    source_db_connection = db_details_obj.get_source_db_connection()
    table_details_obj, created = TableDetailsModel.register_table(table, db_details_obj)
    if not created:
        table_stats = {
            "rows_count": table_details_obj.rows_count,
            "size": table_details_obj.size,
            "fks_to": [],
            "fks_from": [],
        }
        if not rerun:
            return table, table_stats, table_details_obj.rows_count
    table_details_obj.table_details_for_ui = _get_default_table_details_for_ui(
        source_db_connection.get_column_names(table)
    )
    table_details_obj.rows_count = source_db_connection.get_rows_count(table)
    table_details_obj.size = source_db_connection.get_table_size(table)
    table_details_obj.save()

    table_stats = {
        "rows_count": table_details_obj.rows_count,
        "size": table_details_obj.size,
        "fks_to": source_db_connection.fks_to_for_table(table),
        "fks_from": source_db_connection.fks_from_for_table(table),
    }

    source_db_connection.close()

    return table, table_stats, table_details_obj.rows_count

def run_stats_generation_task(db_details_id: int, all_tables: list, rerun: bool = False, dependencies: list[Task] = []):
    nd_logger.info(f"inside the run stats generation task for db-id:  {db_details_id}")
    db_details_obj = DbDetailsModel.objects.get(id=db_details_id)
    source_db_connection = db_details_obj.get_source_db_connection()
    try:
        db_stats = {
            "db_id": db_details_obj.id,
            "db_size": source_db_connection.get_db_size(),
            "tables_stats": {},
            "db_analytics": {
                "table_with_max_rows": "dummyvalue",
                "table_with_min_rows": "dummyvalue",
                "table_with_max_size": "1 GB",
                "table_with_min_size": "1 Kb",
                # "table_with_max_rows": source_db_connection.table_with_max_rows(),
                # "table_with_min_rows": source_db_connection.table_with_min_rows(),
                # "table_with_max_size": source_db_connection.table_with_max_size(),
                # "table_with_min_size": source_db_connection.table_with_min_size(),
            },
        }
        distribution = {
            "less_than_1000": 0,
            "between_1000_and_10000": 0,
            "between_10000_and_100000": 0,
            "between_100000_and_1000000": 0,
            "greater_than_1000000": 0,
            "unknown": 0
        }
        # all_tables = source_db_connection.get_all_tables()
        db_stats["tables_stats"] = {}

        # Adjust max_workers based on your system capabilities
        with ThreadPoolExecutor(max_workers=settings.STATS_GENERATION_MAX_WORKER_COUNT) as executor:
            future_to_table = {
                executor.submit(process_table, table, db_details_obj, rerun): table
                for table in all_tables
            }

            for future in tqdm(as_completed(future_to_table), total=len(all_tables), desc="Generating table stats"):
                table, table_stats, rows_count = future.result()
                db_stats["tables_stats"][table] = table_stats

                if rows_count:
                    if rows_count < 1000:
                        distribution["less_than_1000"] += 1
                    elif rows_count < 10000:
                        distribution["between_1000_and_10000"] += 1
                    elif rows_count < 100000:
                        distribution["between_10000_and_100000"] += 1
                    elif rows_count < 1000000:
                        distribution["between_100000_and_1000000"] += 1
                    else:
                        distribution["greater_than_1000000"] += 1
                else:
                    distribution["unknown"] += 1

        db_stats["graphs"] = {
            "rows_count_distribution": distribution,
        }
        db_details_obj.db_stats = db_stats
        db_details_obj.save()
        db_details_obj.marked_stats_generation_as_completed()
        return {}
    finally:
        source_db_connection.close()


def _get_default_table_details_for_ui(columns_names: list[str]) -> TableDetailsForUI:
    """
    "ignore_rows": {
        "operator": "or",
        "columns": [{"name": "UserType", "value": 3, "condition": "neq"}]
    }
    """
    columns_details = []
    for column_name in columns_names:
        columns_details.append(
            ColumnDetailsForUI(
                column_name=column_name,
                is_phi=False,
                de_identification_rule=None,
                add_to_phi_table=False,
                column_name_for_phi_table=None,
                ignore_rows={},
                reference_mapping={}
            )
        )
    table_details_for_ui = TableDetailsForUI(
        columns_details=columns_details,
        ignore_rows={},
        batch_size=1000,
        reference_patient_id_column=None,
        reference_enc_id_column=None,
        reference_mapping={}
    )
    return table_details_for_ui
