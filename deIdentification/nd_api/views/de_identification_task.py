import traceback
from django.conf import settings
from django.db import transaction
from rest_framework import status
from worker.models import Task, Chain
from rest_framework.views import APIView
from core.dbPkg.dbhandler import NDDBHandler
from rest_framework.response import Response
from nd_api.models import TableDetailsModel, DbDetailsModel, IgnoreRowsDeIdentificaiton
from core.process_df.main import start_de_identification_for_table
from nd_api.hooks import de_identification_failure_hook_for_table
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class StopDeIdentificationView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, table_id: int):
        try:
            try:
                table_details_obj = TableDetailsModel.objects.get(id=table_id)
            except TableDetailsModel.DoesNotExist:
                return Response({"message": "Table details not found", "success": False}, status=status.HTTP_400_BAD_REQUEST)
            with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
                try:
                    chain = Chain.all_objects.get(
                        reference_uuid=table_details_obj.get_chain_reference_uuid()
                    )
                    chain.soft_delete_and_save()
                except Chain.DoesNotExist as e:
                    pass
                table_details_obj.marked_as_not_started()
            nd_logger.info(f"De Identification stopped successfully, table_id: {table_id}")
            return Response({"message": "De Identification stopped successfully", "success": False}, status.HTTP_200_OK)
        except Exception as e:
            message = f"StopDeIdentificationView.post: Internal server error : {e}, for user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
        
@conditional_authentication
class DeIdentifyTableView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, table_id: int):
        try:
            try:
                table_details_obj = TableDetailsModel.objects.get(id=table_id)
            except TableDetailsModel.DoesNotExist:
                return Response(
                    {"message": "table_id does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if not table_details_obj.is_phi_marking_locked:
                return Response(
                    {
                        "message": "Failed, cannot start deIdentifiation, PHI marking is not locked",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            tasks, chain = create_deidentification_task(table_obj=table_details_obj)
            message = f"DeIdentifyTableView.get: Table de-identification started successfully, table_id: {table_id}, {request.user}"
            nd_logger.error(message)
            return Response(
                {
                    "message": "Table de-identification started successfully",
                    "success": True,
                    "chain_id": chain.id,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"DeIdentifyTableView.get: Internal server error : {e}, for user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def create_deidentification_task(table_obj: TableDetailsModel, delete_table=True):
    chain, created = Chain.all_objects.get_or_create(
        reference_uuid=table_obj.get_chain_reference_uuid()
    )
    if not created:
        chain.revive_and_save()
    tables_config = table_obj.table_details_for_ui
    
    batch_size = settings.BATCH_SIZE_DURING_DE_IDENTIFICATION
    table_obj.marked_as_in_progress_if_required()
    tasks = []
    gt_lt_ranges = None
    dest_connection: NDDBHandler = table_obj.db.get_destination_db_connection()
    try:
        if delete_table:
            dest_connection.drop_table(table_obj.table_name)
            ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(db_name=table_obj.db.db_name, table_name=table_obj.table_name)
            nd_logger.info(f"Dropping ignore rows for {table_obj.table_name}, {table_obj.db.db_name}")
            ignore_rows.delete()

        source_connection: NDDBHandler = table_obj.db.get_source_db_connection()
        try:
            gt_lt_ranges = source_connection.get_keyset_pagination_ranges(
                table_obj.table_name, "nd_auto_increment_id", batch_size
            )
        finally:
            source_connection.close()
    finally:
        dest_connection.close()

    if gt_lt_ranges:  # If keyset pagination worked
        for row in gt_lt_ranges:
            task = Task.create_task(
                fn=start_de_identification_for_table,
                chain=chain,
                arguments={
                    "table_id": table_obj.id,
                    "batch_size": batch_size,  # Optional
                    "offset": {"gt": row["gt"], "lt": row["lt"]},
                    "table_config": tables_config,
                },
                hooks={"failure": de_identification_failure_hook_for_table},
            )
            tasks.append(task)
    else:  # Fall back to offset-based batching
        for offset in range(0, table_obj.rows_count, batch_size):
            task = Task.create_task(
                fn=start_de_identification_for_table,
                chain=chain,
                arguments={
                    "table_id": table_obj.id,
                    "batch_size": batch_size,
                    "offset": offset,
                    "table_config": tables_config,
                },
                hooks={"failure": de_identification_failure_hook_for_table},
            )
            tasks.append(task)

    cleanup_task = Task.create_task(
        fn=marked_complete_and_clean_up_tasks,
        chain=chain,
        dependencies=tasks,
        arguments={"table_id": table_obj.id, "chain_id": chain.id},
    )
    tasks.append(cleanup_task)
    return tasks, chain

def marked_complete_and_clean_up_tasks(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_details_obj = TableDetailsModel.objects.get(id=table_id)
    table_details_obj.marked_as_completed()
    chain = Chain.objects.get(id=chain_id)
    with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
        chain.soft_delete_and_save()
