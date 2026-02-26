import traceback
import uuid
from typing import TypedDict
from rest_framework.views import APIView
from django.conf import settings
from django.db import transaction
from rest_framework import status
from worker.models import Task, Chain
from rest_framework.response import Response
from nd_api.models import TableDetailsModel
from nd_api.models.table_details import TableDeIdntStatus, TableQCStatus
from nd_api.hooks import qc_failure_hook_for_table
from deIdentification.nd_logger import nd_logger
from qc_package.scanner import DbScanner
from nd_api.decorator import conditional_authentication
from qc_package.schema import OutputSchemaForTable


@conditional_authentication
class TableQCView(APIView):
    authentication_classes = []

    def post(self, request):
        data = request.data
        tables_id_for_qc = data.get('tables', [])

        if not tables_id_for_qc:
            return Response(
                {"message": "No table IDs provided", "success": False},
                status=status.HTTP_400_BAD_REQUEST,
            )

        tasks = []
        try:
            for table_id in tables_id_for_qc:
                try:
                    table_details_obj = TableDetailsModel.objects.get(id=table_id)
                except TableDetailsModel.DoesNotExist:
                    nd_logger.warning(f"Table ID {table_id} does not exist.")
                    return Response(
                        {"message": f"table_id {table_id} does not exist", "success": False},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                chain, created = Chain.all_objects.get_or_create(
                    reference_uuid=table_details_obj.get_qc_chain_reference_uuid()
                )
                if not created:
                    chain.revive_and_save()

                table_details_obj.update_qc_status('in_progress')

                task = Task.create_task(
                    fn=qc_task,
                    chain=chain,
                    arguments={"table_id": table_details_obj.id},
                    hooks={"failure": qc_failure_hook_for_table},
                )
                tasks.append(task)

            cleanup_task = Task.create_task(
                fn=marked_complete_and_clean_up_tasks,
                chain=chain,
                dependencies=tasks,
                arguments={"table_id": table_details_obj.id, "chain_id": chain.id},
            )
            tasks.append(cleanup_task)

            return Response(
                {
                    "message": "QC for Tables started successfully",
                    "success": True,
                    "chain_id": chain.id,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )



def qc_task(table_id: int):
    try:

        table_obj = TableDetailsModel.objects.get(id=table_id)
        db = table_obj.db
        table_config = table_obj.table_details_for_ui

        db_scanner = DbScanner(
            db.source_db_config['connection_str'],
            db.destination_db_config['connection_str'],
            db.run_config["mapping_db_config"],
            table_obj.get_qc_config()
        )

        result = db_scanner.scan_table(table_obj.table_name, table_config, table_id)
        if result is None:
            raise ValueError(f"QC scan failed for table {table_obj.table_name}")
        if result["final_qc_result"]["is_qc_passed"]:
             table_obj.qc_status = TableQCStatus.COMPLETED
        else:
             table_obj.qc_status = TableQCStatus.FAILED
        table_obj.qc_result = result
        table_obj.save()
    except TableDetailsModel.DoesNotExist:
        nd_logger.error(f"QC Task failed: Table ID {table_id} not found.")
        raise Exception(f"QC Task failed: Table ID {table_id} not found.")
    except Exception as e:
        nd_logger.error(f"QC Task error: {e}, table_id: {table_id}")
        nd_logger.error(traceback.format_exc())
        raise Exception(f"QC Task error: {e}, table_id: {table_id}")


def marked_complete_and_clean_up_tasks(table_id: int, chain_id: int, dependencies: list[Task] = []):
    try:
        table_details_obj = TableDetailsModel.objects.get(id=table_id)
        table_details_obj.update_qc_status(TableQCStatus.COMPLETED)

        chain = Chain.objects.get(id=chain_id)
        with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
            chain.soft_delete_and_save()
    except TableDetailsModel.DoesNotExist:
        nd_logger.error(f"Table ID {table_id} not found for cleanup.")
    except Chain.DoesNotExist:
        nd_logger.error(f"Chain ID {chain_id} not found for cleanup.")
    except Exception as e:
        nd_logger.error(f"Error in cleanup task: {e}")
        nd_logger.error(traceback.format_exc())


@conditional_authentication
class TablesQCListView(APIView):
    authentication_classes = []

    def get(self, request):
        try:
            # Extract 'status' query parameter (default: NOT_STARTED)
            qc_status_filter = request.query_params.get('status', TableQCStatus.NOT_STARTED)
            qc_status_filter = int(qc_status_filter)  # Convert string to int

            print('qc_status_filter', qc_status_filter)
            if qc_status_filter not in [TableQCStatus.NOT_STARTED, TableQCStatus.IN_PROGRESS, TableQCStatus.COMPLETED, TableQCStatus.FAILED]:
                return Response(
                    {"message": "Invalid status filter", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Base Query (For QC In Progress, we don't filter by `table_status`)
            filter_criteria = {"qc_status": qc_status_filter}

            # If QC is NOT_STARTED, filter only COMPLETED tables
            if qc_status_filter == TableQCStatus.NOT_STARTED:
                filter_criteria["table_status"] = TableDeIdntStatus.COMPLETED
            # breakpoint()
            tables = TableDetailsModel.objects.filter(**filter_criteria).values(
                "id", "table_name", "size", "rows_count", "qc_status"
            )

            return Response(
                {"data": list(tables)},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, Unable to fetch QC tables"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class TableQCStatusView(APIView):
    authentication_classes = []

    def get(self, request, table_id):
        try:
            table = TableDetailsModel.objects.get(id=table_id)
            return Response(
                {
                    "table_id": table.id,
                    "qc_status": table.qc_status,
                },
                status=status.HTTP_200_OK,
            )
        except TableDetailsModel.DoesNotExist:
            return Response(
                {"message": f"Table ID {table_id} not found", "success": False},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class QCResultView(APIView):
    authentication_classes = []

    def get(self, request, table_id):
        try:
            table = TableDetailsModel.objects.get(id=table_id)
            return Response(
                {
                    "table_id": table.id,
                    "qc_result": table.qc_result,
                },
                status=status.HTTP_200_OK,
            )
        except TableDetailsModel.DoesNotExist:
            return Response(
                {"message": f"Table ID {table_id} not found", "success": False},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
