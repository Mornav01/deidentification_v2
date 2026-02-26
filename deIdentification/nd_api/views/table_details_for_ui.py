import traceback
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from nd_api.models import DbDetailsModel, TableDetailsModel, TableDeIdntStatus
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class TablesForUIView(APIView):
    authentication_classes = []

    def get(self, request, db_id: int):
        try:
            try:
                db_model_obj = DbDetailsModel.objects.get(id=db_id)
            except DbDetailsModel.DoesNotExist:
                return Response(
                    {"message": "db_id does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            tables_objs = db_model_obj.tables_details.all()
            tables_details = {}
            for table_obj in tables_objs:
                table_obj: TableDetailsModel = table_obj
                tables_details[table_obj.table_name] = {
                    "table_id": table_obj.id,
                    "processing_status": table_obj.table_status
                }
            return Response(tables_details, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"TablesForUIView: Internal server error : {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

class TablesConfigForUIView(APIView):
    def get(self, request, table_id: int):
        try:
            try:
                table_obj = TableDetailsModel.objects.get(id=table_id)
            except DbDetailsModel.DoesNotExist:
                return Response(
                    {"message": f"table_id: {table_id} does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            response_json = {
                "table_name": table_obj.table_name,
                "table_id": table_obj.id,
                "table_details_for_ui": table_obj.table_details_for_ui,
                "table_size": table_obj.size,
                "rows_count": table_obj.rows_count,
                "processing_status": table_obj.table_status,
                "fks": [],
                "is_phi_marking_done": table_obj.is_phi_marking_done,
                "is_phi_marking_locked": table_obj.is_phi_marking_locked,
                "qc_status": table_obj.qc_status
            }
            return Response(
                response_json,
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"TablesConfigForUIView.get: Internal server error : {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def post(self, request, table_id: int):
        try:
            data = request.data
            try:
                table_details_obj = TableDetailsModel.objects.get(id=table_id)
            except TableDetailsModel.DoesNotExist:
                return Response(
                    {"message": "table_id does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            db_details: DbDetailsModel = table_details_obj.db
            if db_details.is_phi_marking_locked:
                return Response(
                    {
                        "message": "Failed, PHI Marking is locked, cant save the config",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if _is_diff_present_in_dict(table_details_obj.table_details_for_ui, data):
                table_details_obj.table_details_for_ui = data
                table_details_obj.is_phi_marking_done = False
                table_details_obj.is_phi_marking_locked = False
                table_details_obj.table_status = TableDeIdntStatus.NOT_STARTED
            table_details_obj.save()
            return Response(
                {"message": "Table details for UI updated successfully", "success": False},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"TablesConfigForUIView.post: Internal server error: {e}, for user : {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

def _is_diff_present_in_dict(dict1, dict2):
    return dict1 != dict2
