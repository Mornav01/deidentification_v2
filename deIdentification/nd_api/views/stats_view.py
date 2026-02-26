import traceback
from nd_api.models import DbDetailsModel, TableDetailsModel
from nd_api.models import DbStatsGeneratedStatus
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class DbStatsView(APIView):
    authentication_classes = []

    def get(self, request, db_id: int):
        try:
            db_details_obj = DbDetailsModel.objects.get(id=db_id)
            db_stats = {}
            if db_details_obj.stats_generated_status == DbStatsGeneratedStatus.COMPLETED:
                db_stats = db_details_obj.db_stats
                db_stats["db_name"] = db_details_obj.db_name
                db_stats["db_id"] = db_details_obj.id

                for table_obj in db_details_obj.tables_details.all():
                    table_obj: TableDetailsModel = table_obj
                    db_stats["tables_stats"][table_obj.table_name].update(
                        {
                            "table_id": table_obj.id,
                            "table_name": table_obj.table_name,
                            "table_status": table_obj.table_status,
                            "failure_remarks": table_obj.failure_remarks,
                        }
                    )
            elif db_details_obj.stats_generated_status == DbStatsGeneratedStatus.FAILED:
                db_stats = db_details_obj.db_stats
                db_stats["failure_remarks"] = db_details_obj.failure_remarks
            elif db_details_obj.stats_generated_status == DbStatsGeneratedStatus.IN_PROGRESS:
                db_stats = {
                    "status": "In Progress",
                }
            else:
                db_stats = {
                    "status": "Not Started",
                }
            return Response(db_stats, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

