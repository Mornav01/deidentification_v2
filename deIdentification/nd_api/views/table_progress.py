import traceback
from typing import TypedDict, Literal
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from nd_api.models import TableDetailsModel, DbDetailsModel, TableDeIdntStatus, TableQCStatus
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


class TableActions:
    MARK_PHI_COMPLETED = "mark_phi_completed"
    MARK_QC_FAILED = "mark_qc_failed"
    MARK_QC_PASSED = "mark_qc_passed"
    MARK_TABLE_LOCKED = "mark_table_locked"


class DbActions:
    PHI_MARKING_LOCK_STATUS = "phi_marking_lock_status"
    MARK_ALL_TABLES_AS_VERIFIED = "mark_all_tables_as_verified"

class TableRequestCtx(TypedDict):
    table_id: int
    action: Literal["mark_phi_completed", "mark_table_locked", "mark_qc_failed", "mark_qc_passed"]
    value: bool

class DbRequestCtx(TypedDict):
    db_id: int
    action: Literal["phi_marking_lock_status", "mark_all_tables_as_verified"]
    value: bool

@conditional_authentication
class UpdateTableProgress(APIView):
    authentication_classes = []

    def update_table_status(
        self, table_details_obj: TableDetailsModel, action: str, value: bool, user=None
    ) -> bool:
        db_details: DbDetailsModel = table_details_obj.db
        message = ""
        updated = False

        if action in [TableActions.MARK_PHI_COMPLETED, TableActions.MARK_TABLE_LOCKED]:
            if db_details.is_phi_marking_locked:
                return (
                    False,
                    "Failed, PHI marking is already locked, cant perform this action",
                )
        if action == TableActions.MARK_TABLE_LOCKED:
            if not table_details_obj.is_phi_marking_done:
                return (
                    False,
                    "Failed, PHI Marking not completed, cant perform this action"
                )
            else:
                message = "Successfully, Locked the table"
                updated = True
                table_details_obj.is_phi_marking_locked = value

        elif action == TableActions.MARK_PHI_COMPLETED:
            table_details_obj.is_phi_marking_done = value
            table_details_obj.is_phi_marking_locked = False
            updated = True
            message = "Successfully registerd phi marking status"

        elif action == TableActions.MARK_QC_FAILED:
            if (
                table_details_obj.is_phi_marking_locked
                and table_details_obj.table_status == TableDeIdntStatus.COMPLETED
            ):
                table_details_obj.qc_status = TableQCStatus.FAILED
                message = "Successfully registerd qc failed status"
            else:
                message = "Failed, Either phi marking is not locked or table de-identificaiton not completed"
        elif action == TableActions.MARK_QC_PASSED:
            if (
                table_details_obj.is_phi_marking_locked
                and table_details_obj.table_status == TableDeIdntStatus.COMPLETED
            ):
                table_details_obj.qc_status = TableQCStatus.COMPLETED
                message = "Successfully registerd qc passed status"
            else:
                message = "Failed, Either phi marking is not locked or table de-identificaiton not completed"
        if updated:
            table_details_obj.save()
        return updated, message

    def post(self, request):
        try:
            data: TableRequestCtx = request.data
            table_id = data["table_id"]
            table_details_obj = TableDetailsModel.objects.get(id=table_id)
            updated, message = self.update_table_status(
                table_details_obj, data["action"], data["value"]
            )

            if updated:
                return Response(
                    {"message": message, "success": True}, status=status.HTTP_200_OK
                )
            else:
                return Response(
                    {"message": message, "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            message = f"UpdateTableProgress.post: Internal server error : {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

@conditional_authentication
class UpdateDbProgress(APIView):
    authentication_classes = []

    def _update_table_lock_status(self, db_details_obj: DbDetailsModel, lock_status: bool):
        total_tables = db_details_obj.tables_details.count()
        table_updater = UpdateTableProgress()
        success_count = 0
        for table in db_details_obj.tables_details.all():
            updated, _ = table_updater.update_table_status(
                table, TableActions.MARK_TABLE_LOCKED, lock_status
            )
            if updated:
                success_count += 1
        if success_count == total_tables:
            db_details_obj.is_phi_marking_locked = lock_status
            db_details_obj.save()
            return Response(
                {"message": "Locked/Unlocked DB successfully"},
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                {
                    "message": f"Partially updated ({success_count}/{total_tables} tables)"
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def post(self, request):
        try:
            data: DbRequestCtx = request.data
            db_id = data["db_id"]
            db_details_obj = DbDetailsModel.objects.get(id=db_id)

            if data["action"] == DbActions.PHI_MARKING_LOCK_STATUS:
                return self._update_table_lock_status(db_details_obj, data["value"])
            else:
                return Response(
                    {"message": "unexpected action type"}, status=status.HTTP_400_BAD_REQUEST
                )
        except Exception as e:
            message = f"UpdateDbProgress.post: Internal server error : {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
