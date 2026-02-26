from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from nd_api.decorator import conditional_authentication

# All permissions are granted — Keycloak auth has been removed.
ALL_PERMISSIONS = {
    "AddDataBase":                  {"has_permission": True},
    "UploadPHIConfigCSV":           {"has_permission": True},
    "PHIMarkingCompletedTick":      {"has_permission": True},
    "TableQCTick":                  {"has_permission": True},
    "UnLockPHIMarkingDB":           {"has_permission": True},
    "LockPHIMarkingDB":             {"has_permission": True},
    "LockPHIMarkingTable":          {"has_permission": True},
    "UnLockPHIMarkingTable":        {"has_permission": True},
    "StartDeIdentificationButton":  {"has_permission": True},
}


@conditional_authentication
class UserPermissions(APIView):
    authentication_classes = []

    def get(self, request):
        return Response(ALL_PERMISSIONS, status=status.HTTP_200_OK)
