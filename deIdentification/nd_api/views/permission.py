from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.rolemodel import RoleModel
from keycloakauth.utils import IsAuthenticated
from keycloakauth.rolemodel import get_default_permissions
from nd_api.decorator import conditional_authentication


@conditional_authentication
class UserPermissions(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):

        all_permissions = RoleModel.get_permissions_for_user(user=request.user)
        return Response(all_permissions, status=status.HTTP_200_OK)