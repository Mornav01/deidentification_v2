import traceback
from nd_api.models import  DataDump, RestoreDump
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.migratedb.migrate import restore_database


class DumpRestoreView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        try:
            all_dumps = RestoreDump.objects.all()
            all_dumps_response = []
            for dumpdb in all_dumps:
                all_dumps_response.append(
                    {
                        "id": dumpdb.id,
                        "status": dumpdb.status,
                        "config": dumpdb.config
                    }
                )
            return Response(all_dumps_response, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def post(self, request):
        try:
            data = request.data
            datadump_obj = DataDump.objects.get(id=data['dump_id'])
            config = {
                "db_type": data['config']["db_type"],
                "user": data['config']["user"],
                "password": data['config']["password"],
                "host": data['config']["host"],
                "port": data['config']["port"],
                "database": data['config']["database"],
            }
            restore_obj = RestoreDump.objects.create(
                dump=datadump_obj,
                config=config
            )
            return Response({"restore_obj_id": restore_obj.id}, status=status.HTTP_200_OK)
        except KeyError as e:
            return Response(f"Invalid data, provide required informatinos:", status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class StartDumpRestoreView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, restore_dump_id: int):
        try:
            restore_dump_obj = RestoreDump.objects.get(id=restore_dump_id)
            dump_task = restore_database(restore_dump_obj)
            return Response(f"generated dump tasks: {len(dump_task)}", status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
