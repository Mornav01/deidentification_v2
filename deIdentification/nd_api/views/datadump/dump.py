import traceback
from nd_api.models import DbDetailsModel, TableDetailsModel, DataDump
from nd_api.models import DbStatsGeneratedStatus
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from deIdentification.nd_logger import nd_logger
from nd_api.migratedb.migrate import dump_database
from nd_api.decorator import conditional_authentication


@conditional_authentication
class DumpDataView(APIView):
    authentication_classes = []

    def get(self, request):
        try:
            all_dumps = DataDump.objects.all()
            all_dumps_response = []
            for dumpdb in all_dumps:
                all_dumps_response.append(
                    {
                        "id": dumpdb.id,
                        "status": dumpdb.status,
                        "dump_name": dumpdb.dump_name,
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
            try:
                data = request.data
                datadump_obj = DataDump.objects.create(
                    dump_name=data["dump_name"],
                    config={
                        "db_type": data['config']["db_type"],
                        "user": data['config']["user"],
                        "password": data['config']["password"],
                        "host": data['config']["host"],
                        "port": data['config']["port"],
                        "db_names": data['config']["db_names"],
                        "start_time": data['config']["start_time"],
                        "end_time": data['config']["end_time"],
                    },
                )
                return Response({"datadump_id": datadump_obj.id}, status=status.HTTP_200_OK)
            except KeyError as e:
                return Response(f"Invalid data, provide required informatinos:", status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class StartDumpView(APIView):
    authentication_classes = []

    def get(self, request, dump_id: int):
        try:
            dump_obj = DataDump.objects.get(id=dump_id)
            dump_task = dump_database(dump_obj)
            return Response(f"generated dump tasks: {len(dump_task)}", status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    