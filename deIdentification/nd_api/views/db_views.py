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

def process_table(table, db_details_obj: DbDetailsModel, fks_to_map: dict, fks_from_map: dict, rerun: bool = False):
    # Each thread gets its own connection — do NOT share NDDBHandler across threads.
    source_db_connection = db_details_obj.get_source_db_connection()
    try:
        table_details_obj, created = TableDetailsModel.register_table(table, db_details_obj)
        if not created:
            table_stats = {
                "rows_count": table_details_obj.rows_count,
                "size": table_details_obj.size,
                "fks_to": fks_to_map.get(table, []),
                "fks_from": fks_from_map.get(table, []),
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
            # FK maps were pre-computed once before the thread pool — no per-table DB scan.
            "fks_to": fks_to_map.get(table, []),
            "fks_from": fks_from_map.get(table, []),
        }
        return table, table_stats, table_details_obj.rows_count
    finally:
        source_db_connection.close()

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
        db_stats["tables_stats"] = {}

        # Pre-compute the entire FK graph in one O(N) pass before spawning threads.
        # Previously each process_table() call did an O(N) scan = O(N²) total.
        nd_logger.info("Pre-computing FK map for all tables...")
        fks_to_map, fks_from_map = source_db_connection.get_all_fks_map()
        nd_logger.info(f"FK map ready. Starting parallel stats for {len(all_tables)} tables...")
    finally:
        source_db_connection.close()

    with ThreadPoolExecutor(max_workers=settings.STATS_GENERATION_MAX_WORKER_COUNT) as executor:
        future_to_table = {
            executor.submit(process_table, table, db_details_obj, fks_to_map, fks_from_map, rerun): table
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


def _register_single_table(table_name: str, db_details_obj: DbDetailsModel):
    """Lightweight registration for one table — column schema only, no row counts.

    Called in parallel by register_tables_for_db().  Each thread opens its own
    connection so they don't share state.
    """
    source_db_connection = db_details_obj.get_source_db_connection()
    try:
        table_obj, created = TableDetailsModel.register_table(table_name, db_details_obj)
        if created or not table_obj.table_details_for_ui.get("columns_details"):
            # First time we see this table — populate the column schema so
            # UploadConfigFromCSV can match CSV column names to it.
            table_obj.table_details_for_ui = _get_default_table_details_for_ui(
                source_db_connection.get_column_names(table_name)
            )
            table_obj.save()
        return table_name, created
    finally:
        source_db_connection.close()


def register_tables_for_db(
    db_details_id: int,
    all_tables: list[str] | None = None,
    max_workers: int | None = None,
):
    """Register (or refresh) tables for a DB — **fast, no stats required**.

    This is the lightweight alternative to run_stats_generation_task that you
    must call once before using upload_config_from_csv or run.ipynb.

    What it does for each table (in parallel):
    - Creates a TableDetailsModel row if it doesn't exist yet.
    - Populates table_details_for_ui with the column schema from the source DB
      (needed by UploadConfigFromCSV to match CSV column names).

    What it deliberately skips (all slow):
    - COUNT(*) / information_schema row-count estimation
    - Table size queries
    - Foreign-key graph scans

    Args:
        db_details_id : id of the DbDetailsModel to register tables for.
        all_tables    : list of table names to register.  If None, all tables
                        in the source DB are discovered automatically.
        max_workers   : thread-pool size.  Defaults to
                        settings.STATS_GENERATION_MAX_WORKER_COUNT.
    """
    db_details_obj = DbDetailsModel.objects.get(id=db_details_id)
    if max_workers is None:
        max_workers = settings.STATS_GENERATION_MAX_WORKER_COUNT

    if all_tables is None:
        source_conn = db_details_obj.get_source_db_connection()
        try:
            all_tables = source_conn.get_all_tables()
        finally:
            source_conn.close()

    nd_logger.info(
        f"[register_tables] Registering {len(all_tables)} tables for db_id={db_details_id} "
        f"using {max_workers} worker(s)…"
    )

    results = {"registered": [], "already_existed": [], "failed": []}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(_register_single_table, table, db_details_obj): table
            for table in all_tables
        }
        for future in tqdm(
            as_completed(future_to_table),
            total=len(all_tables),
            desc="Registering tables",
        ):
            table = future_to_table[future]
            try:
                _, created = future.result()
                if created:
                    results["registered"].append(table)
                else:
                    results["already_existed"].append(table)
            except Exception as exc:
                nd_logger.error(f"[register_tables] Failed to register '{table}': {exc}")
                results["failed"].append(table)

    nd_logger.info(
        f"[register_tables] Done. "
        f"new={len(results['registered'])}, "
        f"existing={len(results['already_existed'])}, "
        f"failed={len(results['failed'])}"
    )
    return results


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
