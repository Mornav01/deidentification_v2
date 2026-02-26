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
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class StopDeIdentificationView(APIView):
    authentication_classes = []

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
    authentication_classes = []

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
    """Create a de-identification task chain for *table_obj*.

    The old design created one task per batch (keyset or offset-based), which
    required knowing rows_count upfront and produced tens of thousands of tasks
    for large tables (e.g. 72 K tasks for 52 tables).

    The new design creates exactly **one processing task per table**.  The task
    itself streams the source table in batches internally using a server-side
    MySQL cursor, so no row count or pagination ranges are needed at this stage.
    Task count: 1 processing task + 1 cleanup task = 2 per table.
    """
    chain, created = Chain.all_objects.get_or_create(
        reference_uuid=table_obj.get_chain_reference_uuid()
    )
    if not created:
        chain.revive_and_save()

    tables_config = table_obj.table_details_for_ui
    batch_size = settings.BATCH_SIZE_DURING_DE_IDENTIFICATION
    table_obj.marked_as_in_progress_if_required()

    dest_connection: NDDBHandler = table_obj.db.get_destination_db_connection()
    try:
        if delete_table:
            dest_connection.drop_table(table_obj.table_name)
            ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(
                db_name=table_obj.db.db_name, table_name=table_obj.table_name
            )
            nd_logger.info(f"Dropping ignore rows for {table_obj.table_name}, {table_obj.db.db_name}")
            ignore_rows.delete()
    finally:
        dest_connection.close()

    # Single task — streaming handles all batching internally.
    process_task = Task.create_task(
        fn=start_de_identification_for_table,
        chain=chain,
        arguments={
            "table_id": table_obj.id,
            "batch_size": batch_size,
            "table_config": tables_config,
        },
        hooks={"failure": de_identification_failure_hook_for_table},
    )

    cleanup_task = Task.create_task(
        fn=marked_complete_and_clean_up_tasks,
        chain=chain,
        dependencies=[process_task],
        arguments={"table_id": table_obj.id, "chain_id": chain.id},
    )

    return [process_task, cleanup_task], chain

def create_deidentification_tasks_parallel(
    table_obj: TableDetailsModel,
    delete_table: bool = True,
    num_parallel: int = 4,
    id_column: str = "nd_auto_increment_id",
    large_table_threshold: int = 5_000_000,
):
    """Create *num_parallel* tasks that process disjoint ID ranges of one table.

    **No row count or stats generation required.**

    Decision logic (two cheap index lookups):
    1. Query MIN(id_column) and MAX(id_column) from the source table.
    2. If (max_id - min_id + 1) <= large_table_threshold  →  table is small;
       fall back to a single streaming task automatically.
    3. Otherwise split [min_id, max_id] into *num_parallel* equal ID-range
       buckets and create one task per bucket.

    A ``cleanup_task`` that depends on all process tasks fires only after every
    worker has finished its slice, then marks the table as completed.

    Example — 10 M-row table, num_parallel=4, batch_size=100 K:
        Worker 1 → id  1        …  2 500 000   (25 batches)
        Worker 2 → id  2 500 001 …  5 000 000  (25 batches)
        Worker 3 → id  5 000 001 …  7 500 000  (25 batches)
        Worker 4 → id  7 500 001 … 10 000 000  (25 batches)
    Wall-clock time ≈ (single-task time) / num_parallel.

    Falls back to a single streaming task when:
    - The table has no id_column / MIN/MAX lookup fails.
    - The ID range is smaller than *large_table_threshold* (small table).
    - Only one bucket would be created.
    """
    chain, created = Chain.all_objects.get_or_create(
        reference_uuid=table_obj.get_chain_reference_uuid()
    )
    if not created:
        chain.revive_and_save()

    tables_config = table_obj.table_details_for_ui
    batch_size = settings.BATCH_SIZE_DURING_DE_IDENTIFICATION
    table_obj.marked_as_in_progress_if_required()

    # Drop destination table and ignored-rows records (once, before any task runs).
    dest_connection: NDDBHandler = table_obj.db.get_destination_db_connection()
    try:
        if delete_table:
            dest_connection.drop_table(table_obj.table_name)
            ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(
                db_name=table_obj.db.db_name, table_name=table_obj.table_name
            )
            nd_logger.info(
                f"Dropping ignore rows for {table_obj.table_name}, {table_obj.db.db_name}"
            )
            ignore_rows.delete()
    finally:
        dest_connection.close()

    # -----------------------------------------------------------------------
    # Two cheap index lookups → no stats / row-count needed.
    # -----------------------------------------------------------------------
    source_connection: NDDBHandler = table_obj.db.get_source_db_connection()
    min_id: int | None = None
    max_id: int | None = None
    try:
        from sqlalchemy import text as sa_text
        with source_connection.engine.connect() as conn:
            row = conn.execute(
                sa_text(
                    f"SELECT MIN(`{id_column}`), MAX(`{id_column}`) "
                    f"FROM `{table_obj.table_name}` "
                    f"WHERE `{id_column}` IS NOT NULL"
                )
            ).fetchone()
            if row and row[0] is not None:
                min_id, max_id = int(row[0]), int(row[1])
    except Exception as exc:
        nd_logger.warning(
            f"[parallel tasks] MIN/MAX query failed for '{table_obj.table_name}': {exc}. "
            "Falling back to single streaming task."
        )
    finally:
        source_connection.close()

    id_range_size = (max_id - min_id + 1) if (min_id is not None and max_id is not None) else 0

    if id_range_size <= large_table_threshold:
        # Small table or empty — single streaming task is fine.
        nd_logger.info(
            f"[parallel tasks] '{table_obj.table_name}': "
            f"ID range {id_range_size:,} ≤ threshold {large_table_threshold:,}. "
            "Creating a single streaming task."
        )
        return create_deidentification_task(table_obj, delete_table=False)

    # -----------------------------------------------------------------------
    # Split [min_id, max_id] into exactly num_parallel equal buckets.
    # -----------------------------------------------------------------------
    chunk = id_range_size // num_parallel
    buckets: list[tuple[int, int]] = []
    for i in range(num_parallel):
        start = min_id + i * chunk
        end = (min_id + (i + 1) * chunk - 1) if i < num_parallel - 1 else max_id
        if start <= end:
            buckets.append((start, end))

    if len(buckets) <= 1:
        nd_logger.info(
            f"[parallel tasks] '{table_obj.table_name}': only one bucket — "
            "single streaming task."
        )
        return create_deidentification_task(table_obj, delete_table=False)

    nd_logger.info(
        f"[parallel tasks] '{table_obj.table_name}': "
        f"ID range {min_id:,}–{max_id:,} → {len(buckets)} buckets"
    )

    process_tasks = []
    for start_id, end_id in buckets:
        task = Task.create_task(
            fn=start_de_identification_for_table,
            chain=chain,
            arguments={
                "table_id": table_obj.id,
                "batch_size": batch_size,
                "table_config": tables_config,
                "start_id": start_id,
                "end_id": end_id,
                "id_column": id_column,
            },
            hooks={"failure": de_identification_failure_hook_for_table},
        )
        process_tasks.append(task)

    # Cleanup runs only after ALL process tasks complete.
    cleanup_task = Task.create_task(
        fn=marked_complete_and_clean_up_tasks,
        chain=chain,
        dependencies=process_tasks,
        arguments={"table_id": table_obj.id, "chain_id": chain.id},
    )

    return process_tasks + [cleanup_task], chain


def marked_complete_and_clean_up_tasks(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_details_obj = TableDetailsModel.objects.get(id=table_id)
    table_details_obj.marked_as_completed()
    chain = Chain.objects.get(id=chain_id)
    with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
        chain.soft_delete_and_save()
