import ast
import select
import sys, os
import time
from typing import Optional
import logging
from django import db
from django.db import transaction
from django.utils import timezone
import datetime
import os


from worker.models import Task, Chain
from worker.utils import get_expiry, get_machine_id
from worker.worker_settings import (
    MACHINE_EXPIRY_OFFSET_TIMEOUT,
    MAX_TASK_PER_WORKER,
    WORKER_POLL_TIME,
    SOFT_DELETE_AGE,
    WORKER_ID,
)
from datetime import timedelta
from deIdentification.nd_logger import nd_logger


_SPLIT_TASK_PICK_AND_EXECUTION_LOCKS = ast.literal_eval(
    os.environ.get("WORKER_SEPARATE_PICK_EXECUTION_LOCKS", "False")
)


class TaskWorker:
    def __init__(
        self,
        max_tasks_to_process=MAX_TASK_PER_WORKER,
        worker_poll_time=WORKER_POLL_TIME,
        back_off_algo=None,
    ):
        self.worker_id = WORKER_ID
        self.num_tasks_processed = 0
        self.max_tasks_to_process = max_tasks_to_process
        self.worker_poll_time = worker_poll_time
        self.notify_channel = "taskchannel"
        self.back_off_algo = back_off_algo
        self.machine_id = get_machine_id()
        nd_logger.info(f"Initialized worker id:{self.worker_id} on {self.machine_id}!")

    def work(self):
        """close all connections inherited from parent process and  django will recreate new connection objects per worker process"""
        # close all connections inherited from parent process
        # django will recreate new connection objects per worker process
        try:
            db.connections.close_all()
        except Exception:
            nd_logger.exception("error while closing connections")

        self._listen()
        while True:
            try:
                self._wait(timeout=self.worker_poll_time)
                self.execute_ready_tasks()
            except db.utils.OperationalError:
                pass

    def _listen(self):
        try:
            with db.connection.cursor() as cur:
                cur.execute('LISTEN "{}";'.format(self.notify_channel))
        except db.utils.OperationalError:
            # not psql likely
            pass

    def _wait(self, timeout):
        try:
            db.connection.connection.poll()
            notifies = db.connection.connection.notifies
            if notifies:
                while notifies:
                    notifies.pop(0)
                return notifies

            select.select([db.connection.connection], [], [], timeout)
            db.connection.connection.poll()
            notifies = db.connection.connection.notifies
            while notifies:
                notifies.pop(0)
            return notifies
        except AttributeError:
            # prob not psql
            # use simple timeout
            time.sleep(timeout)
            return

    def clean_soft_deleted(self):
        creation_time = datetime.datetime.now() - timedelta(seconds=SOFT_DELETE_AGE)
        tasks = Task.all_objects.filter(soft_delete=True, created_at__lte=creation_time)
        batch_size = 10000
        total_batches = int(tasks.count() / batch_size) + 1
        for i in range(total_batches):
            num_deleted, details = Task.all_objects.filter(
                pk__in=Task.all_objects.filter(
                    soft_delete=True, created_at__lte=creation_time
                )
                .order_by("id")
                .values("pk")[:batch_size]
            ).delete()
            if num_deleted > 0:
                nd_logger.info(
                    f"Soft deleted tasks actually deleted. "
                    f"Num deleted = {num_deleted}, details = {details}"
                )

        graphs = Chain.all_objects.filter(
            soft_delete=True, created_at__lte=creation_time
        )
        total_batches = int(graphs.count() / batch_size) + 1
        for i in range(total_batches):
            num_deleted, details = Chain.all_objects.filter(
                pk__in=Chain.all_objects.filter(
                    soft_delete=True, created_at__lte=creation_time
                )
                .order_by("id")
                .values("pk")[:batch_size]
            ).delete()
            if num_deleted > 0:
                nd_logger.info(
                    f"Soft deleted graphs actually deleted. "
                    f"Num deleted = {num_deleted}, details = {details}"
                )

    def _run_task(self, task: Task) -> None:
        # if task is querying the db, it means it is alive, and if expired or about to expire soon update the expiry
        # machine = Machine.objects.get(machine_id=self.machine_id)
        # if machine.expires_at - timezone.now() <= datetime.timedelta(seconds=MACHINE_EXPIRY_OFFSET_TIMEOUT):
        #     machine.expires_at = get_expiry()
        #     machine.save(update_fields=["expires_at"])

        self.num_tasks_processed += 1
        task.run(nd_logger, self.worker_id)

    def execute_ready_tasks(self):
        """Picks ready task from Task and runs the task and if no task found just returns back to caller"""
        nd_logger.debug("Polling for ready tasks")
        while True:
            if self.max_tasks_to_process > 0:
                if self.num_tasks_processed >= self.max_tasks_to_process:
                    nd_logger.info(
                        f"shutting down after processing "
                        f"{self.num_tasks_processed} tasks"
                    )
                    if os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER") == "WORKER":
                        self.clean_soft_deleted()
                    sys.exit(1)

            if _SPLIT_TASK_PICK_AND_EXECUTION_LOCKS:
                # This is thread safe as it runs in a top level transaction
                task: Optional[Task] = Task.get_ready_task(
                    self.back_off_algo, self.machine_id
                )

                if task is None:
                    if os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER") == "POLL":
                        self.clean_soft_deleted()
                    return

                with transaction.atomic():
                    task: Optional[Task] = (
                        Task.objects.select_for_update(skip_locked=True)
                        .filter(id=task.id)
                        .first()
                    )
                    if task:
                        self._run_task(task)
                    else:
                        if (
                            os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER")
                            == "POLL"
                        ):
                            self.clean_soft_deleted()
                        return
            else:
                with transaction.atomic():
                    task = Task.get_ready_task(self.back_off_algo, self.machine_id)
                    if task:
                        self._run_task(task)
                    else:
                        if (
                            os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER")
                            == "POLL"
                        ):
                            self.clean_soft_deleted()
                        return
