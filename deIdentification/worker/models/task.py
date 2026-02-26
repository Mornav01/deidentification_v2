import datetime
import importlib
import traceback
import types
import os
import ast
import contextlib
import io
from enum import Enum
from typing import Optional, Union


from django.core.exceptions import ValidationError
from django.db import models, transaction, InterfaceError, OperationalError, connections
from django.db.models import Q
from django.db.models import F
from django.utils import timezone
from worker import serialization

from .chain import Chain
from .helper import ComputationStatus
from deIdentification.nd_logger import nd_logger


def default_back_off(failure_count):
    # using exponential back off
    return min(300, 4**failure_count)


def function_to_task_type(fn):
    if fn.__name__ == "<lambda>":
        raise ValueError(f"{fn} is a lambda.")

    if isinstance(fn, types.FunctionType):
        return fn.__module__ + "." + fn.__name__
    elif isinstance(fn, types.MethodType) and isinstance(fn.__self__, type):
        # only class methods are supported
        cls = fn.__self__
        return cls.__module__ + "." + cls.__name__ + ":" + fn.__name__

    raise ValueError(f"{fn} is not supported.")


def task_type_to_function(task_type):
    module_name, fn_name = task_type.rsplit(".", 1)
    module = importlib.import_module(module_name)
    if ":" in fn_name:
        cls_name, method_name = fn_name.split(":")
        cls = getattr(module, cls_name)
        return getattr(cls, method_name)
    else:
        return getattr(module, fn_name)


class SoftDeleteManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(soft_delete=False)


class TryAgainLater(Exception):
    pass


class CriticalFailure(Exception):
    """New custom exception, specifically to fail tasks without retrying again"""

    pass


class Task(models.Model):
    id = models.AutoField(primary_key=True)
    chain = models.ForeignKey(
        Chain, related_name="tasks", on_delete=models.CASCADE, blank=True
    )
    type = models.CharField(max_length=100, db_index=True)
    arguments = models.JSONField(default=dict)
    remarks = models.JSONField(default=dict)
    status = models.IntegerField(db_index=True, default=ComputationStatus.NOT_STARTED)
    num_dependencies_total = models.IntegerField(default=0)
    num_dependencies_pending = models.IntegerField(db_index=True, default=0)
    timeout = models.IntegerField(default=60)
    started_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True, db_index=True)
    back_off = models.DateTimeField(auto_now_add=True, db_index=True)
    failure_count = models.IntegerField(default=0)
    soft_delete = models.BooleanField(default=False, db_index=True)
    repeat = models.BooleanField(default=False)
    repeat_period = models.IntegerField(default=10, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(db_index=True, auto_now=True)
    runtime = models.IntegerField(default=-1, null=True)
    processed_at = models.DateTimeField(blank=True, null=True)
    ready_at = models.DateTimeField(blank=True, null=True)
    max_failure_count = models.IntegerField(default=5)
    objects = SoftDeleteManager()
    all_objects = models.Manager()

    back_off_algo = models.TextField(default="")

    class Meta:
        pass

    def downstream(self):
        task_id = str(self.id)
        downstream_tasks_ids = map(int, self.chain.adjacency_list.get(task_id, []))
        return Task.objects.filter(id__in=downstream_tasks_ids)

    def update_downstream(self):
        """Decrement num_dependencies_pending by one for dependent tasks."""
        return (
            self.downstream()
            .filter(num_dependencies_pending__gt=0)
            .update(num_dependencies_pending=models.F("num_dependencies_pending") - 1)
        )

    def update_task_ready_time(self):
        """update ready_at value if num_dependencies_pending is zero"""
        return (
            self.downstream()
            .filter(num_dependencies_pending=0)
            .update(ready_at=timezone.now())
        )

    def upstream(self):
        """Get list of tasks current task is depending on.

        Returns:
            QuerySet: List of dependencies.
        """
        upstream_task_ids = self.arguments.get("dependencies", [])
        # https://stackoverflow.com/a/38390480
        preserved = models.Case(
            *[models.When(id=id, then=pos) for pos, id in enumerate(upstream_task_ids)]
        )
        return Task.objects.filter(id__in=upstream_task_ids).order_by(preserved)

    def clean(self, *args, **kwargs):
        current_task_id = str(self.id)
        adjacency_list = self.chain.adjacency_list
        upstream_task_ids_adjacency = [
            int(task_id)
            for task_id, dependencies in adjacency_list.items()
            if current_task_id in dependencies
        ]
        upstream_task_ids = self.arguments.get("dependencies", [])
        if set(upstream_task_ids_adjacency) != set(upstream_task_ids):
            raise ValidationError(
                "Something wrong with adjacency_list. circular dependency?"
            )
        super().clean(*args, **kwargs)

    def _success(self, remarks):
        """Indicate that task has executed successfully."""
        self.status = ComputationStatus.COMPLETED
        flattened_results = serialization.flatten(remarks)
        self.remarks.update(remarks=flattened_results)
        self.processed_at = timezone.now()
        self.save()
        self.update_downstream()
        self.update_task_ready_time()

    def _failure(self, exception=None):
        """Indicate that task has failed.

        Args:
            exception (Exception, optional): exception to be stored.
        """
        self.failure_count = self.failure_count + 1
        if not exception:
            exception = RuntimeError(f"{self} failed {self.failure_count} times.")
        if self.failure_count >= self.max_failure_count:
            self.status = ComputationStatus.FAILURE
            try:
                failure_hook = self.arguments["hooks"]["failure"]
                nd_logger.info(f"running failure hook {failure_hook}")
                task_type_to_function(failure_hook)(self.chain)
            except KeyError:
                pass
            except Exception:
                nd_logger.exception("failure hook failed")
        else:
            self.status = ComputationStatus.NOT_STARTED

        self.remarks.update(
            {
                "exception": {
                    "message": str(exception),
                    "type": type(exception).__name__,
                    "traceback": traceback.format_tb(exception.__traceback__),
                }
            }
        )

        # Use atomic transaction to ensure the save operation succeeds
        # even if we're already in a transaction that encountered an error
        try:
            with transaction.atomic():
                self.save()
        except Exception as e:
            nd_logger.exception(f"Failed to save task {self.id} after failure: {e}")
            # If we can't save, at least log the failure
            nd_logger.error(f"Task {self.id} failed but could not be saved to database")

    def _critical_failure(self, exception):
        """Indicates that the task has failed permanently. No need to retry.

        Args:
            exception (Exception): exception to be stored.
        """
        self.failure_count += 1
        self.status = ComputationStatus.FAILURE
        try:
            failure_hook = self.arguments["hooks"]["failure"]
            nd_logger.info(f"running failure hook {failure_hook}")
            task_type_to_function(failure_hook)(self.chain)
        except KeyError:
            pass

        self.remarks["success"] = False
        exception_args = exception.args
        if len(exception_args) == 1:
            exception_value = exception_args[0]
            if isinstance(exception_value, dict):
                self.remarks.update(exception_value)
            elif isinstance(exception_value, str):
                self.remarks["message"] = str(exception_value)
            else:
                self.remarks["details"] = str(exception_value)
        else:
            self.remarks["details"] = exception_args

        # Use atomic transaction to ensure the save operation succeeds
        # even if we're already in a transaction that encountered an error
        try:
            with transaction.atomic():
                self.save()
        except Exception as e:
            nd_logger.exception(f"Failed to save task {self.id} after critical failure: {e}")
            # If we can't save, at least log the failure
            nd_logger.error(f"Task {self.id} critically failed but could not be saved to database")

    def interrupt(self):
        """Sets the status of the task as INTERRUPTED"""
        self.status = ComputationStatus.INTERRUPTED
        self.save()

    def crashed_previously(self):
        """Returns if the task is previously crashed"""
        if self.status == ComputationStatus.PROCESSING:
            if self.expires_at < timezone.now():
                return True
        return False

    def fetch_results(self):
        """Get result of a successfully run task."""
        if self.status != ComputationStatus.COMPLETED:
            raise ValueError(f"{self} is not successfully run.")
        flattened_result = self.remarks["remarks"]
        remarks = serialization.restore(flattened_result)
        return remarks

    def soft_delete_and_save(self):
        self.soft_delete = True
        self.save()

    @classmethod
    def create_task(
        cls,
        fn,
        arguments={},
        dependencies=[],
        hooks={},
        **kwargs,
    ) -> Union[list["Task"], "Task"]:
        task_type, flattened_arguments, already_processed_tasks = (
            cls._preprocess_create_task_params(fn, arguments, dependencies, kwargs)
        )
        task = cls._create_task_for_machine(
            task_type,
            flattened_arguments,
            hooks,
            already_processed_tasks,
            dependencies,
            **kwargs,
        )
        return task

    @classmethod
    def _create_task_for_machine(
        cls,
        task_type,
        flattened_arguments,
        hooks,
        already_processed_tasks,
        dependencies,
        **kwargs,
    ):
        task = cls.objects.create(
            type=task_type,
            arguments=flattened_arguments,
            **kwargs,
        )

        if "max_failure_count" in kwargs:
            task.max_failure_count = kwargs["max_failure_count"]

        if "back_off_algo" in kwargs:
            task.back_off_algo = kwargs.get("back_off_algo")

        if hooks:
            task.arguments["hooks"] = {
                k: function_to_task_type(v) for k, v in hooks.items()
            }

        if dependencies:
            task.arguments["dependencies"] = [x.id for x in dependencies]
            pending_dependencies = Task.objects.filter(
                id__in=[x.id for x in dependencies],
                status__in=[
                    ComputationStatus.NOT_STARTED,
                    ComputationStatus.PROCESSING,
                    ComputationStatus.FAILURE,
                ],
            )

            task.num_dependencies_total = len(dependencies)
            task.num_dependencies_pending = len(pending_dependencies) - len(
                already_processed_tasks
            )
            adjacency_list = task.chain.adjacency_list
            for dependency in dependencies:
                dependency_id = str(dependency.id)
                task_id = str(task.id)
                adjacency_list[dependency_id] = adjacency_list.get(dependency_id, [])
                adjacency_list[dependency_id].append(task_id)

            task.chain.adjacency_list = adjacency_list

        if (task.num_dependencies_pending == 0) and (
            task.status == ComputationStatus.NOT_STARTED
        ):
            task.ready_at = timezone.now()

        task.clean()
        task.chain.save()
        task.save()
        return task

    @classmethod
    def _preprocess_create_task_params(cls, fn, arguments, dependencies, kwargs):
        task_type = function_to_task_type(fn)
        flattened_arguments = serialization.flatten(arguments)
        already_processed_tasks = kwargs.get("already_processed", set())
        if dependencies:
            already_processed_tasks.intersection_update(dependencies)
        kwargs.pop("already_processed", None)
        return task_type, flattened_arguments, already_processed_tasks

    def set_backoff_and_activate(self, back_off_in_seconds):
        self.status = ComputationStatus.NOT_STARTED
        self.back_off = datetime.datetime.now() + datetime.timedelta(
            seconds=back_off_in_seconds
        )
        self.save()

    def run(self, run_logger=None, worker_id=None):
        run_logger = run_logger or nd_logger
        self.worker_id = worker_id

        if self.num_dependencies_pending > 0:
            raise ValueError(f"Dependencies for {self} are not run yet")

        if self.status == ComputationStatus.COMPLETED:
            return self.fetch_results()

        kwargs = serialization.restore(self.arguments)
        fn = task_type_to_function(self.type)
        # TODO: this is not generic. Make it generic.
        dependencies = self.upstream()
        if dependencies:
            kwargs["dependencies"] = [x.fetch_results() for x in dependencies]
        kwargs.pop("hooks", None)
        self.status = ComputationStatus.PROCESSING
        self.save()
        task_output = None
        try:
            now = timezone.now()
            # Some environments have broken stdout; capture all prints from the
            # task function into an in-memory buffer to avoid OSError: [Errno 5].
            stdout_buffer = io.StringIO()
            with contextlib.redirect_stdout(stdout_buffer):
                task_output = fn(**kwargs)
            run_time = (timezone.now() - now).total_seconds() * 1000
            self.runtime = run_time
            if self.repeat:
                self._schedule_next()
            else:
                self._success(task_output)
            run_logger.info(f"Task successful: {self} ran on {self.worker_id}")
        except CriticalFailure as e:
            run_logger.info(f"Task failed critically: {self}")
            self._critical_failure(e)
        except TryAgainLater:
            run_logger.info(f"Task asked it to be tried again: {self}")
            self._schedule_next()
        except (InterfaceError, OperationalError) as e:
            run_logger.error(f"Database connection error {e}, retrying...")
            connections.close_all()
            self._schedule_next()
        except RuntimeError as e:
            if self.repeat:
                self._schedule_next()
            else:
                self._failure(e)
            run_logger.exception(f"Task failed: {self}")

        except Exception as e:
            if self.repeat:
                self._schedule_next()
            else:
                self._failure(e)
            run_logger.exception(f"Task failed: {self}")

        return task_output

    def _schedule_next(self):
        self.status = ComputationStatus.NOT_STARTED
        self.back_off = timezone.now() + datetime.timedelta(seconds=self.repeat_period)
        self.save()

    def prep(self, back_off_algo=None):
        if self.crashed_previously():
            nd_logger.warning(
                "detected previous crash for task: id=%s type=%s", self.id, self.type
            )
            self.failure_count += 1
        now = timezone.now()
        self.status = ComputationStatus.PROCESSING
        if not back_off_algo or type(back_off_algo) is str:
            if self.back_off_algo == "":
                back_off_algo = default_back_off
            else:
                try:
                    back_off_algo = eval(self.back_off_algo)
                    if not (
                        callable(back_off_algo) and back_off_algo.__name__ == "<lambda>"
                    ):
                        raise TypeError("Not a lambda function")
                except NameError:
                    try:
                        back_off_algo = task_type_to_function(self.back_off_algo)
                    except:
                        back_off_algo = default_back_off
                except Exception:
                    back_off_algo = default_back_off

        self.back_off = now + datetime.timedelta(
            seconds=back_off_algo(self.failure_count)
        )

        self.started_at = now
        self.expires_at = now + datetime.timedelta(seconds=self.timeout)
        self.save()
        self.refresh_from_db()

    @classmethod
    def _sort(cls, tasks):
        return tasks.order_by("failure_count", "updated_at")

    @classmethod
    def _get_query_prefix_for_index_performance(cls):
        return Q(soft_delete=False) & Q(num_dependencies_pending=0)

    @classmethod
    def _get_ready_task(cls, machine_id=None):
        # Filter task that has zero dependencies and has not failed max allowable times so far and is not expired

        performant_filter = cls._get_query_prefix_for_index_performance()
        _ready_tasks = (
            cls.objects.filter(
                (performant_filter & Q(status=ComputationStatus.NOT_STARTED))
                | (
                    performant_filter
                    & Q(status=ComputationStatus.PROCESSING)
                    & Q(expires_at__lt=timezone.now())
                )
            )
            # .filter(Q(failure_count__lt=F("max_failure_count")))
            .filter(Q(failure_count__lt=1))
            .filter(back_off__lt=timezone.now())
        )

        custom_query_enabled = ast.literal_eval(
            os.getenv("WORKERS_CUSTOM_QUERY_ENABLED", "False")
        )
        custom_query_include_dict = ast.literal_eval(
            os.getenv("WORKERS_CUSTOM_QUERY_INCLUDE_DICT", "{}")
        )
        custom_query_exclude_dict = ast.literal_eval(
            os.getenv("WORKERS_CUSTOM_QUERY_EXCLUDE_DICT", "{}")
        )

        if custom_query_enabled:
            if custom_query_include_dict:
                _ready_tasks = _ready_tasks.filter(**custom_query_include_dict)
            if custom_query_exclude_dict:
                _ready_tasks = _ready_tasks.exclude(**custom_query_exclude_dict)

        task = cls._sort(_ready_tasks).select_for_update(skip_locked=True).first()
        if task and task.failure_count == 0:
            task.save()
        return task

    @classmethod
    def get_ready_task(cls, back_off_algo=None, machine_id=None) -> Optional["Task"]:
        """
        Fetches a task ready for execution. This is threadsafe if there are no external atomic transations
        Args:
            back_off_algo: back off mechanism

        Returns:

        """
        with transaction.atomic(savepoint=True):
            task = cls._get_ready_task(machine_id)
            if not task:
                nd_logger.info("Found no task to pick up")
                return
            nd_logger.info("Task picked up: id=%s, type=%s", task.id, task.type)
            task.prep(back_off_algo)
            return task

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, type={self.type}, "
            f"arguments=({str(self.arguments):.20}...), "
            f"status={self.status}, "
            f"num_dependencies={self.num_dependencies_total}, "
            f"num_dependencies_pending={self.num_dependencies_pending}, "
            f"failure_count={self.failure_count})"
        )

    def __str__(self):
        return self.__repr__()

    def get_exception(self):
        if "exception" in self.remarks:
            return self.remarks["exception"]["message"]
        return None
