import os

import logging
from django.core.exceptions import ValidationError
from django.db import models

from .helper import ComputationStatus

logger = logging.getLogger(__name__)


class SoftDeleteManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(soft_delete=False)


class Chain(models.Model):
    id = models.AutoField(primary_key=True)
    status = models.IntegerField(default=ComputationStatus.NOT_STARTED)
    reference_uuid = models.CharField(max_length=100, null=True, unique=True)
    adjacency_list = models.JSONField(default=dict)
    soft_delete = models.BooleanField(default=False)
    objects = SoftDeleteManager()
    all_objects = models.Manager()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(id={self.id}, reference_uuid={self.reference_uuid}, adjacency_list={self.adjacency_list})"
        )

    def __str__(self):
        return self.__repr__()

    def clean(self, *args, **kwargs):
        """Validator.

        See: https://stackoverflow.com/a/18876223
        """
        if (not self.reference_id) and (not self.reference_uuid):
            raise ValidationError(
                "Populate at least one of reference_id and reference_uuid"
            )

        super().clean(*args, **kwargs)

    @classmethod
    def create_graph(cls, *args, **kwargs):
        dbname = kwargs.pop("dbname", "default")
        return cls.objects.using(dbname).create(*args, **kwargs)

    @classmethod
    def get_or_create_graph(cls, *args, **kwargs):
        dbname = kwargs.pop("dbname", "default")
        return cls.objects.using(dbname).get_or_create(*args, **kwargs)

    @classmethod
    def get_or_create_all_objects_graph(cls, *args, **kwargs):
        dbname = kwargs.pop("dbname", "default")
        return cls.all_objects.using(dbname).get_or_create(*args, **kwargs)

    @classmethod
    def get_graph_from_reference_id(cls, reference_id, **kwargs):
        dbname = kwargs.pop("dbname", "default")
        return cls.objects.using(dbname).get(reference_id=reference_id)

    @classmethod
    def get_graph_from_reference_ids(cls, reference_id, reference_uuid, **kwargs):
        dbname = kwargs.pop("dbname", "default")
        return cls.objects.using(dbname).get(
            reference_id=reference_id, reference_uuid=reference_uuid
        )

    def soft_delete_tasks(self):
        """Soft Deletes the tasks marked to soft delete"""
        for task in self.tasks.all():
            task.soft_delete_and_save()

    def interrupt(self):
        """Interrups the task if it's ComputationStatus is INITIAL
        Moreover it doesn't execute the remaining tasks in the graph
        """
        for task in self.tasks.all():
            if task.status == ComputationStatus.NOT_STARTED:
                task.interrupt()

    @classmethod
    def get_all_ids(cls):
        return list(cls.objects.all().values_list("id", flat=True))

    def get_distinct_task_types(self):
        """Returns the distinct type of tasks"""
        return {t.type for t in self.tasks.all()}

    def soft_delete_and_save(self):
        self.soft_delete_tasks()
        self.soft_delete = True
        self.save()

    def revive_and_save(self):
        self.soft_delete_tasks()
        self.soft_delete = False
        self.save()
