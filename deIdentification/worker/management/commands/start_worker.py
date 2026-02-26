import os

import logging
from django.core.management.base import BaseCommand
from worker.worker import TaskWorker

logger = logging.getLogger(__name__)

_CPU_COUNT = os.cpu_count()
if not _CPU_COUNT:
    logger.warning(f"unable to find cpu count. setting cpu count to 8")
    _CPU_COUNT = 8


class Command(BaseCommand):
    """provides options to add args and start the taskworker"""

    help = "Start Workers"
    task_worker = TaskWorker()

    def handle(self, *args, **options):
        """calls teh work function of the Taskworker class"""
        self.task_worker.work()
