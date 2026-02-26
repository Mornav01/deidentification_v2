from django.conf import settings

MAX_FAILURE_COUNT = int(getattr(settings, "MAX_FAILURE_COUNT", 1))

MAX_TASK_PER_WORKER = int(getattr(settings, "MAX_TASK_PER_WORKER", -1))
WORKER_POLL_TIME = int(getattr(settings, "WORKER_POLL_TIME", 2))
SOFT_DELETE_AGE = int(getattr(settings, "SOFT_DELETE_AGE", 24 * 3600))  # in seconds
MACHINE_EXPIRY_OFFSET_TIMEOUT = int(
    getattr(settings, "MACHINE_EXPIRY_OFFSET_TIMEOUT", 60)
)  # in seconds
MACHINE_EXPIRY_TIME = int(getattr(settings, "MACHINE_EXPIRY_TIME", 3600))  # in seconds

MACHINE_ID = getattr(settings, "MACHINE_ID", None)
WORKER_ID = getattr(settings, "WORKER_ID", None)

WORKER_MAKE_SAVEPOINT_IN_TASK_QUERY = getattr(
    settings, "WORKER_MAKE_SAVEPOINT_IN_TASK_QUERY", True
)

CREATE_SAVEPOINT_IN_TRANSACTION = getattr(
    settings, "CREATE_SAVEPOINT_IN_TRANSACTION", True
)
