import os
import django


def setup_django():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
    django.setup()
