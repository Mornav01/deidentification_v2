from django.conf import settings
from rest_framework.permissions import AllowAny

def conditional_authentication(cls):
    """
    Decorator to conditionally disable authentication based on the DISABLE_AUTHENTICATION flag.
    """
    if getattr(settings, "DISABLE_AUTHENTICATION", False):
        cls.authentication_classes = []
        cls.permission_classes = [AllowAny]
    return cls
