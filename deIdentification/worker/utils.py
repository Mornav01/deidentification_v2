import math
import random
import string
from json.encoder import (
    INFINITY,
    _make_iterencode,
    encode_basestring,
    encode_basestring_ascii,
)
import requests
from django.utils import timezone
import datetime
from django.core.serializers.json import DjangoJSONEncoder
from .worker_settings import MACHINE_EXPIRY_TIME, MACHINE_ID, WORKER_ID


# need this to override builtin type serialization
class CustomObjectEncoder(DjangoJSONEncoder):
    def iterencode(self, o, _one_shot=False):
        """Encode the given object and yield each string
        representation as available.

        """
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = lambda x: encode_basestring_ascii(x.replace("\x00", ""))
        else:
            _encoder = encode_basestring

        def floatstr(
            o,
            allow_nan=self.allow_nan,
            _repr=float.__repr__,
            _inf=INFINITY,
            _neginf=-INFINITY,
        ):
            # Check for specials.  Note that this type of test is processor
            # and/or platform-specific, so do tests which don't depend on the
            # internals.

            if o != o:
                text = "NaN"
            elif o == _inf:
                text = _repr(1e6)
            elif o == _neginf:
                text = _repr(-1e6)
            else:
                return _repr(o)

            if not allow_nan:
                raise ValueError(
                    "Out of range float values are not JSON compliant: " + repr(o)
                )

            return text

        kwargs = {}
        if self.isinstance:
            kwargs["isinstance"] = self.isinstance
        _iterencode = _make_iterencode(
            markers,
            self.default,
            _encoder,
            self.indent,
            floatstr,
            self.key_separator,
            self.item_separator,
            self.sort_keys,
            self.skipkeys,
            _one_shot,
            **kwargs,
        )
        return _iterencode(o, 0)


class CustomJSONEncoder(CustomObjectEncoder):
    def isinstance(self, obj, cls):
        if isinstance(obj, float) and math.isnan(obj):
            return False
        return isinstance(obj, cls)

    def default(self, o):
        if isinstance(o, bytes):
            return str(o)
        # Nan becomes None post serialize and deserialize in jsonb postgres
        elif isinstance(o, float) and math.isnan(o):
            return None
        else:
            return super().default(o)


def random_string(size=6, chars=string.ascii_lowercase + string.digits):
    return "".join(random.choice(chars) for x in range(size))


def get_machine_id():
    global MACHINE_ID

    if not MACHINE_ID:
        try:
            MACHINE_ID = requests.get(
                url="http://instance-data/latest/meta-data/instance-id"
            ).text
        except Exception:
            pass

    return MACHINE_ID


def get_expiry():
    return timezone.now() + datetime.timedelta(seconds=MACHINE_EXPIRY_TIME)
