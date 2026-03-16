"""Load and validate config.yaml with env var interpolation."""
from __future__ import annotations

import os
from pathlib import Path
try:
    import re2
except ImportError:
    try:
        import regex as re2  # type: ignore[no-redef]
    except ImportError:
        import re as re2  # type: ignore[no-redef]

import yaml

from deid.config.schema import DeidConfig
from pydantic import validate_call

_ENV_VAR_PATTERN = re2.compile(r"\$\{(\w+)\}")


@validate_call(config=dict(arbitrary_types_allowed=True))
def _interpolate_env_vars(obj):
    """Recursively replace ${VAR_NAME} with os.environ[VAR_NAME]."""
    if isinstance(obj, str):
        @validate_call(config=dict(arbitrary_types_allowed=True))
        def _replacer(match):
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"Environment variable '{var}' not set (referenced in config)")
            return val
        return _ENV_VAR_PATTERN.sub(_replacer, obj)
    elif isinstance(obj, dict):
        return {k: _interpolate_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_interpolate_env_vars(item) for item in obj]
    return obj


@validate_call(config=dict(arbitrary_types_allowed=True))
def load_config(path: str | Path) -> DeidConfig:
    """Load config from YAML file, interpolate env vars, validate with Pydantic."""
    path = Path(path)
    with open(path) as f:
        raw = yaml.safe_load(f)
    interpolated = _interpolate_env_vars(raw)
    return DeidConfig(**interpolated)
