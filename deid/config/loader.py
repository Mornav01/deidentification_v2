"""Load and validate config.yaml with env var interpolation."""
from __future__ import annotations

import os
from pathlib import Path
try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already available

import yaml

from deid.config.schema import DeidConfig
from pydantic import validate_call

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


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


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge overlay into base. Overlay values win on conflict.

    Lists are replaced entirely (not appended) — this matches the semantics
    of "the task config overrides the base config".
    """
    merged = dict(base)
    for key, value in overlay.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


@validate_call(config=dict(arbitrary_types_allowed=True))
def load_config(path: str | Path, overlay_path: str | Path | None = None) -> DeidConfig:
    """Load config from YAML file, interpolate env vars, validate with Pydantic.

    If *overlay_path* is provided, it is loaded as a second YAML file and
    deep-merged on top of the base config — overlay keys override base keys,
    new keys are appended, and nested dicts are merged recursively.
    """
    path = Path(path)
    with open(path) as f:
        raw = yaml.safe_load(f)

    if overlay_path is not None:
        overlay_path = Path(overlay_path)
        with open(overlay_path) as f:
            overlay_raw = yaml.safe_load(f) or {}
        raw = _deep_merge(raw, overlay_raw)

    interpolated = _interpolate_env_vars(raw)
    return DeidConfig(**interpolated)
