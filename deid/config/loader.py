"""Load and validate config.yaml with env var interpolation."""
from __future__ import annotations

import json
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
# Whole-value token, optionally with a shell-style default: ${VAR} or ${VAR:-default}.
# The default may itself contain ${...} tokens (e.g. ${SOURCE_DB_HOST:-${DB_HOST}})
# and is resolved recursively. Greedy .* + trailing \} lets the nested closing
# brace belong to the inner token rather than terminating the outer one.
_WHOLE_TOKEN_PATTERN = re.compile(r"\$\{(\w+)(?::-(.*))?\}")


@validate_call(config=dict(arbitrary_types_allowed=True))
def _interpolate_env_vars(obj):
    """Recursively replace ${VAR_NAME} with os.environ[VAR_NAME].

    When a value is *exactly* a single ${VAR} token and the environment value
    parses as JSON yielding a list or dict, the parsed structure is returned.
    This lets structured config — e.g. a pii_config ``replace_value`` list — be
    supplied from one env var. Embedded/partial references and scalar values
    (ports, names, connection strings) fall back to plain string substitution,
    so existing configs are unaffected.

    A whole-value token may carry a shell-style default: ``${VAR:-default}``
    returns *default* when VAR is unset instead of raising. The default may be
    a literal or itself a ``${OTHER}`` token (resolved recursively), e.g.
    ``${SOURCE_DB_HOST:-${DB_HOST}}``.
    """
    if isinstance(obj, str):
        whole = _WHOLE_TOKEN_PATTERN.fullmatch(obj)
        if whole is not None:
            var = whole.group(1)
            default = whole.group(2)  # None when no ':-' is present
            val = os.environ.get(var)
            # Shell ``:-`` semantics: fall back to the default when the var is
            # unset OR empty ("" — how the .env parser stores a blank KEY=).
            if default is not None and not val:
                # Resolve the default (may contain its own ${...} tokens).
                return _interpolate_env_vars(default)
            if val is None:
                raise ValueError(f"Environment variable '{var}' not set (referenced in config)")
            try:
                parsed = json.loads(val)
            except (ValueError, TypeError):
                return val
            # Only substitute structured JSON; scalars stay strings so that e.g.
            # "${MYSQL_PORT}" remains "3306" rather than becoming the int 3306.
            return parsed if isinstance(parsed, (list, dict)) else val

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
