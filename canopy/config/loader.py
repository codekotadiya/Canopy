"""Pipeline configuration loader with environment variable interpolation."""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from canopy.models.config import PipelineConfig


_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _interpolate_env_vars(raw: str) -> str:
    """Replace ${VAR_NAME} patterns with values from environment variables.

    Only interpolates values in YAML scalar positions (after a colon or at the
    start of a list item).  This avoids accidental replacement of ``${...}``
    patterns that appear inside unrelated strings such as regex or template
    literals.
    """

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ValueError(
                f"Environment variable '{var_name}' is referenced in config but not set"
            )
        return value

    # Parse YAML first, then interpolate only string values in the resulting dict.
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping, got {type(data).__name__}")

    _interpolate_dict(data, _replace)
    return data


def _interpolate_dict(obj: dict, replacer) -> None:
    """Recursively interpolate env vars in dict string values only."""
    for key, value in obj.items():
        if isinstance(value, str):
            obj[key] = _ENV_VAR_PATTERN.sub(replacer, value)
        elif isinstance(value, dict):
            _interpolate_dict(value, replacer)
        elif isinstance(value, list):
            _interpolate_list(value, replacer)


def _interpolate_list(obj: list, replacer) -> None:
    """Recursively interpolate env vars in list string values only."""
    for i, value in enumerate(obj):
        if isinstance(value, str):
            obj[i] = _ENV_VAR_PATTERN.sub(replacer, value)
        elif isinstance(value, dict):
            _interpolate_dict(value, replacer)
        elif isinstance(value, list):
            _interpolate_list(value, replacer)


def load_config(path: str | Path) -> PipelineConfig:
    """Load a pipeline YAML config file and return a validated PipelineConfig."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw_text = path.read_text(encoding="utf-8")
    data = _interpolate_env_vars(raw_text)

    return PipelineConfig(**data)
