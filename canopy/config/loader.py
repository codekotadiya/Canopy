from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from canopy.models.config import PipelineConfig


_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _interpolate_env_vars(raw: str) -> str:
    """Replace ${VAR_NAME} patterns with values from environment variables."""

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ValueError(
                f"Environment variable '{var_name}' is referenced in config but not set"
            )
        return value

    return _ENV_VAR_PATTERN.sub(_replace, raw)


def load_config(path: str | Path) -> PipelineConfig:
    """Load a pipeline YAML config file and return a validated PipelineConfig."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw_text = path.read_text(encoding="utf-8")
    interpolated = _interpolate_env_vars(raw_text)
    data = yaml.safe_load(interpolated)

    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping, got {type(data).__name__}")

    return PipelineConfig(**data)
