from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from canopy.models.execution import ScriptExecutionResult


def _load_module(script_path: Path) -> ModuleType:
    """Dynamically load a Python script as a module."""
    spec = importlib.util.spec_from_file_location("canopy_transform", str(script_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    # Don't pollute sys.modules permanently
    spec.loader.exec_module(module)
    return module


class ScriptRunner:
    """Executes generated conversion scripts on data."""

    def run_on_sample(
        self, script_path: Path, sample_rows: list[dict[str, str]]
    ) -> ScriptExecutionResult:
        """Run the script's transform() on sample rows. Captures errors per-row."""
        try:
            module = _load_module(script_path)
        except Exception as e:
            return ScriptExecutionResult(
                success=False,
                errors=[f"Failed to load script: {e}"],
                row_count_in=len(sample_rows),
            )

        transform = getattr(module, "transform", None)
        if transform is None:
            return ScriptExecutionResult(
                success=False,
                errors=["Script is missing a transform() function"],
                row_count_in=len(sample_rows),
            )

        output_rows: list[dict[str, Any]] = []
        errors: list[str] = []

        for i, row in enumerate(sample_rows):
            try:
                result = transform(row)
                if result is not None:
                    output_rows.append(result)
            except Exception as e:
                errors.append(f"Row {i}: {type(e).__name__}: {e}")

        return ScriptExecutionResult(
            success=len(errors) == 0,
            output_rows=output_rows,
            errors=errors,
            row_count_in=len(sample_rows),
            row_count_out=len(output_rows),
        )

    def run_on_batch(
        self, script_path: Path, rows: list[dict[str, str]]
    ) -> ScriptExecutionResult:
        """Run transform on a batch during full execution. Same logic as run_on_sample."""
        return self.run_on_sample(script_path, rows)
